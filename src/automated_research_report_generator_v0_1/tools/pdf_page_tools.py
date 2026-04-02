from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fitz
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

"""PDF 页索引与读页工具"""

CURRENT_PDF_FILE_PATH = ""  # 默认空；功能：保存当前 PDF 上下文；原因：由入口统一注入。
CURRENT_PAGE_INDEX_PATH = ""  # 默认空；功能：保存当前页索引路径；原因：供全部工具共享。

_PAGE_TEXT_CACHE: dict[str, list[str]] = {}  # 默认空；功能：缓存逐页文本；原因：避免重复抽取。
_PAGE_INDEX_CACHE: dict[str, dict[str, Any]] = {}  # 默认空；功能：缓存索引 JSON；原因：减少磁盘读取。

MAX_TOOL_PAGE_READ = 75  # 可调：建议 1-100；默认 75，原因：限制单次上下文体积。
PAGE_INDEX_FORMAT_VERSION = 2  # 可调：整数递增；默认 2，原因：索引结构变化时强制失效旧缓存。


def reset_pdf_page_tool_runtime_state() -> None:  # 设计：清理工具运行态；功能：重置上下文和内存缓存；默认不碰磁盘缓存，原因：复跑更快。
    global CURRENT_PDF_FILE_PATH, CURRENT_PAGE_INDEX_PATH
    CURRENT_PDF_FILE_PATH = ""
    CURRENT_PAGE_INDEX_PATH = ""
    _PAGE_TEXT_CACHE.clear()
    _PAGE_INDEX_CACHE.clear()


class PdfPageIndexEntry(BaseModel):  # 设计：定义单页索引条目；功能：保存页码与主题；默认 1-based 页码，原因：与人工阅读一致。
    page_number: int = Field(..., description="1-based page number in the PDF")
    topic: str = Field(..., description="A short topic label for this page")


class PdfPageIndexPayload(BaseModel):  # 设计：定义索引落盘结构；功能：把页列表与校验信息一起保存；默认保留指纹和版本，原因：便于判断缓存是否可用。
    format_version: int
    pdf_file_path: str
    pdf_name: str
    generated_at: str
    fingerprint: str
    page_count: int
    pages: list[PdfPageIndexEntry]


class ReadPdfPageIndexInput(BaseModel):  # 设计：定义索引工具入参；功能：支持关键词筛页；默认允许空关键字，原因：可直接返回全量索引。
    keyword: str = Field(
        default="",
        description="Optional keyword to filter the JSON page index. Leave empty to return the full index.",
    )
    max_results: int = Field(
        default=0,
        ge=0,
        description="Maximum number of page entries to return after filtering. Use 0 for all matches.",
    )


class ReadPdfPagesInput(BaseModel):  # 设计：定义读页工具入参；功能：支持页码选择器；默认要求显式传页码串，原因：避免误读整本 PDF。
    pages: str = Field(
        ...,
        description="Page selector such as '3,5,8-10'. Read the page index first, then request only the relevant pages.",
    )


def resolve_pdf_path(pdf_file_path: str) -> Path:  # 设计：统一路径解析；功能：把相对路径转绝对路径；默认 resolve，原因：避免上下文漂移。
    return Path(pdf_file_path).expanduser().resolve()


def get_output_directory() -> Path:  # 设计：统一输出目录；功能：返回页索引缓存目录；默认写入 .cache/pdf_page_indexes，原因：便于集中管理。
    output_dir = Path.cwd() / ".cache" / "pdf_page_indexes"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def compute_pdf_fingerprint(pdf_path: str | Path) -> str:  # 设计：生成轻量指纹；功能：判断 PDF 是否变化；默认用路径+大小+mtime，原因：足够快且区分度够用。
    path = resolve_pdf_path(str(pdf_path))
    stat = path.stat()
    raw = f"{path}|{stat.st_size}|{stat.st_mtime_ns}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def default_page_index_path(pdf_file_path: str | Path) -> Path:  # 设计：统一索引命名；功能：生成固定 JSON 路径；默认用 stem 命名，原因：同 PDF 易对应。
    pdf_path = resolve_pdf_path(str(pdf_file_path))
    filename = f"{pdf_path.stem}_page_index.json"
    return get_output_directory() / filename


def set_pdf_context(pdf_file_path: str, page_index_path: str | None = None) -> None:  # 设计：注入工具上下文；功能：缓存当前 PDF 与索引路径；默认索引为空时按规则推导，原因：减少入口样板。
    global CURRENT_PDF_FILE_PATH, CURRENT_PAGE_INDEX_PATH
    CURRENT_PDF_FILE_PATH = str(resolve_pdf_path(pdf_file_path))
    CURRENT_PAGE_INDEX_PATH = (
        str(Path(page_index_path).expanduser().resolve())
        if page_index_path
        else str(default_page_index_path(CURRENT_PDF_FILE_PATH))
    )


def _require_pdf_context() -> Path:  # 设计：统一前置校验；功能：确保工具执行前已有有效 PDF 上下文；默认缺失就抛错，原因：比静默失败更好排查。
    if not CURRENT_PDF_FILE_PATH:
        raise ValueError("CURRENT_PDF_FILE_PATH is empty. Initialize the PDF context first.")

    pdf_path = resolve_pdf_path(CURRENT_PDF_FILE_PATH)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")
    return pdf_path


def _normalize_page_text(text: str) -> str:  # 设计：统一清洗页文本；功能：去空字符和冗余空白；默认保留段落边界，原因：后续阅读更自然。
    normalized = (text or "").replace("\x00", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def extract_pdf_pages(pdf_file_path: str | Path) -> list[str]:  # 设计：统一抽页文本；功能：为索引和读页共用；默认命中内存缓存先返回，原因：减少重复 IO。
    pdf_path = resolve_pdf_path(str(pdf_file_path))
    cache_key = str(pdf_path)
    if cache_key in _PAGE_TEXT_CACHE:
        return _PAGE_TEXT_CACHE[cache_key]

    document = fitz.open(pdf_path)
    try:
        pages = [
            _normalize_page_text(document.load_page(page_index).get_text("text"))
            for page_index in range(document.page_count)
        ]
    finally:
        document.close()

    _PAGE_TEXT_CACHE[cache_key] = pages
    return pages


def build_page_index_payload(  # 设计：构建落盘载荷；功能：给页索引补齐元数据；默认自动带指纹和时间，原因：便于缓存校验。
    pdf_file_path: str | Path,
    page_entries: list[PdfPageIndexEntry],
) -> PdfPageIndexPayload:
    pdf_path = resolve_pdf_path(str(pdf_file_path))
    return PdfPageIndexPayload(
        format_version=PAGE_INDEX_FORMAT_VERSION,
        pdf_file_path=str(pdf_path),
        pdf_name=pdf_path.name,
        generated_at=datetime.now(timezone.utc).isoformat(),
        fingerprint=compute_pdf_fingerprint(pdf_path),
        page_count=len(page_entries),
        pages=page_entries,
    )


def save_page_index(payload: PdfPageIndexPayload, output_path: str | Path | None = None) -> str:  # 设计：统一写索引；功能：保存页索引 JSON；默认 output_path 为空时写标准路径，原因：减少调用方分支。
    index_path = (
        Path(output_path).expanduser().resolve()
        if output_path
        else default_page_index_path(payload.pdf_file_path)
    )
    index_path.parent.mkdir(parents=True, exist_ok=True)
    data = payload.model_dump()
    index_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _PAGE_INDEX_CACHE[str(index_path)] = data
    return str(index_path)


def load_page_index(page_index_path: str | Path | None = None) -> dict[str, Any]:  # 设计：统一读索引；功能：加载并缓存 JSON；默认不传时读当前上下文路径，原因：方便工具直接复用。
    path = (
        Path(page_index_path).expanduser().resolve()
        if page_index_path
        else Path(CURRENT_PAGE_INDEX_PATH).expanduser().resolve()
    )
    if not path.exists():
        raise FileNotFoundError(f"Page index JSON does not exist: {path}")

    cache_key = str(path)
    if cache_key in _PAGE_INDEX_CACHE:
        return _PAGE_INDEX_CACHE[cache_key]

    data = json.loads(path.read_text(encoding="utf-8"))
    _PAGE_INDEX_CACHE[cache_key] = data
    return data


def page_index_is_current(pdf_file_path: str | Path, page_index_path: str | Path) -> bool:  # 设计：判断索引是否可复用；功能：校验版本与指纹；默认任一不符即失效，原因：避免旧索引污染。
    index_path = Path(page_index_path).expanduser().resolve()
    if not index_path.exists():
        return False

    try:
        data = load_page_index(index_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return False

    return (
        data.get("format_version") == PAGE_INDEX_FORMAT_VERSION
        and data.get("fingerprint") == compute_pdf_fingerprint(pdf_file_path)
    )


def parse_page_selector(page_selector: str, page_count: int) -> list[int]:  # 设计：解析页码选择器；功能：展开范围并校验边界；默认超范围和超量即报错，原因：保护上下文体积。
    if not page_selector.strip():
        raise ValueError("Page selector cannot be empty.")

    normalized = (
        page_selector.replace("，", ",")
        .replace("；", ",")
        .replace("、", ",")
        .replace("~", "-")
        .replace("～", "-")
        .replace(":", "-")
        .replace("：", "-")
    )

    pages: set[int] = set()
    for part in normalized.split(","):
        chunk = part.strip()
        if not chunk:
            continue

        if "-" in chunk:
            start_str, end_str = chunk.split("-", 1)
            start_page = int(start_str)
            end_page = int(end_str)
            if start_page > end_page:
                start_page, end_page = end_page, start_page
            pages.update(range(start_page, end_page + 1))
            continue

        pages.add(int(chunk))

    if not pages:
        raise ValueError("No valid pages were parsed from the selector.")

    invalid_pages = sorted(page for page in pages if page < 1 or page > page_count)
    if invalid_pages:
        raise ValueError(
            f"Pages out of range: {invalid_pages}. The PDF has {page_count} pages."
        )

    selected_pages = sorted(pages)
    if len(selected_pages) > MAX_TOOL_PAGE_READ:
        raise ValueError(
            f"Too many pages requested at once ({len(selected_pages)}). "
            f"Please narrow the selection to {MAX_TOOL_PAGE_READ} pages or fewer."
        )

    return selected_pages


def format_pdf_pages_for_agent(pdf_file_path: str | Path, page_numbers: list[int]) -> str:  # 设计：统一渲染读页结果；功能：按 [Page N] 拼多页文本；默认无文本页给占位说明，原因：避免误判为空返回。
    pages = extract_pdf_pages(pdf_file_path)
    rendered_pages: list[str] = []
    for page_number in page_numbers:
        page_text = pages[page_number - 1]
        if not page_text:
            page_text = "[No extractable text found on this page. It may be image-only or scanned.]"
        rendered_pages.append(f"[Page {page_number}]\n{page_text}")
    return "\n\n".join(rendered_pages)


class ReadPdfPageIndexTool(BaseTool):  # 设计：提供筛页工具；功能：返回当前 PDF 页索引；默认要求先读索引再读页，原因：减少无关页加载。
    name: str = "Step 1 - Read PDF Page Index"
    description: str = (
        "Read the current PDF page index JSON. Always call this first before reading any PDF page."
        "Use the page-by-page topic list to decide which exact pages are relevant to the current task."
    )
    args_schema: type[BaseModel] = ReadPdfPageIndexInput

    def _run(self, keyword: str = "", max_results: int = 0) -> str:  # 设计：执行索引读取；功能：按条件返回 JSON 文本；可调：keyword、max_results；默认空值返回全量，原因：兼容最简单调用。
        pdf_path = _require_pdf_context()
        index_data = load_page_index()
        pages = index_data.get("pages", [])

        if keyword:
            lowered_keyword = keyword.lower().strip()
            pages = [
                page
                for page in pages
                if lowered_keyword in str(page.get("topic", "")).lower()
                or lowered_keyword in str(page.get("page_number", ""))
            ]

        if max_results > 0:
            pages = pages[:max_results]

        payload = {
            "pdf_file_path": str(pdf_path),
            "page_index_file_path": CURRENT_PAGE_INDEX_PATH,
            "page_count": index_data.get("page_count", len(pages)),
            "pages": pages,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)


class ReadPdfPagesTool(BaseTool):  # 设计：提供读页工具；功能：返回指定页完整文本；默认要求先筛页后读页，原因：控制上下文大小。
    name: str = "Step 2 - Read Relevant PDF Pages"
    description: str = (
        "Read the full extracted text of specific PDF pages directly, without RAG. "
        "Use this only after reading the page index and only request the pages relevant to your task."
    )
    args_schema: type[BaseModel] = ReadPdfPagesInput

    def _run(self, pages: str) -> str:  # 设计：执行读页；功能：返回指定页文本；可调：pages 支持 1,3,5-8；默认直读不走 RAG，原因：证据更可控。
        pdf_path = _require_pdf_context()
        page_texts = extract_pdf_pages(pdf_path)
        page_numbers = parse_page_selector(pages, len(page_texts))
        return format_pdf_pages_for_agent(pdf_path, page_numbers)
