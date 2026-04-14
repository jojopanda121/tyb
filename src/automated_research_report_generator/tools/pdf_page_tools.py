from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fitz
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

# 设计目的：把 PDF 原文读取和逐页索引查询封装成基础工具层，供所有 crew 共享同一套读页方式。
# 模块功能：读取整份 PDF、维护逐页索引，并提供“先读索引、再读正文”的两个工具。
# 实现逻辑：统一管理当前 PDF 上下文、页文本缓存、页索引缓存和工具输入输出格式。
# 可调参数：`MAX_TOOL_PAGE_READ`、`PAGE_INDEX_FORMAT_VERSION` 和当前 PDF 上下文路径。
# 默认参数及原因：默认先读索引再读页，原因是这样能强制 agent 先筛页，避免盲读全文。

PROJECT_ROOT = Path(__file__).resolve().parents[3]

_pdf_ctx = threading.local()

_PAGE_TEXT_CACHE: dict[str, list[str]] = {}
_PAGE_INDEX_CACHE: dict[str, dict[str, Any]] = {}
_ACTIVE_PAGE_INDEX_DIR: Path | None = None

MAX_TOOL_PAGE_READ = 100
PAGE_INDEX_FORMAT_VERSION = 3


def reset_pdf_page_tool_runtime_state() -> None:
    """
    设计目的：给 PDF 工具层提供统一的运行时重置入口。
    模块功能：清空当前 PDF 上下文和工具内存缓存。
    实现逻辑：重置当前线程的 PDF 路径、索引路径、页文本缓存和索引缓存。
    可调参数：无。
    默认参数及原因：一次清空全部状态，原因是预处理和正式分析都依赖同一套上下文变量。
    """

    _pdf_ctx.pdf_file_path = ""
    _pdf_ctx.page_index_path = ""
    _PAGE_TEXT_CACHE.clear()
    _PAGE_INDEX_CACHE.clear()
    global _ACTIVE_PAGE_INDEX_DIR
    _ACTIVE_PAGE_INDEX_DIR = None


class PdfPageIndexEntry(BaseModel):
    """
    设计目的：定义逐页索引里的单页记录格式。
    模块功能：保存页码、页面短概括和固定主题归类结果。
    实现逻辑：用 Pydantic 约束字段类型，保证索引文件结构稳定。
    可调参数：`page_number`、`topic` 和 `matched_topics`。
    默认参数及原因：`matched_topics` 默认空列表，原因是兼容旧数据和兜底场景。
    """

    page_number: int = Field(..., description="PDF 中的 1 基页码")
    topic: str = Field(..., description="当前页面的短概括")
    matched_topics: list[str] = Field(
        default_factory=list,
        description="从固定主题字典中选出的最接近主题",
    )


class PdfPageIndexPayload(BaseModel):
    """
    设计目的：定义完整页索引文件的结构。
    模块功能：保存 PDF 基本信息、指纹、页数和全部页面记录。
    实现逻辑：用统一载荷结构把索引元数据和逐页结果一起落盘。
    可调参数：各字段由页索引流程写入。
    默认参数及原因：无默认业务值，原因是索引文件必须准确绑定当前 PDF。
    """

    format_version: int
    pdf_file_path: str
    pdf_name: str
    generated_at: str
    fingerprint: str
    page_count: int
    pages: list[PdfPageIndexEntry]


class ReadPdfPageIndexInput(BaseModel):
    """
    设计目的：定义读取页索引工具的输入格式。
    模块功能：约束 PDF 路径、关键词过滤和返回条数限制。
    实现逻辑：通过 Pydantic 校验 `pdf_path`、`keyword` 和 `max_results`。
    可调参数：`pdf_path`、`keyword` 和 `max_results`。
    默认参数及原因：`max_results` 默认 0，原因是 0 更适合表达“不截断”。
    """

    pdf_path: str = Field(
        ...,
        description="当前要读取的 PDF 绝对路径。必须显式传入任务输入里的 `pdf_file_path`。",
    )
    keyword: str = Field(
        default="",
        description="可选关键词。留空时返回完整页索引。",
    )
    max_results: int = Field(
        default=0,
        ge=0,
        description="过滤后最多返回多少条结果。0 表示不限制。",
    )


class ReadPdfPagesInput(BaseModel):
    """
    设计目的：定义直接读页工具的输入格式。
    模块功能：约束调用方显式传入 PDF 路径，并用页码选择器而不是自由文本传参。
    实现逻辑：通过 Pydantic 约束 `pdf_path` 和 `pages` 必填。
    可调参数：`pdf_path` 和 `pages`。
    默认参数及原因：无默认页码，原因是正文读取必须显式指定范围。
    """

    pdf_path: str = Field(
        ...,
        description="当前要读取的 PDF 绝对路径。必须显式传入任务输入里的 `pdf_file_path`。",
    )
    pages: str = Field(
        ...,
        description="页码选择器，例如 `3,5,8-10`。应先读页索引，再读取相关页面。",
    )


def resolve_pdf_path(pdf_file_path: str) -> Path:
    """
    设计目的：统一 PDF 路径解析逻辑。
    模块功能：展开用户目录并返回绝对路径。
    实现逻辑：使用 `Path(...).expanduser().resolve()` 做标准化。
    可调参数：`pdf_file_path`。
    默认参数及原因：固定返回绝对路径，原因是缓存键和日志都更稳定。
    """

    return Path(pdf_file_path).expanduser().resolve()


def activate_page_index_directory(output_dir: str | Path) -> Path:
    """
    设计目的：让页索引输出目录跟随当前 run，而不是落到项目级公共缓存目录。
    模块功能：记录当前活动的页索引输出目录，并确保目录存在。
    实现逻辑：把输入路径标准化后写入模块级上下文，再创建目录。
    可调参数：`output_dir`。
    默认参数及原因：由 Flow 在拿到 run 目录后显式设置，原因是页索引属于单次运行中间产物。
    """

    global _ACTIVE_PAGE_INDEX_DIR

    resolved = Path(output_dir).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    _ACTIVE_PAGE_INDEX_DIR = resolved
    return resolved


def get_output_directory() -> Path:
    """
    设计目的：统一页索引文件输出目录。
    模块功能：返回当前 run 已激活的页索引输出目录。
    实现逻辑：只接受 Flow 显式激活过的 run 级目录；若未激活则直接报错，不再回退到项目级公共缓存目录。
    可调参数：目录位置由 `PROJECT_ROOT` 决定。
    默认参数及原因：默认不再提供项目级回退目录，原因是所有页索引都必须严格落在 `.cache/<run_slug>/indexing/` 内。
    """

    if _ACTIVE_PAGE_INDEX_DIR is None:
        raise RuntimeError(
            "Page index output directory is not activated. "
            "Call activate_page_index_directory() before resolving default page index paths."
        )
    return _ACTIVE_PAGE_INDEX_DIR


def compute_pdf_fingerprint(pdf_path: str | Path) -> str:
    """
    设计目的：快速判断 PDF 是否发生变化。
    模块功能：基于路径、文件大小和修改时间生成稳定指纹。
    实现逻辑：拼接路径、大小和纳秒级修改时间，再做 SHA-256 哈希。
    可调参数：`pdf_path`。
    默认参数及原因：不读取整份文件内容，原因是这样更轻量，且对当前场景已经足够。
    """

    path = resolve_pdf_path(str(pdf_path))
    stat = path.stat()
    raw = f"{path}|{stat.st_size}|{stat.st_mtime_ns}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def default_page_index_path(pdf_file_path: str | Path) -> Path:
    """
    设计目的：统一页索引文件命名规则。
    模块功能：为指定 PDF 生成默认索引路径。
    实现逻辑：使用 `<pdf_stem>_page_index.json` 作为文件名。
    可调参数：`pdf_file_path`。
    默认参数及原因：文件名固定为 `<pdf_stem>_page_index.json`，原因是人工定位时更直观。
    """

    pdf_path = resolve_pdf_path(str(pdf_file_path))
    filename = f"{pdf_path.stem}_page_index.json"
    return get_output_directory() / filename


def set_pdf_context(pdf_file_path: str, page_index_path: str | None = None) -> None:
    """
    设计目的：让所有 PDF 工具共享同一份当前上下文。
    模块功能：记录当前线程的 PDF 路径和对应页索引路径。
    实现逻辑：先标准化 PDF 路径，再记录索引路径；未传索引路径时自动推导默认值。
    可调参数：`pdf_file_path` 和 `page_index_path`。
    默认参数及原因：页索引路径缺省时只允许在已激活 run 级 `indexing/` 的前提下自动推导，原因是禁止再回退到项目级公共目录。
    """

    _pdf_ctx.pdf_file_path = resolve_pdf_path(pdf_file_path).as_posix()
    _pdf_ctx.page_index_path = (
        Path(page_index_path).expanduser().resolve().as_posix()
        if page_index_path
        else default_page_index_path(_pdf_ctx.pdf_file_path).as_posix()
    )


def _prepare_pdf_tool_context(pdf_file_path: str) -> Path:
    """
    设计目的：让 PDF 工具每次执行都显式绑定本次调用的 PDF。
    模块功能：校验传入的 PDF 路径，并把它写回当前线程上下文。
    实现逻辑：先解析并检查 `pdf_file_path`，再调用 `set_pdf_context()` 让后续索引和读页共享同一份显式输入。
    可调参数：`pdf_file_path`。
    默认参数及原因：每次工具调用都重设上下文，原因是这样才能让 CrewAI 的 tool cache key 带上 PDF 维度，避免跨文档串缓存。
    """

    pdf_path = resolve_pdf_path(pdf_file_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")
    set_pdf_context(str(pdf_path))
    return pdf_path


def _normalize_page_text(text: str) -> str:
    """
    设计目的：统一 PDF 页面文本清洗规则。
    模块功能：去掉空字符、压缩多余空白和多行空行。
    实现逻辑：替换空字符、压缩连续空格和连续空行，再做首尾清理。
    可调参数：`text`。
    默认参数及原因：默认保留段落换行，原因是后续 agent 读页时仍需要基本结构。
    """

    normalized = (text or "").replace("\x00", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def extract_pdf_pages(pdf_file_path: str | Path) -> list[str]:
    """
    设计目的：为页索引工具和读页工具提供统一的 PDF 文本来源。
    模块功能：读取整份 PDF，并按页缓存纯文本结果。
    实现逻辑：优先读内存缓存；未命中时用 `fitz` 逐页抽取文本并清洗。
    可调参数：PDF 路径。
    默认参数及原因：优先走内存缓存，原因是同一份 PDF 在一轮 flow 中会被反复读取。
    """

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


def build_page_index_payload(
    pdf_file_path: str | Path,
    page_entries: list[PdfPageIndexEntry],
) -> PdfPageIndexPayload:
    """
    设计目的：把逐页主题列表打包成统一的索引载荷。
    模块功能：写入 PDF 基本信息、指纹、页数和页面记录。
    实现逻辑：把路径、名称、时间、指纹和逐页结果组合成 `PdfPageIndexPayload`。
    可调参数：`pdf_file_path` 和 `page_entries`。
    默认参数及原因：时间戳固定写 UTC，原因是跨机器排查更稳定。
    """

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


def save_page_index(payload: PdfPageIndexPayload, output_path: str | Path | None = None) -> str:
    """
    设计目的：把页索引落盘并同步内存缓存。
    模块功能：确保目录存在，保存 JSON，并缓存解析结果。
    实现逻辑：先决定输出路径，再把模型字典序列化为 UTF-8 JSON。
    可调参数：`payload` 和可选 `output_path`。
    默认参数及原因：`output_path` 为空时写默认路径，原因是大多数流程只需要一套标准索引位置。
    """

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


def load_page_index(page_index_path: str | Path | None = None) -> dict[str, Any]:
    """
    设计目的：提供统一的页索引读取入口。
    模块功能：读取并缓存指定的页索引 JSON。
    实现逻辑：优先读内存缓存；未命中时按 UTF-8 读取并解析文件。
    可调参数：`page_index_path`。
    默认参数及原因：为空时读取当前上下文索引，原因是工具调用通常已经先设置了 PDF 上下文。
    """

    current_page_index_path = getattr(_pdf_ctx, "page_index_path", "")
    path = (
        Path(page_index_path).expanduser().resolve()
        if page_index_path
        else Path(current_page_index_path).expanduser().resolve()
    )
    if not path.exists():
        raise FileNotFoundError(f"Page index JSON does not exist: {path}")

    cache_key = str(path)
    if cache_key in _PAGE_INDEX_CACHE:
        return _PAGE_INDEX_CACHE[cache_key]

    data = json.loads(path.read_text(encoding="utf-8"))
    _PAGE_INDEX_CACHE[cache_key] = data
    return data


def page_index_is_current(pdf_file_path: str | Path, page_index_path: str | Path) -> bool:
    """
    设计目的：判断现有页索引是否还能安全复用。
    模块功能：同时检查索引版本和 PDF 指纹是否匹配。
    实现逻辑：先确认索引文件存在，再读取索引并比较版本号和指纹。
    可调参数：`pdf_file_path` 和 `page_index_path`。
    默认参数及原因：读取或解析失败直接返回 `False`，原因是宁可重建也不要复用坏索引。
    """

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


def parse_page_selector(page_selector: str, page_count: int) -> list[int]:
    """
    设计目的：允许 agent 用自然但受控的方式请求页码范围。
    模块功能：把 `3,5,8-10` 这类字符串解析成合法页码列表。
    实现逻辑：先标准化中英文分隔符，再逐段解析单页和区间，最后做范围校验。
    可调参数：页码选择器和总页数。
    默认参数及原因：超出范围或一次读取过多页时直接报错，原因是保护上下文窗口。
    """

    if not page_selector.strip():
        raise ValueError("Page selector cannot be empty.")

    normalized = (
        page_selector.replace("，", ",")
        .replace("、", ",")
        .replace("；", ",")
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


def format_pdf_pages_for_agent(pdf_file_path: str | Path, page_numbers: list[int]) -> str:
    """
    设计目的：把多页原文整理成 agent 更容易引用的文本块。
    模块功能：逐页加上 `[Page N]` 标记并拼接正文。
    实现逻辑：按页号读取页面文本，空白页补充说明文字，再统一拼接输出。
    可调参数：`pdf_file_path` 和 `page_numbers`。
    默认参数及原因：空白页保留占位说明，原因是这样能让 agent 知道该页不是读取失败。
    """

    pages = extract_pdf_pages(pdf_file_path)
    rendered_pages: list[str] = []
    for page_number in page_numbers:
        page_text = pages[page_number - 1]
        if not page_text:
            page_text = "[No extractable text found on this page. It may be image-only or scanned.]"
        rendered_pages.append(f"[Page {page_number}]\n{page_text}")
    return "\n\n".join(rendered_pages)


class ReadPdfPageIndexTool(BaseTool):
    """
    设计目的：要求 agent 在读正文前先浏览页索引。
    模块功能：返回完整或筛选后的逐页主题列表。
    实现逻辑：读取当前页索引，根据关键词过滤，再按上限截断结果。
    可调参数：`keyword` 和 `max_results`。
    默认参数及原因：默认返回全量索引，原因是先看全局再缩小范围更稳妥。
    """

    name: str = "Step 1 - Read PDF Page Index"
    description: str = (
        "Read the current PDF page index JSON. Always call this first before reading any PDF page."
        " Use the page-by-page topic list to decide which exact pages are relevant to the current task. "
        "You must explicitly pass the current task input `pdf_file_path` into the `pdf_path` argument."
    )
    args_schema: type[BaseModel] = ReadPdfPageIndexInput

    def _run(self, pdf_path: str, keyword: str = "", max_results: int = 0) -> str:
        """
        设计目的：强制 agent 先看索引，再决定读哪些页。
        模块功能：返回完整或过滤后的逐页索引 JSON。
        实现逻辑：读取当前索引后，按 `topic`、`matched_topics` 和页码做过滤。
        可调参数：关键词和最大结果数。
        默认参数及原因：`max_results=0` 表示不过滤数量，原因是有些任务需要先看全量索引再缩小范围。
        """

        resolved_pdf_path = _prepare_pdf_tool_context(pdf_path)
        index_data = load_page_index()
        pages = index_data.get("pages", [])

        if keyword:
            lowered_keyword = keyword.lower().strip()
            pages = [
                page
                for page in pages
                if lowered_keyword in str(page.get("topic", "")).lower()
                or lowered_keyword in " ".join(page.get("matched_topics", [])).lower()
                or lowered_keyword in str(page.get("page_number", ""))
            ]

        if max_results > 0:
            pages = pages[:max_results]

        payload = {
            "pdf_file_path": resolved_pdf_path.as_posix(),
            "page_index_file_path": getattr(_pdf_ctx, "page_index_path", ""),
            "page_count": index_data.get("page_count", len(pages)),
            "pages": pages,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)


class ReadPdfPagesTool(BaseTool):
    """
    设计目的：提供无 RAG 的直接读页能力。
    模块功能：按页码选择器返回指定页面的完整文本。
    实现逻辑：先解析页码选择器，再读取对应页文本并拼接输出。
    可调参数：`pages`。
    默认参数及原因：默认按页分段输出，原因是引用证据时更容易定位来源页码。
    """

    name: str = "Step 2 - Read Relevant PDF Pages"
    description: str = (
        "Read the full extracted text of specific PDF pages directly, without RAG. "
        "Use this only after reading the page index and only request the pages relevant to your task. "
        "You must explicitly pass the current task input `pdf_file_path` into the `pdf_path` argument."
    )
    args_schema: type[BaseModel] = ReadPdfPagesInput

    def _run(self, pdf_path: str, pages: str) -> str:
        """
        设计目的：提供无 RAG 的直接读页能力，让 agent 可以看到最原始的页文本。
        模块功能：解析页码选择器并返回指定页的完整文本。
        实现逻辑：先校验当前 PDF 上下文，再解析页码并读取对应文本。
        可调参数：页码字符串。
        默认参数及原因：输出按页拼接，原因是后续引用证据时更容易定位来源页码。
        """

        resolved_pdf_path = _prepare_pdf_tool_context(pdf_path)
        page_texts = extract_pdf_pages(resolved_pdf_path)
        page_numbers = parse_page_selector(pages, len(page_texts))
        return format_pdf_pages_for_agent(resolved_pdf_path, page_numbers)
