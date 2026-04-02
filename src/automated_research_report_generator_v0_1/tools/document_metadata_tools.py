from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from automated_research_report_generator_v0_1.tools.pdf_page_tools import (
    compute_pdf_fingerprint,
    default_page_index_path,
    extract_pdf_pages,
)


"""元数据缓存与采样工具"""


MAX_METADATA_SOURCE_PAGES = 20  # 可调：建议 >=1；默认 20，原因：覆盖关键页且控制成本。
MAX_METADATA_PAGE_CHARS = 2500  # 可调：建议 >=200；默认 2500，原因：兼顾线索保留与 token 成本。


def default_document_metadata_path(pdf_file_path: str | Path) -> Path:  # 设计：统一缓存路径；功能：给元数据 JSON 固定命名；默认与页索引同目录，原因：便于集中查看。
    pdf_path = Path(pdf_file_path).expanduser().resolve()
    return default_page_index_path(pdf_path).with_name(f"{pdf_path.stem}_document_metadata.json")


def load_document_metadata(metadata_path: str | Path) -> dict[str, Any]:  # 设计：统一读缓存；功能：加载已保存元数据；默认按 utf-8 读取，原因：兼容中文内容。
    path = Path(metadata_path).expanduser().resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def save_document_metadata(payload: BaseModel, metadata_path: str | Path) -> str:  # 设计：统一写缓存；功能：落盘识别结果；默认自动建目录，原因：减少调用方负担。
    path = Path(metadata_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(path)


def document_metadata_is_current(pdf_file_path: str | Path, metadata_path: str | Path) -> bool:  # 设计：校验缓存新鲜度；功能：用指纹判断是否仍匹配当前 PDF；默认不匹配即重建，原因：宁可重跑也不串旧结果。
    path = Path(metadata_path).expanduser().resolve()
    if not path.exists():
        return False

    try:
        data = load_document_metadata(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return False

    return data.get("fingerprint") == compute_pdf_fingerprint(pdf_file_path)


def sample_document_metadata_pages(  # 设计：抽样识别页；功能：只取前若干非空页供元数据识别；可调：max_pages、max_chars_per_page；默认 20/2500，原因：覆盖关键页且控制成本。
    pdf_file_path: str | Path,
    max_pages: int = MAX_METADATA_SOURCE_PAGES,
    max_chars_per_page: int = MAX_METADATA_PAGE_CHARS,
) -> list[tuple[int, str]]:
    sampled_pages: list[tuple[int, str]] = []
    for page_number, page_text in enumerate(extract_pdf_pages(pdf_file_path), start=1):
        if page_text.strip():
            sampled_pages.append((page_number, page_text[:max_chars_per_page]))
        if len(sampled_pages) >= max_pages:
            break
    return sampled_pages
