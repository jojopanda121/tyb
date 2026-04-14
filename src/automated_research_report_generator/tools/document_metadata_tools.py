from __future__ import annotations

# 设计目的：为 metadata 识别提供文件层辅助能力，避免识别逻辑和文件操作耦在一起。
# 模块功能：计算 metadata 路径、读写 JSON、校验指纹，并抽样候选页。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：`MAX_METADATA_SOURCE_PAGES` 和 `MAX_METADATA_PAGE_CHARS`。
# 默认参数及原因：默认只抽前面的非空页并截断长度，原因是封面、目录和概览通常已经够用。

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from automated_research_report_generator.tools.pdf_page_tools import (
    compute_pdf_fingerprint,
    default_page_index_path,
    extract_pdf_pages,
)


MAX_METADATA_SOURCE_PAGES = 20
MAX_METADATA_PAGE_CHARS = 2500


def default_document_metadata_path(pdf_file_path: str | Path) -> Path:
    """
    设计目的：统一 metadata 文件命名规则。
    模块功能：基于 PDF 路径生成对应的 metadata JSON 路径。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`pdf_file_path`。
    默认参数及原因：默认和页索引文件放在同一目录，原因是两者都属于 PDF 预处理产物。
    """

    pdf_path = Path(pdf_file_path).expanduser().resolve()
    return default_page_index_path(pdf_path).with_name(f"{pdf_path.stem}_document_metadata.json")


def load_document_metadata(metadata_path: str | Path) -> dict[str, Any]:
    """
    设计目的：提供统一的 metadata 读取入口。
    模块功能：读取 JSON 文件并还原成普通字典。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`metadata_path`。
    默认参数及原因：固定按 UTF-8 读取，原因是项目产物统一使用 UTF-8。
    """

    path = Path(metadata_path).expanduser().resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def save_document_metadata(payload: BaseModel, metadata_path: str | Path) -> str:
    """
    设计目的：把 metadata 落盘逻辑集中管理。
    模块功能：确保目录存在，再把 `payload` 保存成缩进 JSON。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`payload` 和 `metadata_path`。
    默认参数及原因：默认保留缩进格式，原因是人工排查 metadata 时更易读。
    """

    path = Path(metadata_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(path)


def document_metadata_is_current(pdf_file_path: str | Path, metadata_path: str | Path) -> bool:
    """
    设计目的：避免重复调用模型生成已经过期的 metadata。
    模块功能：检查 metadata 文件是否存在且指纹仍匹配当前 PDF。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`pdf_file_path` 和 `metadata_path`。
    默认参数及原因：读取或解析失败直接判定为过期，原因是宁可重建也不要复用坏文件。
    """

    path = Path(metadata_path).expanduser().resolve()
    if not path.exists():
        return False

    try:
        data = load_document_metadata(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return False

    return data.get("fingerprint") == compute_pdf_fingerprint(pdf_file_path)


def sample_document_metadata_pages(
    pdf_file_path: str | Path,
    max_pages: int = MAX_METADATA_SOURCE_PAGES,
    max_chars_per_page: int = MAX_METADATA_PAGE_CHARS,
) -> list[tuple[int, str]]:
    """
    设计目的：给 metadata agent 提供高密度候选页。
    模块功能：按页扫描 PDF，只保留前若干个非空页。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：最大页数和每页最大字符数。
    默认参数及原因：优先保留靠前页面，原因是更容易覆盖封面、目录和概览。
    """

    sampled_pages: list[tuple[int, str]] = []
    for page_number, page_text in enumerate(extract_pdf_pages(pdf_file_path), start=1):
        if page_text.strip():
            sampled_pages.append((page_number, page_text[:max_chars_per_page]))
        if len(sampled_pages) >= max_pages:
            break
    return sampled_pages
