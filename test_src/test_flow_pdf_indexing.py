from __future__ import annotations

import time
from pathlib import Path

import pytest
import automated_research_report_generator.tools.pdf_page_tools as pdf_page_tools
from automated_research_report_generator.flow import pdf_indexing
from automated_research_report_generator.flow import document_metadata as document_metadata_module
from automated_research_report_generator.tools.pdf_page_tools import (
    PdfPageIndexEntry,
    ReadPdfPageIndexTool,
)


def test_summarize_pages_in_parallel_keeps_page_order(monkeypatch) -> None:
    """
    设计目的：锁住并发索引最关键的顺序约束。
    模块功能：验证页面并发执行后，返回结果仍然按原始页码排序。
    实现逻辑：用假的单页摘要函数制造乱序完成，再检查最终列表顺序。
    可调参数：通过 `monkeypatch` 替换 `summarize_page_topic`。
    默认参数及原因：测试固定 3 页，原因是这个规模已经足够覆盖乱序回收场景。
    """

    pages = ["第一页", "第二页", "第三页"]

    def fake_summarize_page_topic(
        page_number: int,
        page_text: str,
        company_name: str = "",
        total_pages: int = 0,
    ) -> PdfPageIndexEntry:
        """
        设计目的：给并发调度测试提供稳定的假执行器。
        模块功能：按不同页码制造不同等待时间，并返回固定结构的页索引结果。
        实现逻辑：页码越小延迟越长，故意制造“先提交的不一定先完成”的情况。
        可调参数：页码、页面文本、公司名和总页数。
        默认参数及原因：这里只用页码控制延迟，原因是测试目标只关心并发回收顺序。
        """

        time.sleep((4 - page_number) * 0.02)
        return PdfPageIndexEntry(
            page_number=page_number,
            topic=f"主题{page_number}",
            matched_topics=["业务"],
        )

    monkeypatch.setattr(pdf_indexing, "summarize_page_topic", fake_summarize_page_topic)

    entries = pdf_indexing.summarize_pages_in_parallel(
        pages=pages,
        company_name="测试公司",
        max_concurrency=3,
    )

    assert [entry.page_number for entry in entries] == [1, 2, 3]
    assert [entry.topic for entry in entries] == ["主题1", "主题2", "主题3"]
    assert [entry.matched_topics for entry in entries] == [["业务"], ["业务"], ["业务"]]


def test_page_index_runtime_config_from_env(monkeypatch) -> None:
    """
    设计目的：锁住并发数和重试次数的运行时配置入口。
    模块功能：验证环境变量能够覆盖默认并发数和重试次数。
    实现逻辑：先写入环境变量，再读取模块配置函数结果并做断言。
    可调参数：通过 `monkeypatch` 写入环境变量。
    默认参数及原因：测试只覆盖整数环境变量，原因是这两个入口都以整数配置为核心。
    """

    monkeypatch.setenv("PDF_INDEX_MAX_CONCURRENCY", "6")
    monkeypatch.setenv("PDF_INDEX_RETRY_LIMIT", "3")

    assert pdf_indexing.get_page_index_max_concurrency() == 6
    assert pdf_indexing.get_page_index_retry_limit() == 3


def test_extract_page_summary_from_raw_parses_json_code_block() -> None:
    """
    设计目的：锁住页面索引对原始 JSON 文本的本地解析能力。
    模块功能：验证模型把结果包在 Markdown 代码块里时，仍能正确还原主题和主题分类。
    实现逻辑：直接调用原始文本解析函数，覆盖 JSON 代码块这一条关键回归路径。
    可调参数：无。
    默认参数及原因：固定使用一段最小 JSON 示例，原因是这里关注解析链路而不是模型行为。
    """

    topic, matched_topics = pdf_indexing._extract_page_summary_from_raw(
        raw='```json\n{"topic":"主营业务概览","matched_topics":["业务","产品"]}\n```',
        fallback_topic="第1页内容",
        fallback_topics=["其他"],
    )

    assert topic == "主营业务概览"
    assert matched_topics == ["业务", "产品"]


def test_default_page_index_path_follows_active_run_indexing_directory(tmp_path) -> None:
    """
    设计目的：锁住页索引文件要跟随当前 run 的 `indexing/` 目录，而不是落到项目级公共缓存目录。
    模块功能：激活 run 级 indexing 目录后，验证默认页索引路径会写到该目录下。
    实现逻辑：先重置工具层运行时状态，再激活一个临时 indexing 目录并检查默认路径。
    可调参数：`tmp_path` 由 pytest 提供。
    默认参数及原因：默认使用临时目录，原因是测试不应污染项目真实缓存。
    """

    pdf_page_tools.reset_pdf_page_tool_runtime_state()
    indexing_dir = tmp_path / ".cache" / "20260409_test-company" / "indexing"
    pdf_page_tools.activate_page_index_directory(indexing_dir)

    index_path = pdf_page_tools.default_page_index_path(tmp_path / "sample.pdf")

    assert index_path == Path(indexing_dir / "sample_page_index.json").resolve()


def test_default_page_index_path_requires_active_run_indexing_directory(tmp_path, monkeypatch) -> None:
    """
    设计目的：锁住页索引默认路径不再允许回退到项目级 `pdf_page_indexes` 目录。
    模块功能：验证未激活 run 级 indexing 目录时，`default_page_index_path()` 会直接报错。
    实现逻辑：先把 `PROJECT_ROOT` 指向临时目录，再在未激活上下文下调用默认路径函数并断言抛出异常。
    可调参数：`tmp_path` 与 `monkeypatch` 由 pytest 提供。
    默认参数及原因：默认使用临时项目根目录，原因是测试不应污染真实工作区缓存。
    """

    pdf_page_tools.reset_pdf_page_tool_runtime_state()
    monkeypatch.setattr(pdf_page_tools, "PROJECT_ROOT", tmp_path)

    fallback_dir = tmp_path / ".cache" / "pdf_page_indexes"
    with pytest.raises(RuntimeError, match="activate_page_index_directory"):
        pdf_page_tools.default_page_index_path(tmp_path / "sample.pdf")
    assert not fallback_dir.exists()


def test_resolve_pdf_document_metadata_payload_does_not_touch_default_cache_path(tmp_path, monkeypatch) -> None:
    """
    设计目的：锁住 metadata 预读逻辑不会再尝试访问项目级默认缓存路径。
    模块功能：验证 `resolve_pdf_document_metadata_payload()` 直接走内存识别链路，而不是先计算默认 metadata 路径。
    实现逻辑：把默认 metadata 路径函数替换成会失败的替身，再替换抽样和摘要逻辑并断言函数仍能成功返回 payload。
    可调参数：`tmp_path` 与 `monkeypatch` 由 pytest 提供。
    默认参数及原因：默认用最小假 payload，原因是这里只验证“不碰默认缓存路径”这一条边界。
    """

    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_text("stub pdf", encoding="utf-8")

    monkeypatch.setattr(
        document_metadata_module,
        "default_document_metadata_path",
        lambda pdf_file_path: (_ for _ in ()).throw(AssertionError("should not touch default metadata path")),
    )
    monkeypatch.setattr(document_metadata_module, "sample_document_metadata_pages", lambda *args, **kwargs: [(1, "封面")])
    monkeypatch.setattr(document_metadata_module, "create_document_metadata_agent", lambda: object())
    monkeypatch.setattr(
        document_metadata_module,
        "summarize_document_metadata",
        lambda agent, pdf_file_path, sampled_pages: document_metadata_module.PdfDocumentMetadataPayload(
            pdf_file_path=str(Path(pdf_file_path).resolve()),
            generated_at="2026-04-09T22:30:00+08:00",
            fingerprint="fake-fingerprint",
            company_name="测试公司",
            industry="测试行业",
            source_pages=[1],
        ),
    )

    payload = document_metadata_module.resolve_pdf_document_metadata_payload(pdf_path.as_posix())

    assert payload.company_name == "测试公司"
    assert payload.industry == "测试行业"
    assert payload.source_pages == [1]


def test_read_pdf_page_index_tool_uses_explicit_pdf_path(tmp_path) -> None:
    """
    设计目的：锁住 PDF 页索引工具必须显式携带 `pdf_path` 才能工作。
    模块功能：验证工具调用会基于显式传入的 PDF 路径绑定上下文，而不是偷偷依赖上一轮残留状态。
    实现逻辑：先准备临时 PDF 文件和对应页索引，再直接调用工具并检查返回的 `pdf_file_path`。
    可调参数：`tmp_path` 由 `pytest` 提供。
    默认参数及原因：使用最小化的单页索引，原因是这里只关心显式传参和上下文绑定，不关心索引生成质量。
    """

    pdf_page_tools.reset_pdf_page_tool_runtime_state()
    indexing_dir = tmp_path / ".cache" / "20260409_test-company" / "indexing"
    pdf_page_tools.activate_page_index_directory(indexing_dir)
    pdf_path = (tmp_path / "sample.pdf").resolve()
    pdf_path.write_text("stub pdf", encoding="utf-8")

    payload = pdf_page_tools.build_page_index_payload(
        pdf_path,
        [PdfPageIndexEntry(page_number=1, topic="公司概览", matched_topics=["业务"])],
    )
    pdf_page_tools.save_page_index(payload)

    result = ReadPdfPageIndexTool()._run(pdf_path=pdf_path.as_posix())

    assert '"pdf_file_path": "' + pdf_path.as_posix() + '"' in result
