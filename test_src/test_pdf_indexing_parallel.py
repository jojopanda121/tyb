from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import fitz
import pytest

pytest.skip("legacy v0.1 parallel indexing script is not part of the v0.2 package layout", allow_module_level=True)

"""并发页索引测试脚本"""

TEST_CACHE_DIR = Path.cwd() / ".cache" / "pdf_indexing_parallel_test"  # 默认测试缓存目录；功能：集中保存样例 PDF；原因：避免污染正式输出目录。
DEFAULT_COMPANY_NAME = "并发测试公司"  # 默认公司名；功能：仅在内置样例 PDF 或识别失败时兜底；原因：真实 PDF 优先走 document_metadata 自动识别。


@dataclass
class MockRunStats:  # 设计：记录 mock 运行统计；功能：校验并发峰值和调用次数；默认只保留最小统计字段，原因：测试目标聚焦在并发调度。
    """
    设计目的：集中保存 mock 运行过程里的计数信息。
    模块功能：记录调用次数、当前并发数和并发峰值，供后面断言复用。
    实现逻辑：用 dataclass 承载三个整数计数位，测试前后统一读写同一个对象。
    可调参数：无。
    默认参数及原因：三个字段默认都从 0 开始，原因是每轮测试都需要从干净状态起步。
    """
    kickoff_calls: int = 0
    active_calls: int = 0
    max_active_calls: int = 0


MOCK_STATS = MockRunStats()


def _reset_mock_stats() -> None:  # 设计：重置 mock 统计；功能：避免多次运行互相污染；默认每次测试前清零，原因：断言依赖精确计数。
    """
    设计目的：保证每轮 mock 测试都从同一套初始统计值开始。
    模块功能：把 `MOCK_STATS` 里的三个计数器全部清零。
    实现逻辑：直接逐项覆盖现有统计值，不保留上一轮残留结果。
    可调参数：无。
    默认参数及原因：默认全部重置为 0，原因是并发和调用断言依赖精确计数。
    """
    MOCK_STATS.kickoff_calls = 0
    MOCK_STATS.active_calls = 0
    MOCK_STATS.max_active_calls = 0


def _ensure_test_cache_dir() -> Path:  # 设计：统一准备测试目录；功能：创建并返回缓存路径；默认缺失时自动创建，原因：减少外部准备步骤。
    """
    设计目的：给测试样例文件提供固定缓存目录。
    模块功能：确保测试缓存目录存在，并把路径返回给调用方。
    实现逻辑：先按固定位置创建目录，再返回同一个 `Path` 对象。
    可调参数：无。
    默认参数及原因：默认使用 `TEST_CACHE_DIR`，原因是测试文件应和正式输出隔离。
    """
    TEST_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return TEST_CACHE_DIR


def build_sample_pdf(pdf_path: Path | None = None) -> Path:  # 设计：生成最小样例 PDF；功能：覆盖普通页、表述清晰页和空白页；默认使用随机文件名，原因：避免历史测试文件占锁。
    """
    设计目的：给并发页索引测试准备一份稳定、可复现的最小 PDF。
    模块功能：生成包含普通页、财务页、空白页和风险页的测试样例。
    实现逻辑：先准备缓存目录，再逐页写入预设文本，最后保存到目标路径。
    可调参数：`pdf_path`。
    默认参数及原因：默认使用随机文件名，原因是避免历史测试文件占锁或互相覆盖。
    """
    _ensure_test_cache_dir()
    pdf_path = pdf_path or (TEST_CACHE_DIR / f"sample_parallel_index_test_{uuid4().hex}.pdf")
    document = fitz.open()
    sample_pages = [
        "Company Overview\nMain business: industrial robot controllers\nCore product: motion platform",
        "Financial Summary\nRevenue growth: 35 percent\nGross margin: 42 percent",
        "",
        "Risk Factors\nCustomer concentration is high\nSupply chain costs may fluctuate",
    ]

    try:
        for page_text in sample_pages:
            page = document.new_page()
            if page_text:
                page.insert_text((72, 72), page_text, fontsize=12)
        document.save(pdf_path)
    finally:
        document.close()

    return pdf_path


def resolve_company_name(  # 设计：统一解析测试用公司名；功能：把命令行输入、元数据识别和默认值串起来；默认真实 PDF 优先自动识别，原因：减少手工维护 default_company_name。
    pdf_path: Path,
    company_name: str,
) -> str:
    """
    设计目的：统一测试脚本里公司名的取值规则。
    模块功能：在手动传值、样例默认值和真实 PDF 元数据之间做选择。
    实现逻辑：先看命令行传值，再判断是否是样例 PDF，最后才尝试走文档元数据识别。
    可调参数：`pdf_path` 和 `company_name`。
    默认参数及原因：默认真实 PDF 优先走元数据识别，原因是这样更接近真实运行方式。
    """
    normalized = " ".join((company_name or "").split()).strip()
    if normalized:
        return normalized

    if str(pdf_path).startswith(str(TEST_CACHE_DIR)):
        return DEFAULT_COMPANY_NAME

    metadata = ensure_pdf_document_metadata(str(pdf_path))
    detected_company_name = " ".join(str(metadata.get("company_name", "")).split()).strip()
    return detected_company_name or DEFAULT_COMPANY_NAME


class MockLiteAgent:  # 设计：替身 LiteAgent；功能：脱离外网验证并发逻辑、索引落盘和缓存复用；默认固定延时和规则匹配，原因：让测试结果可重复。
    """
    设计目的：在不依赖真实模型的情况下稳定验证并发页索引流程。
    模块功能：模拟异步调用、记录并发统计，并返回固定结构的主题结果。
    实现逻辑：通过固定延时和关键词匹配模拟模型输出，保证每轮测试结果可复现。
    可调参数：无。
    默认参数及原因：默认走固定规则匹配，原因是 mock 测试要优先保证可重复和可断言。
    """

    async def kickoff_async(  # 设计：模拟异步调用；功能：制造受控并发并返回结构化结果；默认忽略 response_format，原因：mock 只验证调用链是否正确。
        self,
        prompt: str,
        response_format: type[ParallelPageTopicSummary] | None = None,
    ):
        """
        设计目的：模拟真实 LiteAgent 的异步入口。
        模块功能：记录调用统计、制造受控等待，并返回带 `pydantic` 和 `raw` 的假结果。
        实现逻辑：进入时先累计计数，等待固定时间后按 prompt 关键词生成主题，最后回收活跃计数。
        可调参数：`prompt` 和 `response_format`。
        默认参数及原因：默认忽略 `response_format` 的具体差异，原因是这里主要验证调用链和并发行为。
        """
        MOCK_STATS.kickoff_calls += 1
        MOCK_STATS.active_calls += 1
        MOCK_STATS.max_active_calls = max(MOCK_STATS.max_active_calls, MOCK_STATS.active_calls)

        try:
            await asyncio.sleep(0.2)
            topic = self._infer_topic_from_prompt(prompt)
            return type(
                "MockLiteAgentResult",
                (),
                {
                    "pydantic": ParallelPageTopicSummary(topic=topic),
                    "raw": json.dumps({"topic": topic}, ensure_ascii=False),
                },
            )()
        finally:
            MOCK_STATS.active_calls -= 1

    @staticmethod
    def _infer_topic_from_prompt(prompt: str) -> str:  # 设计：用 prompt 反推主题；功能：模拟模型给出稳定 topic；默认命中关键词即返回固定标签，原因：便于精确断言。
        """
        设计目的：让 mock 返回值保持稳定、可预测。
        模块功能：按 prompt 里的关键词把页面内容映射成预设主题。
        实现逻辑：依次匹配财务、风险和概览关键词，命中后返回固定标签，否则回退为 `other`。
        可调参数：`prompt`。
        默认参数及原因：默认未命中时返回 `other`，原因是测试需要一个明确兜底标签。
        """
        if "Financial Summary" in prompt or "Revenue growth" in prompt or "Gross margin" in prompt:
            return "finance"
        if "Risk Factors" in prompt or "Customer concentration" in prompt or "Supply chain" in prompt:
            return "risk"
        if "Company Overview" in prompt or "Main business" in prompt or "Core product" in prompt:
            return "overview"
        return "other"


def run_mock_test(  # 设计：mock 模式总入口；功能：验证并发上限、主题输出和缓存复用；默认强制重建一次再命中缓存一次，原因：一次覆盖两条主路径。
    pdf_path: Path,
    max_concurrency: int,
    company_name: str,
) -> dict[str, object]:
    """
    设计目的：用 mock 模式一次覆盖并发、索引结果和缓存复用三条主路径。
    模块功能：执行并发页索引、读取结果、做关键断言，并返回一份便于观察的结果字典。
    实现逻辑：先替换真实 agent 工厂和环境变量，再跑强制重建与缓存命中两轮，最后恢复现场。
    可调参数：`pdf_path`、`max_concurrency` 和 `company_name`。
    默认参数及原因：默认会先强制重建再命中缓存，原因是这样一轮就能覆盖最关键的两条路径。
    """
    import automated_research_report_generator_v0_1.pdf_indexing_parellel as parallel_module

    _reset_mock_stats()
    original_factory = parallel_module.create_parallel_page_indexer_lite_agent
    original_max_concurrency = os.environ.get("PDF_INDEX_PARALLEL_MAX_CONCURRENCY")
    original_retry_limit = os.environ.get("PDF_INDEX_PARALLEL_RETRY_LIMIT")

    # 并发数量调这里：
    # 1. 当前函数参数 max_concurrency 是测试层显式入口。
    # 2. 这里把它写入 PDF_INDEX_PARALLEL_MAX_CONCURRENCY，驱动被测脚本读取相同配置。
    os.environ["PDF_INDEX_PARALLEL_MAX_CONCURRENCY"] = str(max_concurrency)
    os.environ["PDF_INDEX_PARALLEL_RETRY_LIMIT"] = "0"
    parallel_module.create_parallel_page_indexer_lite_agent = lambda: MockLiteAgent()

    try:
        started_at = perf_counter()
        index_path = ensure_pdf_page_index_parallel(
            pdf_file_path=str(pdf_path),
            company_name=company_name,
            force_rebuild=True,
        )
        elapsed_seconds = perf_counter() - started_at
        index_data = load_page_index(index_path)
        topics = [page["topic"] for page in index_data["pages"]]

        assert index_data["page_count"] == 4, "样例 PDF 应生成 4 条页索引。"
        assert topics == ["overview", "finance", "空白页", "risk"], f"页主题不符合预期: {topics}"
        assert MOCK_STATS.kickoff_calls == 3, f"非空白页应只触发 3 次 LLM 调用，实际 {MOCK_STATS.kickoff_calls}"
        assert 1 < MOCK_STATS.max_active_calls <= max_concurrency, (
            f"并发峰值应位于 2..{max_concurrency} 之间，实际 {MOCK_STATS.max_active_calls}"
        )

        calls_after_build = MOCK_STATS.kickoff_calls
        cached_index_path = ensure_pdf_page_index_parallel(
            pdf_file_path=str(pdf_path),
            company_name=company_name,
            force_rebuild=False,
        )
        assert cached_index_path == index_path, "命中缓存时应返回同一个索引路径。"
        assert MOCK_STATS.kickoff_calls == calls_after_build, "命中缓存时不应再次触发 LLM 调用。"

        return {
            "mode": "mock",
            "pdf_path": str(pdf_path),
            "index_path": index_path,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "page_count": index_data["page_count"],
            "topics": topics,
            "kickoff_calls": MOCK_STATS.kickoff_calls,
            "max_active_calls": MOCK_STATS.max_active_calls,
        }
    finally:
        parallel_module.create_parallel_page_indexer_lite_agent = original_factory
        if original_max_concurrency is None:
            os.environ.pop("PDF_INDEX_PARALLEL_MAX_CONCURRENCY", None)
        else:
            os.environ["PDF_INDEX_PARALLEL_MAX_CONCURRENCY"] = original_max_concurrency
        if original_retry_limit is None:
            os.environ.pop("PDF_INDEX_PARALLEL_RETRY_LIMIT", None)
        else:
            os.environ["PDF_INDEX_PARALLEL_RETRY_LIMIT"] = original_retry_limit


def run_live_test(  # 设计：live 模式总入口；功能：对真实 PDF 执行完整并发页索引；默认只校验索引存在且 topic 非空，原因：真实模型输出允许轻微波动。
    pdf_path: Path,
    max_concurrency: int,
    company_name: str,
) -> dict[str, object]:
    """
    设计目的：给真实 LiteAgent 路径提供一层最小冒烟测试。
    模块功能：执行真实并发页索引流程，并验证至少产出可用的页索引和主题结果。
    实现逻辑：先写入并发度配置，再跑一次强制重建，最后读取索引并做最小断言。
    可调参数：`pdf_path`、`max_concurrency` 和 `company_name`。
    默认参数及原因：默认只做最小正确性检查，原因是实时模型输出允许一定波动。
    """
    original_max_concurrency = os.environ.get("PDF_INDEX_PARALLEL_MAX_CONCURRENCY")
    try:
        # 并发数量调这里：
        # live 模式和 mock 模式一样，最终都是通过 PDF_INDEX_PARALLEL_MAX_CONCURRENCY
        # 传给 pdf_indexing_parellel.py。
        os.environ["PDF_INDEX_PARALLEL_MAX_CONCURRENCY"] = str(max_concurrency)
        started_at = perf_counter()
        index_path = ensure_pdf_page_index_parallel(
            pdf_file_path=str(pdf_path),
            company_name=company_name,
            force_rebuild=True,
        )
        elapsed_seconds = perf_counter() - started_at
        index_data = load_page_index(index_path)
        topics = [page["topic"] for page in index_data["pages"]]

        assert index_data["page_count"] >= 1, "真实模式至少应产出 1 条页索引。"
        assert all(str(topic).strip() for topic in topics), "真实模式下所有 topic 都应非空。"

        return {
            "mode": "live",
            "pdf_path": str(pdf_path),
            "index_path": index_path,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "page_count": index_data["page_count"],
            "topics": topics,
        }
    finally:
        if original_max_concurrency is None:
            os.environ.pop("PDF_INDEX_PARALLEL_MAX_CONCURRENCY", None)
        else:
            os.environ["PDF_INDEX_PARALLEL_MAX_CONCURRENCY"] = original_max_concurrency


def parse_args() -> argparse.Namespace:  # 设计：集中解析命令行参数；功能：暴露模式、PDF 路径和并发度；默认 mock + 2 并发，原因：本地调试更稳。
    """
    设计目的：统一管理测试脚本的命令行入口。
    模块功能：解析模式、PDF 路径、公司名和并发度四类参数。
    实现逻辑：先定义参数选项和默认值，再直接返回 argparse 解析结果。
    可调参数：命令行里的 `--mode`、`--pdf`、`--company-name` 和 `--max-concurrency`。
    默认参数及原因：默认走 `mock` 且并发度为 2，原因是本地调试更稳，也更容易看清行为。
    """
    parser = argparse.ArgumentParser(description="Test the parallel PDF indexing pipeline.")
    parser.add_argument("--mode", choices=["mock", "live"], default="mock", help="Choose mock or real LiteAgent mode.")
    parser.add_argument("--pdf", type=Path, default=None, help="Optional existing PDF path. If omitted, a sample PDF is generated.")
    parser.add_argument(
        "--company-name",
        default="",
        help="Optional company name injected into the page-topic prompt. Leave empty to auto-detect from document_metadata.",
    )
    # 并发数量也可以从命令行调这里：
    # 例如 --max-concurrency 4。
    # 这个参数会传入 run_mock_test()/run_live_test()，再写入环境变量给被测脚本读取。
    parser.add_argument("--max-concurrency", type=int, default=2, help="Max parallel page summarization concurrency.")
    return parser.parse_args()


def main() -> int:  # 设计：脚本入口；功能：按参数选择 mock 或 live 测试；默认未传 PDF 时自动生成样例，原因：让脚本开箱即用。
    """
    设计目的：给测试脚本提供统一入口，方便直接从命令行运行。
    模块功能：解析参数、准备 PDF、选择 mock 或 live 模式，并输出测试结果。
    实现逻辑：先拿到参数和 PDF 路径，再根据模式分发到对应测试函数，最后打印 JSON 和 PASS。
    可调参数：实际运行时的命令行参数。
    默认参数及原因：默认未传 PDF 时自动生成样例文件，原因是脚本应尽量开箱即用。
    """
    args = parse_args()
    pdf_path = args.pdf.resolve() if args.pdf else build_sample_pdf()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")
    company_name = resolve_company_name(pdf_path=pdf_path, company_name=args.company_name)

    result = (
        run_mock_test(pdf_path=pdf_path, max_concurrency=args.max_concurrency, company_name=company_name)
        if args.mode == "mock"
        else run_live_test(pdf_path=pdf_path, max_concurrency=args.max_concurrency, company_name=company_name)
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
