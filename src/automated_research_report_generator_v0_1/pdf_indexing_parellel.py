from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from time import perf_counter

from crewai.lite_agent import LiteAgent
from pydantic import BaseModel, Field

from automated_research_report_generator_v0_1.crew import get_llm
from automated_research_report_generator_v0_1.tools.pdf_page_tools import (
    PdfPageIndexEntry,
    build_page_index_payload,
    default_page_index_path,
    extract_pdf_pages,
    page_index_is_current,
    save_page_index,
    set_pdf_context,
)

"""并发版 PDF 逐页索引"""

"""并发页索引 Agent 参数"""

PARALLEL_PAGE_INDEX_AGENT_ROLE = "PDF 并发逐页索引专员"  # 默认角色名；功能：限定 agent 只做逐页打标；原因：避免提前跨页分析。
PARALLEL_PAGE_INDEX_AGENT_GOAL = (  # 默认目标；功能：为后续 agent 提供筛页标签；原因：减少全文盲读。
    "并发读取 PDF 页面，并为每一页生成一个简短主题标签，帮助后续 agent 快速定位相关页。"
)
PARALLEL_PAGE_INDEX_AGENT_BACKSTORY = (  # 默认背景；功能：强化单页视角；原因：防止越界总结整份文档。
    "你专门负责按页阅读 PDF。你不会跨页总结，也不会提前做整份文档分析。"
    "你的唯一职责是只根据当前这一页的内容，输出一个非常短、便于筛页的主题标签。"
)
PARALLEL_PAGE_INDEX_AGENT_TEMPERATURE = 0.1  # 可调：常用 0-0.3；默认 0.1，原因：标签更稳更短。
PARALLEL_PAGE_INDEX_AGENT_VERBOSE = False  # 可调：True/False；默认 False，原因：并发执行时避免日志过多。
PARALLEL_PAGE_INDEX_AGENT_TIMEOUT_SECONDS = 20  # 可调：建议 >0；默认 20，原因：单页调用不应长时间阻塞。
PARALLEL_PAGE_INDEX_MAX_CONCURRENCY_DEFAULT = 4  # 可调：建议 1-8；默认 4，原因：在吞吐和限流风险之间折中。
PARALLEL_PAGE_INDEX_RETRY_LIMIT_DEFAULT = 2  # 可调：建议 >=0；默认 2，原因：预处理容忍短暂波动但不无限重试。
PARALLEL_PAGE_INDEX_RETRY_BASE_DELAY_SECONDS = 2.0  # 可调：建议 >0；默认 2 秒，原因：给限流或瞬时故障留恢复时间。

"""并发页索引任务参数"""

PARALLEL_PAGE_INDEX_TOPIC_MAX_CHARS = 10  # 可调：建议 4-15；默认 10，原因：既短又够区分主题。
PARALLEL_PAGE_INDEX_UNKNOWN_COMPANY = "未知公司"  # 默认公司占位；功能：补齐 prompt 上下文；原因：上游可能识别失败。
PARALLEL_PAGE_INDEX_EMPTY_PAGE_TOPIC = "空白页"  # 默认空白页标签；功能：显式标记无文本页；原因：避免误判异常。
PARALLEL_PAGE_INDEX_FORCE_REBUILD_DEFAULT = False  # 可调：True/False；默认 False，原因：优先复用缓存。
PARALLEL_PAGE_INDEX_PROMPT_RULES = (  # 默认规则集；功能：约束标签长度和口径；原因：后续筛页更稳定。
    "只输出一个 `topic` 字段。",
    f"`topic` 最多 {PARALLEL_PAGE_INDEX_TOPIC_MAX_CHARS} 个字。",
    "只关注当前这一页，不要总结整份文档。",
    "尽量使用具体的业务主题，不要使用空泛表述。",
    "如果本页主要是表格，就概括表格的主题。",
    "如果本页难以判断，就返回一个谨慎、简短的主题。",
    "不要输出解释、前后缀、Markdown 代码块或额外文本。",
)


class ParallelPageTopicSummary(BaseModel):  # 设计：定义单页结构化输出；功能：只保留 topic；默认最小字段集便于稳定解析。
    topic: str = Field(..., description="当前页的简短主题标签")


"""并发页索引辅助函数"""


def _normalize_topic(topic: str, fallback: str) -> str:  # 设计：统一清洗主题；功能：压成稳定短标签；默认超长截断且空值回退，原因：便于筛页与展示。
    normalized = " ".join((topic or "").split())
    normalized = normalized.replace('"', "").replace("'", "").replace("`", "").strip(" ,.;:()[]{}")
    compact = normalized.replace(" ", "").lower()
    if compact in {"json", "{", "}", "topic", "none", "null"}:
        normalized = ""
    if not normalized:
        normalized = fallback
    return normalized[:PARALLEL_PAGE_INDEX_TOPIC_MAX_CHARS]


def _heuristic_topic(page_text: str, fallback: str) -> str:  # 设计：兜底生成主题；功能：失败时取首个像标题的文本；默认回退 fallback，原因：索引流程不能中断。
    for line in page_text.splitlines():
        cleaned = " ".join(line.split()).strip(" ,.;:()[]{}")
        if len(cleaned) >= 2:
            return _normalize_topic(cleaned, fallback)
    return fallback


def _extract_topic_from_raw(raw: str) -> str:  # 设计：兼容兜底解析；功能：从 raw 中提取 topic；默认异常时返回原文或空串，原因：后续还会再清洗。
    cleaned = (raw or "").strip()
    if not cleaned:
        return ""

    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()

    try:
        data = json.loads(cleaned)
    except Exception:
        lowered = cleaned.lower()
        topic_prefixes = ("topic:", "topic：", '"topic":', '"topic"：')
        for prefix in topic_prefixes:
            if lowered.startswith(prefix):
                return cleaned[len(prefix) :].strip()
        return cleaned

    if isinstance(data, dict):
        return str(data.get("topic", "") or "")
    return ""


def _extract_topic_from_result(result: object) -> str:  # 设计：统一解析 LiteAgent 输出；功能：优先读结构化字段，失败再读 raw；原因：兼容不同返回形态。
    topic = ""
    if getattr(result, "pydantic", None):
        topic = str(result.pydantic.topic or "")
    if not topic:
        topic = _extract_topic_from_raw(getattr(result, "raw", "") or "")
    return topic


def _read_int_env(name: str, default: int, minimum: int = 1) -> int:  # 设计：读取整型环境变量；功能：给并发和重试参数留运行时开关；默认非法值回退默认值，原因：减少配置错误影响。
    raw = os.getenv(name, "").strip()
    if not raw:
        return default

    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def get_parallel_page_index_max_concurrency() -> int:  # 设计：暴露并发度配置；功能：统一从环境变量读取；默认回退常量，原因：便于部署侧调优。
    return _read_int_env(
        "PDF_INDEX_PARALLEL_MAX_CONCURRENCY",
        PARALLEL_PAGE_INDEX_MAX_CONCURRENCY_DEFAULT,
        minimum=1,
    )


def get_parallel_page_index_retry_limit() -> int:  # 设计：暴露重试次数配置；功能：统一从环境变量读取；默认回退常量，原因：便于按模型稳定性调优。
    return _read_int_env(
        "PDF_INDEX_PARALLEL_RETRY_LIMIT",
        PARALLEL_PAGE_INDEX_RETRY_LIMIT_DEFAULT,
        minimum=0,
    )


def _is_retryable_page_index_error(exc: Exception) -> bool:  # 设计：识别可重试错误；功能：只对限流和临时故障重试；原因：避免对确定性错误无意义重放。
    error_text = f"{type(exc).__name__}: {exc}".lower()
    retryable_markers = (
        "429",
        "rate limit",
        "too many requests",
        "timeout",
        "timed out",
        "connection",
        "temporarily unavailable",
        "bad gateway",
        "gateway timeout",
        "service unavailable",
        "502",
        "503",
        "504",
    )
    return any(marker in error_text for marker in retryable_markers)


def build_parallel_page_topic_task_prompt(page_number: int, page_text: str, company_name: str = "") -> str:  # 设计：集中组装单页 prompt；功能：拼规则、页码和页文；默认 company_name 为空，原因：兼容上游未识别公司名。
    prompt_parts = [
        "你是一个投行初级分析师，负责给一页 PDF 打一个简短主题标签。",
        "",
        "规则：",
    ]
    prompt_parts.extend(f"- {rule}" for rule in PARALLEL_PAGE_INDEX_PROMPT_RULES)
    prompt_parts.extend(
        [
            "",
            f"公司名称：{company_name or PARALLEL_PAGE_INDEX_UNKNOWN_COMPANY}",
            f"页码：{page_number}",
            f"当前页文本：{page_text}",
        ]
    )
    return "\n".join(prompt_parts).strip()


"""并发页索引 Agent"""


def create_parallel_page_indexer_lite_agent() -> LiteAgent:  # 设计：独立创建并发索引 agent；功能：为每个异步任务提供轻量单页执行器；默认快失败配置，原因：预处理应尽快回退。
    return LiteAgent(
        role=PARALLEL_PAGE_INDEX_AGENT_ROLE,
        goal=PARALLEL_PAGE_INDEX_AGENT_GOAL,
        backstory=PARALLEL_PAGE_INDEX_AGENT_BACKSTORY,
        llm=get_llm(
            temperature=PARALLEL_PAGE_INDEX_AGENT_TEMPERATURE,
            timeout=PARALLEL_PAGE_INDEX_AGENT_TIMEOUT_SECONDS,
            max_retries=0,
        ),
        verbose=PARALLEL_PAGE_INDEX_AGENT_VERBOSE,
        max_execution_time=PARALLEL_PAGE_INDEX_AGENT_TIMEOUT_SECONDS,
    )


"""并发页索引执行"""


async def summarize_page_topic_with_lite_agent_async(  # 设计：执行单页异步打标；功能：返回页码与主题映射；默认空页直返固定标签，原因：避免无效调用。
    page_number: int,
    page_text: str,
    company_name: str,
    total_pages: int,
    semaphore: asyncio.Semaphore,
) -> PdfPageIndexEntry:
    fallback = PARALLEL_PAGE_INDEX_EMPTY_PAGE_TOPIC if not page_text.strip() else f"第{page_number}页"
    if not page_text.strip():
        return PdfPageIndexEntry(page_number=page_number, topic=fallback)

    prompt = build_parallel_page_topic_task_prompt(
        page_number=page_number,
        page_text=page_text,
        company_name=company_name,
    )
    retry_limit = get_parallel_page_index_retry_limit()

    async with semaphore:
        for attempt in range(retry_limit + 1):
            started_at = perf_counter()
            try:
                # CrewAI 1.12.2 下 LiteAgent 的 response_format 会让完成事件携带
                # Pydantic 对象，随后触发 LiteAgentExecutionCompletedEvent.output
                # 的字符串校验失败。这里改为纯文本返回，再由本模块自行解析。
                result = await create_parallel_page_indexer_lite_agent().kickoff_async(
                    prompt,
                )
                topic = _extract_topic_from_result(result)
                elapsed_seconds = perf_counter() - started_at
                print(
                    f"[PDF Index Parallel] page {page_number}/{total_pages} "
                    f"completed in {elapsed_seconds:.1f}s"
                )
                return PdfPageIndexEntry(
                    page_number=page_number,
                    topic=_normalize_topic(str(topic), fallback),
                )
            except Exception as exc:
                elapsed_seconds = perf_counter() - started_at
                should_retry = attempt < retry_limit and _is_retryable_page_index_error(exc)
                if should_retry:
                    delay_seconds = PARALLEL_PAGE_INDEX_RETRY_BASE_DELAY_SECONDS * (2 ** attempt)
                    print(
                        "[PDF Index Parallel] page "
                        f"{page_number}/{total_pages} failed after {elapsed_seconds:.1f}s with "
                        f"{type(exc).__name__}: {exc}. "
                        f"Retrying in {delay_seconds:.1f}s "
                        f"({attempt + 1}/{retry_limit})"
                    )
                    await asyncio.sleep(delay_seconds)
                    continue

                print(
                    "[PDF Index Parallel] page "
                    f"{page_number}/{total_pages} failed after {elapsed_seconds:.1f}s with "
                    f"{type(exc).__name__}: {exc}"
                )
                return PdfPageIndexEntry(
                    page_number=page_number,
                    topic=_heuristic_topic(page_text, fallback),
                )

    return PdfPageIndexEntry(page_number=page_number, topic=fallback)


async def summarize_pages_with_parallel_lite_agent(  # 设计：并发执行整本 PDF 打标；功能：受限并发汇总全部页结果；默认保持页序，原因：便于直接落盘成索引。
    pages: list[str],
    company_name: str = "",
    max_concurrency: int = PARALLEL_PAGE_INDEX_MAX_CONCURRENCY_DEFAULT,
) -> list[PdfPageIndexEntry]:
    semaphore = asyncio.Semaphore(max_concurrency)
    total_pages = len(pages)
    tasks = [
        summarize_page_topic_with_lite_agent_async(
            page_number=page_number,
            page_text=page_text,
            company_name=company_name,
            total_pages=total_pages,
            semaphore=semaphore,
        )
        for page_number, page_text in enumerate(pages, start=1)
    ]
    return await asyncio.gather(*tasks)


"""并发页索引入口"""


def ensure_pdf_page_index_parallel(  # 设计：并发页索引总入口；功能：优先读缓存再按需重建；可调：company_name、force_rebuild；默认空公司名且不强制重建，原因：兼容性更高。
    pdf_file_path: str,
    company_name: str = "",
    force_rebuild: bool = PARALLEL_PAGE_INDEX_FORCE_REBUILD_DEFAULT,
) -> str:
    pdf_path = Path(pdf_file_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

    index_path = default_page_index_path(pdf_path)
    if not force_rebuild and page_index_is_current(pdf_path, index_path):
        set_pdf_context(str(pdf_path), str(index_path))
        return str(index_path)

    pages = extract_pdf_pages(pdf_path)
    max_concurrency = get_parallel_page_index_max_concurrency()
    print(
        "[PDF Index Parallel] using bounded concurrency "
        f"(max_concurrency={max_concurrency}, pages={len(pages)})"
    )
    page_entries = asyncio.run(
        summarize_pages_with_parallel_lite_agent(
            pages=pages,
            company_name=company_name,
            max_concurrency=max_concurrency,
        )
    )

    payload = build_page_index_payload(pdf_path, page_entries)
    saved_index_path = save_page_index(payload, index_path)
    set_pdf_context(str(pdf_path), saved_index_path)
    return saved_index_path
