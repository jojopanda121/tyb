from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter

from crewai import Agent
from pydantic import BaseModel, Field

from automated_research_report_generator_v0_1.crew import get_llm
from automated_research_report_generator_v0_1.tools.pdf_page_tools import (
    PdfPageIndexEntry,
    build_page_index_payload,
    default_page_index_path,
    extract_pdf_pages,
    page_index_is_current,
    reset_pdf_page_tool_runtime_state,
    save_page_index,
    set_pdf_context,
)

"""页索引参数"""

PAGE_INDEX_AGENT_ROLE = "PDF 逐页索引专员"  # 默认角色名；功能：限定只做逐页打标；原因：避免提前跨页分析。
PAGE_INDEX_AGENT_GOAL = (  # 默认目标；功能：为后续 agent 提供筛页标签；原因：减少全文盲读。
    "逐页读取 PDF，并为每一页生成一个简短主题标签，帮助后续 agent 快速判断哪些页与自己的分析任务相关。"
)
PAGE_INDEX_AGENT_BACKSTORY = (  # 默认背景；功能：强化单页视角；原因：防止越界总结整份文档。
    "你专门负责按页阅读 PDF。你不会跨页总结，也不会提前做整份文档分析。"
    "你的唯一职责是只根据当前这一页的内容，输出一个非常短、便于筛页的主题标签。"
)
PAGE_INDEX_AGENT_TEMPERATURE = 0.1  # 可调：常用 0-0.3；默认 0.1，原因：标签更稳更短。
PAGE_INDEX_AGENT_VERBOSE = True  # 可调：True/False；默认 True，原因：便于排查慢页与异常页。
PAGE_INDEX_AGENT_ALLOW_DELEGATION = False  # 可调：True/False；默认 False，原因：逐页任务无需委派。
PAGE_INDEX_AGENT_REASONING = False  # 可调：True/False；默认 False，原因：短标签任务不必展开推理。
PAGE_INDEX_AGENT_CACHE = True  # 可调：True/False；默认 True，原因：相同页可复用结果。
PAGE_INDEX_AGENT_TIMEOUT_SECONDS = 20  # 可调：建议 >0；默认 20，原因：短调用不应长时间卡住。
PAGE_INDEX_AGENT_MAX_RETRIES = 5  # 可调：建议 >=0；默认 5，原因：逐页预处理容忍少量波动。

"""页索引任务参数"""

PAGE_INDEX_TOPIC_MAX_CHARS = 10  # 可调：建议 4-15；默认 10，原因：既短又够区分主题。
PAGE_INDEX_UNKNOWN_COMPANY = "未知公司"  # 默认公司占位；功能：补齐 prompt 上下文；原因：上游可能识别失败。
PAGE_INDEX_EMPTY_PAGE_TOPIC = "空白页"  # 默认空白页标签；功能：显式标记无文本页；原因：避免误判异常。
PAGE_INDEX_FORCE_REBUILD_DEFAULT = False  # 可调：True/False；默认 False，原因：优先复用缓存。
PAGE_INDEX_PROMPT_RULES = (  # 默认规则集；功能：约束标签长度和口径；原因：后续筛页更稳定。
    "只输出一个 `topic` 字段。",
    f"`topic` 最多 {PAGE_INDEX_TOPIC_MAX_CHARS} 个字。",
    "只关注当前这一页，不要总结整份文档。",
    "尽量使用具体的业务主题，不要使用空泛表述。",
    "如果本页主要是表格，就概括表格的主题。",
    "如果本页难以判断，就返回一个谨慎、简短的主题。",
    "不要输出解释、前后缀、Markdown 代码块或额外文本。",
)

"""页索引模型"""

class PageTopicSummary(BaseModel):  # 设计：定义单页结构化输出；功能：只保留 topic；默认最小字段集便于稳定解析。
    topic: str = Field(..., description="当前页的简短主题标签")

"""页索引辅助函数"""

def _normalize_topic(topic: str, fallback: str) -> str:  # 设计：统一清洗主题；功能：压成稳定短标签；默认超长截断且空值回退，原因：便于筛页与展示。
    normalized = " ".join((topic or "").split())
    normalized = normalized.replace('"', "").replace("'", "").replace("`", "").strip(" ,.;:()[]{}")
    compact = normalized.replace(" ", "").lower()
    if compact in {"json", "{", "}", "topic", "none", "null"}:
        normalized = ""
    if not normalized:
        normalized = fallback
    return normalized[:PAGE_INDEX_TOPIC_MAX_CHARS]

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
        return cleaned

    if isinstance(data, dict):
        return str(data.get("topic", "") or "")
    return ""

def build_page_topic_task_prompt(page_number: int, page_text: str, company_name: str = "") -> str:  # 设计：集中组装单页 prompt；功能：拼规则、页码和页文；默认 company_name 为空，原因：兼容上游未识别公司名。
    prompt_parts = [
        "你是一个投行初级分析师，负责给一页 PDF 打一个简短主题标签。",
        "",
        "规则：",
    ]
    prompt_parts.extend(f"- {rule}" for rule in PAGE_INDEX_PROMPT_RULES)
    prompt_parts.extend(
        [
            "",
            f"公司名称：{company_name or PAGE_INDEX_UNKNOWN_COMPANY}",
            f"页码：{page_number}",
            f"当前页文本：{page_text}",
        ]
    )
    return "\n".join(prompt_parts).strip()

"""页索引 Agent"""

def create_page_indexer_agent() -> Agent:  # 设计：独立创建索引 agent；功能：供整本 PDF 逐页复用；默认快失败配置，原因：预处理应尽快回退。
    return Agent(
        role=PAGE_INDEX_AGENT_ROLE,  # 默认“PDF 逐页索引专员”；可改成更贴近你内部角色定义的名字。
        goal=PAGE_INDEX_AGENT_GOAL,  # 默认聚焦“逐页筛页”；可改成更强调招股书目录化摘要。
        backstory=PAGE_INDEX_AGENT_BACKSTORY,  # 默认强调“不跨页总结”；可按文档类型微调语境。
        llm=get_llm(
            temperature=PAGE_INDEX_AGENT_TEMPERATURE,
            timeout=PAGE_INDEX_AGENT_TIMEOUT_SECONDS,
            max_retries=PAGE_INDEX_AGENT_MAX_RETRIES,
        ),
        verbose=PAGE_INDEX_AGENT_VERBOSE,
        allow_delegation=PAGE_INDEX_AGENT_ALLOW_DELEGATION,
        reasoning=PAGE_INDEX_AGENT_REASONING,
        cache=PAGE_INDEX_AGENT_CACHE,
        max_retry_limit=PAGE_INDEX_AGENT_MAX_RETRIES,
    )

"""页索引执行"""

def summarize_page_topic(  # 设计：执行单页打标；功能：返回页码与主题映射；可调：company_name 可传空串；默认空页直返固定标签，原因：避免无效调用。
    agent: Agent,
    page_number: int,
    page_text: str,
    company_name: str = "",  # 默认空字符串；上游识别出公司名时传入可提升主题稳定性。
) -> PdfPageIndexEntry:
    fallback = PAGE_INDEX_EMPTY_PAGE_TOPIC if not page_text.strip() else f"第{page_number}页"
    if not page_text.strip():
        return PdfPageIndexEntry(page_number=page_number, topic=fallback)

    prompt = build_page_topic_task_prompt(page_number=page_number, page_text=page_text, company_name=company_name)
    started_at = perf_counter()

    try:
        result = agent.kickoff(prompt, response_format=PageTopicSummary)
        topic = ""
        if getattr(result, "pydantic", None):
            topic = str(result.pydantic.topic or "")
        if not topic:
            topic = _extract_topic_from_raw(getattr(result, "raw", "") or "")
        elapsed_seconds = perf_counter() - started_at
        print(f"[PDF Index] page {page_number} completed in {elapsed_seconds:.1f}s")
    except Exception as exc:
        elapsed_seconds = perf_counter() - started_at
        print(
            "[PDF Index] page "
            f"{page_number} failed after {elapsed_seconds:.1f}s with "
            f"{type(exc).__name__}: {exc}"
        )
        topic = _heuristic_topic(page_text, fallback)

    return PdfPageIndexEntry(
        page_number=page_number,
        topic=_normalize_topic(str(topic), fallback),
    )

"""页索引入口"""

def ensure_pdf_page_index(  # 设计：页索引总入口；功能：优先读缓存再按需重建；可调：company_name、force_rebuild；默认空公司名且不强制重建，原因：兼容性更高。
    pdf_file_path: str,
    company_name: str = "",  # 默认空字符串；传入公司名可让逐页主题标注更贴近业务语境。
    force_rebuild: bool = PAGE_INDEX_FORCE_REBUILD_DEFAULT,  # 默认 False；改 True 时忽略缓存，强制重建整本索引。
) -> str:
    pdf_path = Path(pdf_file_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

    index_path = default_page_index_path(pdf_path)
    if not force_rebuild and page_index_is_current(pdf_path, index_path):
        set_pdf_context(str(pdf_path), str(index_path))
        return str(index_path)

    pages = extract_pdf_pages(pdf_path)
    page_indexer = create_page_indexer_agent()
    page_entries: list[PdfPageIndexEntry] = []

    for page_number, page_text in enumerate(pages, start=1):
        print(f"[PDF Index] summarizing page {page_number}/{len(pages)}")
        page_entries.append(
            summarize_page_topic(
                agent=page_indexer,
                page_number=page_number,
                page_text=page_text,
                company_name=company_name,
            )
        )

    payload = build_page_index_payload(pdf_path, page_entries)
    saved_index_path = save_page_index(payload, index_path)
    set_pdf_context(str(pdf_path), saved_index_path)
    return saved_index_path


"""运行态清理"""

def reset_pdf_preprocessing_runtime_state() -> None:  # 设计：清理预处理运行态；功能：只清进程内缓存；默认保留磁盘缓存，原因：复跑更快。
    reset_pdf_page_tool_runtime_state()


def reset_pdf_indexing_runtime_state() -> None:  # 设计：兼容旧入口；功能：转调新清理函数；默认仅做别名保留，原因：避免旧代码失效。
    reset_pdf_preprocessing_runtime_state()
