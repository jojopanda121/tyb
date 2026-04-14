from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path
from time import perf_counter, sleep

from crewai import Agent

from automated_research_report_generator.flow.common import append_preprocess_log_line
from automated_research_report_generator.llm_config import get_heavy_llm
from automated_research_report_generator.tools.pdf_page_tools import (
    PdfPageIndexEntry,
    build_page_index_payload,
    default_page_index_path,
    extract_pdf_pages,
    page_index_is_current,
    reset_pdf_page_tool_runtime_state,
    save_page_index,
    set_pdf_context,
)

# 设计目的：先为每份 PDF 生成稳定的逐页主题索引，降低后续 crew 在长文档里定位页面的成本。
# 模块功能：读取 PDF、为每一页生成短主题概括和字典主题归类、保存索引，并同步运行时 PDF 上下文。
# 实现逻辑：对外继续暴露 `ensure_pdf_page_index()`，内部使用“受限并发 + 重试 + 失败兜底”的流水线。
# 可调参数：agent 参数、并发数、重试次数、主题长度和 `force_rebuild`。
# 默认参数及原因：默认优先复用已有索引，原因是同一份 PDF 在一次运行中通常不会频繁变化。

PAGE_INDEX_AGENT_ROLE = "PDF 页面主题索引专员"
PAGE_INDEX_AGENT_GOAL = (
    "逐页阅读 PDF，为每一页生成 15 字以内的主题概括，并从固定主题字典中选择 1 到 2 个最接近的主题。"
)
PAGE_INDEX_AGENT_BACKSTORY = (
    "你专门负责把长 PDF 拆成可检索的页面主题索引。"
    "你不写长分析，只输出结构稳定、便于筛页的主题概括和主题归类。"
)
PAGE_INDEX_AGENT_TEMPERATURE = 0.1
PAGE_INDEX_AGENT_VERBOSE = True
PAGE_INDEX_AGENT_ALLOW_DELEGATION = False
PAGE_INDEX_AGENT_REASONING = False
PAGE_INDEX_AGENT_CACHE = True
PAGE_INDEX_AGENT_TIMEOUT_SECONDS = 20
PAGE_INDEX_AGENT_MAX_RETRIES = 5

PAGE_INDEX_TOPIC_MAX_CHARS = 15
PAGE_INDEX_MATCHED_TOPICS_MAX_COUNT = 2
PAGE_INDEX_UNKNOWN_COMPANY = "未知公司"
PAGE_INDEX_EMPTY_PAGE_TOPIC = "空白页"
PAGE_INDEX_EMPTY_PAGE_MATCHED_TOPICS = ["其他"]
PAGE_INDEX_FORCE_REBUILD_DEFAULT = False
PAGE_INDEX_MAX_CONCURRENCY_DEFAULT = 500  # 最大并发数量当前设为 500，优先提升整份 PDF 的处理速度。
PAGE_INDEX_RETRY_LIMIT_DEFAULT = 2
PAGE_INDEX_RETRY_BASE_DELAY_SECONDS = 2.0
PAGE_INDEX_ALLOWED_TOPICS = (
    "行业",
    "业务",
    "产品",
    "技术",
    "财务",
    "历史",
    "公司治理",
    "发行方案",
    "估值",
    "股东",
    "风险",
    "市场",
    "竞争",
    "募投项目",
    "资产",
    "目录",
    "声明",
    "封面",
    "其他",
)
PAGE_INDEX_PROMPT_RULES = (
    f"给出一个中文短概括，长度不超过 {PAGE_INDEX_TOPIC_MAX_CHARS} 个字。",
    f"再从固定主题字典里选 1 到 {PAGE_INDEX_MATCHED_TOPICS_MAX_COUNT} 个最接近的主题。",
    "主题不要重复，不要自造新主题。",
    "只输出一个 JSON 对象，不要输出解释、前后缀或 Markdown 代码块。",
    "JSON 里只保留 `topic` 和 `matched_topics` 两个字段。",
    "如果页面主要是目录、封面或声明，也要照样概括并归类。",
    "如果难以判断，就优先选择最接近的主题；实在不合适时使用 `其他`。",
)
RETRYABLE_ERROR_KEYWORDS = (
    "request timeout",
    "timeout",
    "timed out",
    "read timeout",
    "connect timeout",
    "readtimeout",
    "connecttimeout",
    "408",
    "rate limit",
    "too many requests",
    "429",
    "bad gateway",
    "502",
    "temporarily unavailable",
    "service unavailable",
    "503",
    "gateway timeout",
    "504",
    "temporarily overloaded",
    "overloaded",
    "overloaded_error",
    "remoteprotocolerror",
    "protocolerror",
    "connection",
    "connect",
    "network",
    "connection reset",
    "reset by peer",
    "connection aborted",
    "connection closed",
    "ssl",
    "tls",
    "apierror",
    "apiconnectionerror",
    "internalservererror",
    "server error",
)
TOPIC_HEURISTIC_KEYWORDS = {
    "目录": ("目录", "章节", "页码", "目 录"),
    "声明": ("声明", "承诺", "提示", "免责", "说明书声明"),
    "封面": ("招股说明书", "首次公开发行", "发行人", "股票代码"),
    "行业": ("行业", "赛道", "产业链", "上下游"),
    "业务": ("业务", "主营", "收入来源", "商业模式"),
    "产品": ("产品", "方案", "服务", "型号"),
    "技术": ("技术", "研发", "专利", "工艺"),
    "财务": ("财务", "收入", "利润", "现金流", "毛利率", "资产负债"),
    "历史": ("历史", "沿革", "设立", "变更", "发展历程"),
    "公司治理": ("治理", "董事", "监事", "高管", "独立董事"),
    "发行方案": ("发行", "募集", "承销", "发行价", "询价"),
    "估值": ("估值", "市盈率", "可比公司", "定价"),
    "股东": ("股东", "持股", "控股股东", "实际控制人", "股权"),
    "风险": ("风险", "不确定", "波动", "依赖", "诉讼"),
    "市场": ("市场", "客户", "需求", "销售", "区域"),
    "竞争": ("竞争", "竞争对手", "壁垒", "替代"),
    "募投项目": ("募投", "募集资金", "投资项目", "建设项目"),
    "资产": ("资产", "设备", "厂房", "土地", "无形资产"),
}
PDF_INDEX_LOG_LOCK = Lock()


def log_pdf_index_message(message: str) -> None:
    """
    设计目的：给页面索引阶段提供统一的控制台和文件双写日志入口。
    模块功能：把同一条日志同时打印到终端，并追加到预处理 latest 日志文件。
    实现逻辑：先 `print()` 保留实时可见性，再调用预处理日志追加函数落盘。
    可调参数：`message`。
    默认参数及原因：默认双写，原因是页面索引通常耗时较长，既要实时看进度，也要事后可回放。
    """

    with PDF_INDEX_LOG_LOCK:
        print(message, flush=True)
        append_preprocess_log_line(message)


def _read_int_env(name: str, default: int, minimum: int = 1) -> int:
    """
    设计目的：统一读取并校正整数环境变量。
    模块功能：把字符串环境变量转换成合法整数，并在异常时回退默认值。
    实现逻辑：先读环境变量，再做 `int()` 转换和下限裁剪。
    可调参数：环境变量名、默认值和最小值。
    默认参数及原因：默认最小值是 1，原因是并发数和重试次数都不应小于可执行下限。
    """

    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return max(minimum, default)

    try:
        value = int(raw_value)
    except ValueError:
        return max(minimum, default)

    return max(minimum, value)


def get_page_index_max_concurrency() -> int:
    """
    设计目的：给页面索引并发数提供统一入口。
    模块功能：返回当前运行时允许的最大并发页数。
    实现逻辑：优先读取 `PDF_INDEX_MAX_CONCURRENCY`，否则回退到模块默认值。
    可调参数：环境变量 `PDF_INDEX_MAX_CONCURRENCY`。
    默认参数及原因：默认值是 100，原因是在速度、API 压力和失败隔离之间取平衡。
    """

    return _read_int_env("PDF_INDEX_MAX_CONCURRENCY", PAGE_INDEX_MAX_CONCURRENCY_DEFAULT, minimum=1)


def get_page_index_retry_limit() -> int:
    """
    设计目的：给页面索引失败重试次数提供统一入口。
    模块功能：返回单页主题生成时允许的最大重试次数。
    实现逻辑：优先读取 `PDF_INDEX_RETRY_LIMIT`，否则回退到模块默认值。
    可调参数：环境变量 `PDF_INDEX_RETRY_LIMIT`。
    默认参数及原因：默认值是 2，原因是短任务不值得无限重试，但也需要留出瞬时失败恢复空间。
    """

    return _read_int_env("PDF_INDEX_RETRY_LIMIT", PAGE_INDEX_RETRY_LIMIT_DEFAULT, minimum=0)


def _is_retryable_page_index_error(exc: Exception) -> bool:
    """
    设计目的：区分哪些异常值得重试，哪些异常应直接兜底。
    模块功能：根据异常类型名和异常文本判断是否属于瞬时错误。
    实现逻辑：把异常名和消息转成小写，再匹配一组网络、限流、超时关键词。
    可调参数：异常对象和关键词集合 `RETRYABLE_ERROR_KEYWORDS`。
    默认参数及原因：默认只覆盖常见瞬时错误，原因是过度重试会拖慢整份 PDF 的处理。
    """

    error_text = f"{type(exc).__name__}: {exc}".lower()
    return any(keyword in error_text for keyword in RETRYABLE_ERROR_KEYWORDS)


def _normalize_topic(topic: str, fallback: str) -> str:
    """
    设计目的：统一清洗页面短概括文本。
    模块功能：去掉多余空白和无意义包裹符号，并在为空时回退到兜底概括。
    实现逻辑：先压缩空白，再做轻量清洗，最后做空值兜底和长度裁剪。
    可调参数：原始概括和兜底概括。
    默认参数及原因：最终长度裁剪到 15 个字，原因是这里强调短概括而不是完整摘要。
    """

    normalized = " ".join((topic or "").split())
    normalized = normalized.replace('"', "").replace("'", "").replace("`", "")
    normalized = normalized.strip(" ，,.;:()[]{}")
    if not normalized:
        normalized = fallback
    return normalized[:PAGE_INDEX_TOPIC_MAX_CHARS]


def _normalize_matched_topics(topics: list[str] | tuple[str, ...] | str, fallback: list[str]) -> list[str]:
    """
    设计目的：统一清洗主题归类结果。
    模块功能：过滤非法主题、去重、限数量，并在为空时回退到兜底主题列表。
    实现逻辑：先把输入转成列表，再逐项清洗和去重，最后做空值兜底和数量裁剪。
    可调参数：原始主题列表或字符串，以及兜底主题列表。
    默认参数及原因：最多保留 2 个主题，原因是用户要求主题归类不超过 2 个。
    """

    raw_items = topics if isinstance(topics, (list, tuple)) else [topics]
    normalized_items: list[str] = []
    for item in raw_items:
        cleaned = " ".join(str(item or "").split()).strip(" ，,.;:()[]{}")
        if not cleaned or cleaned not in PAGE_INDEX_ALLOWED_TOPICS:
            continue
        if cleaned in normalized_items:
            continue
        normalized_items.append(cleaned)
        if len(normalized_items) >= PAGE_INDEX_MATCHED_TOPICS_MAX_COUNT:
            break

    if normalized_items:
        return normalized_items
    return fallback[:PAGE_INDEX_MATCHED_TOPICS_MAX_COUNT]


def _strip_json_fence(raw: str) -> str:
    """
    设计目的：在解析模型原始输出前，先去掉常见的 Markdown 包裹层。
    模块功能：移除 ```json 代码块边界，保留其中真正的 JSON 或纯文本内容。
    实现逻辑：先做首尾裁剪，再用正则去掉头尾代码块标记。
    可调参数：`raw`。
    默认参数及原因：默认兼容 Markdown 代码块，原因是模型常把 JSON 包在代码块里返回。
    """

    cleaned = (raw or "").strip()
    if not cleaned:
        return ""

    cleaned = re.sub(r"^```[\w-]*\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _try_parse_page_summary_dict(raw: str) -> dict[str, object] | None:
    """
    设计目的：把模型原始输出尽量还原成可读取的字典。
    模块功能：优先解析完整 JSON；若外层夹杂说明文本，再尝试提取首个 JSON 对象。
    实现逻辑：先清理代码块，再按“完整字符串 -> 花括号子串”顺序尝试 `json.loads()`。
    可调参数：`raw`。
    默认参数及原因：默认只接受字典，原因是当前索引结果天然应该是键值结构。
    """

    cleaned = _strip_json_fence(raw)
    if not cleaned:
        return None

    candidates = [cleaned]
    json_start = cleaned.find("{")
    json_end = cleaned.rfind("}")
    if 0 <= json_start < json_end:
        embedded_candidate = cleaned[json_start : json_end + 1]
        if embedded_candidate not in candidates:
            candidates.append(embedded_candidate)

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed

    return None


def _extract_page_summary_from_raw(
    raw: str,
    fallback_topic: str,
    fallback_topics: list[str],
) -> tuple[str, list[str]]:
    """
    设计目的：在不依赖 `response_format` 的前提下，从原始文本中还原页面摘要结果。
    模块功能：优先解析 JSON；解析失败时，再从普通文本里尽量提取 `topic` 和 `matched_topics`。
    实现逻辑：先走 JSON 解析，再走轻量正则兜底，最后统一归一化字段。
    可调参数：原始文本、兜底主题和兜底主题分类。
    默认参数及原因：默认最终总会回到兜底值，原因是索引流程不能因为单页格式漂移而中断。
    """

    parsed = _try_parse_page_summary_dict(raw)
    if parsed is not None:
        parsed_topic = parsed.get("topic", "")
        parsed_topics = parsed.get("matched_topics", parsed.get("matchedTopics", fallback_topics))
        return (
            _normalize_topic(str(parsed_topic or ""), fallback_topic),
            _normalize_matched_topics(parsed_topics, fallback_topics),
        )

    cleaned = _strip_json_fence(raw)
    topic_match = re.search(r"(?:^|\n)\s*topic\s*[:=：]\s*(.+)", cleaned, flags=re.IGNORECASE)
    topics_match = re.search(
        r"(?:^|\n)\s*matched_topics\s*[:=：]\s*(.+)",
        cleaned,
        flags=re.IGNORECASE,
    )

    parsed_topic = topic_match.group(1).strip() if topic_match else cleaned
    parsed_topic = parsed_topic.splitlines()[0].strip() if parsed_topic else ""

    parsed_topics: list[str] | str = fallback_topics
    if topics_match:
        parsed_topics = [
            item.strip(" \"'[]")
            for item in re.split(r"[，,、]", topics_match.group(1))
            if item.strip(" \"'[]")
        ]

    return (
        _normalize_topic(parsed_topic, fallback_topic),
        _normalize_matched_topics(parsed_topics, fallback_topics),
    )


def _extract_page_summary_from_result(
    result: object,
    fallback_topic: str,
    fallback_topics: list[str],
) -> tuple[str, list[str]]:
    """
    设计目的：统一兼容不同返回形态的 `Agent.kickoff()` 结果。
    模块功能：优先读取 `.raw`，没有时退回对象字符串表示，再走统一文本解析。
    实现逻辑：提取原始文本后转调 `_extract_page_summary_from_raw()`。
    可调参数：返回对象、兜底主题和兜底主题分类。
    默认参数及原因：默认按原始文本解析，原因是这里已经不再依赖 CrewAI 的结构化输出链。
    """

    raw_output = getattr(result, "raw", result)
    return _extract_page_summary_from_raw(str(raw_output or ""), fallback_topic, fallback_topics)


def _heuristic_topic(page_text: str, fallback: str) -> str:
    """
    设计目的：在模型调用失败时仍然给页面一个可用概括。
    模块功能：从页面前几行里抓取最像标题的一行作为回退概括。
    实现逻辑：逐行清洗文本，找到第一条较稳定的非空内容，再走统一概括归一化。
    可调参数：页面全文和兜底概括。
    默认参数及原因：优先取页面靠前文本，原因是标题通常出现在页面开头。
    """

    for line in page_text.splitlines():
        cleaned = " ".join(line.split()).strip(" ，,.;:()[]{}")
        if len(cleaned) >= 2:
            return _normalize_topic(cleaned, fallback)
    return fallback


def _heuristic_matched_topics(page_text: str) -> list[str]:
    """
    设计目的：在模型失败时仍然尽量给页面一个合理归类。
    模块功能：根据页面文本中的关键词，从固定主题字典里猜测最接近的 1 到 2 个主题。
    实现逻辑：按预设主题关键词表顺序扫描页面文本，命中后收集主题并限量返回。
    可调参数：页面全文。
    默认参数及原因：命中不到时回退 `其他`，原因是主题字段不能为空。
    """

    text = (page_text or "").lower()
    matched: list[str] = []
    for topic_name, keywords in TOPIC_HEURISTIC_KEYWORDS.items():
        if any(keyword.lower() in text for keyword in keywords):
            matched.append(topic_name)
        if len(matched) >= PAGE_INDEX_MATCHED_TOPICS_MAX_COUNT:
            break

    if matched:
        return matched
    return ["其他"]


def build_page_topic_task_prompt(
    page_number: int,
    page_text: str,
    company_name: str = "",
    total_pages: int = 0,
) -> str:
    """
    设计目的：把单页内容包装成稳定 prompt，降低模型输出漂移。
    模块功能：写入固定主题字典、归类规则、公司名、页码、总页数和页面正文。
    实现逻辑：先写任务说明和规则，再提供固定主题集合，最后拼接页面内容。
    可调参数：页码、总页数、页面文本和公司名称。
    默认参数及原因：`total_pages` 默认为 0，原因是有些调用场景不强依赖总页数信息。
    """

    prompt_parts = [
        "你是负责给 PDF 页面生成索引标签的助手。",
        "请根据页面内容，提炼一个短概括，并从固定主题字典里挑出最接近的主题。",
        "",
        "固定主题字典：",
        "、".join(PAGE_INDEX_ALLOWED_TOPICS),
        "",
        "规则：",
    ]
    prompt_parts.extend(f"- {rule}" for rule in PAGE_INDEX_PROMPT_RULES)
    prompt_parts.extend(
        [
            "",
            f"公司名称：{company_name or PAGE_INDEX_UNKNOWN_COMPANY}",
            f"页码：{page_number}",
            f"总页数：{total_pages or '未知'}",
            f"页面内容：\n{page_text}",
        ]
    )
    return "\n".join(prompt_parts).strip()


def create_page_indexer_agent() -> Agent:
    """
    设计目的：集中管理页面索引 agent 的构造逻辑。
    模块功能：按模块常量创建一个稳定的逐页主题摘要 agent。
    实现逻辑：把角色、目标、LLM 参数和容错参数统一封装在这里。
    可调参数：由模块常量控制 temperature、timeout、retry 和 reasoning。
    默认参数及原因：默认低温度且不启用 reasoning，原因是这里追求稳定归纳而不是开放式推理。
    """

    return Agent(
        role=PAGE_INDEX_AGENT_ROLE,
        goal=PAGE_INDEX_AGENT_GOAL,
        backstory=PAGE_INDEX_AGENT_BACKSTORY,
        llm=get_heavy_llm(
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


def summarize_page_topic(
    page_number: int,
    page_text: str,
    company_name: str = "",
    total_pages: int = 0,
) -> PdfPageIndexEntry:
    """
    设计目的：统一处理单页主题生成、重试、兜底和最终索引输出。
    模块功能：调用 agent 生成短概括和字典主题，并在异常时有限重试，在最终失败时回退启发式结果。
    实现逻辑：空白页直接返回；非空页按“构造 prompt -> 调 LLM -> 解析原始文本 -> 失败时退避重试 -> 最终兜底”执行。
    可调参数：页码、页面文本、公司名和总页数；重试上限来自环境变量或模块默认值。
    默认参数及原因：模型失败时回退启发式结果，原因是索引流程应尽量完成而不是被单页阻断。
    """

    fallback_topic = PAGE_INDEX_EMPTY_PAGE_TOPIC if not page_text.strip() else f"第{page_number}页内容"
    fallback_topics = (
        PAGE_INDEX_EMPTY_PAGE_MATCHED_TOPICS[:] if not page_text.strip() else _heuristic_matched_topics(page_text)
    )
    if not page_text.strip():
        return PdfPageIndexEntry(
            page_number=page_number,
            topic=fallback_topic,
            matched_topics=fallback_topics,
        )

    prompt = build_page_topic_task_prompt(
        page_number=page_number,
        page_text=page_text,
        company_name=company_name,
        total_pages=total_pages,
    )
    retry_limit = get_page_index_retry_limit()

    for attempt in range(retry_limit + 1):
        started_at = perf_counter()
        try:
            agent = create_page_indexer_agent()
            result = agent.kickoff(prompt)
            parsed_topic, parsed_topics = _extract_page_summary_from_result(
                result=result,
                fallback_topic=fallback_topic,
                fallback_topics=fallback_topics,
            )

            elapsed_seconds = perf_counter() - started_at
            log_pdf_index_message(
                f"[PDF Index] page {page_number}/{total_pages or '?'} completed in {elapsed_seconds:.1f}s"
            )
            return PdfPageIndexEntry(
                page_number=page_number,
                topic=parsed_topic,
                matched_topics=parsed_topics,
            )
        except Exception as exc:
            elapsed_seconds = perf_counter() - started_at
            log_pdf_index_message(
                "[PDF Index] page "
                f"{page_number}/{total_pages or '?'} attempt {attempt + 1}/{retry_limit + 1} failed in "
                f"{elapsed_seconds:.1f}s with {type(exc).__name__}: {exc}"
            )
            if attempt >= retry_limit or not _is_retryable_page_index_error(exc):
                break

            delay_seconds = PAGE_INDEX_RETRY_BASE_DELAY_SECONDS * (attempt + 1)
            log_pdf_index_message(
                f"[PDF Index] page {page_number}/{total_pages or '?'} will retry after {delay_seconds:.1f}s"
            )
            sleep(delay_seconds)

    return PdfPageIndexEntry(
        page_number=page_number,
        topic=_normalize_topic(_heuristic_topic(page_text, fallback_topic), fallback_topic),
        matched_topics=_normalize_matched_topics(_heuristic_matched_topics(page_text), fallback_topics),
    )


def summarize_pages_in_parallel(
    pages: list[str],
    company_name: str = "",
    max_concurrency: int | None = None,
) -> list[PdfPageIndexEntry]:
    """
    设计目的：把整份 PDF 的页面主题摘要切成受控并发任务。
    模块功能：并发处理多页主题生成，并保证最终结果顺序与原页码一致。
    实现逻辑：预先分配结果槽位，再用线程池提交每页任务，最后按页码顺序回收结果。
    可调参数：页面列表、公司名和最大并发数。
    默认参数及原因：未显式传入并发数时使用环境配置，原因是不同机器和 API 配额需要不同并发上限。
    """

    if not pages:
        return []

    resolved_max_concurrency = max_concurrency or get_page_index_max_concurrency()
    worker_count = max(1, min(resolved_max_concurrency, len(pages)))
    total_pages = len(pages)
    results: list[PdfPageIndexEntry | None] = [None] * total_pages

    log_pdf_index_message(f"[PDF Index] parallel mode enabled with max_concurrency={worker_count}")

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="pdf-index") as executor:
        future_to_page_number = {
            executor.submit(
                summarize_page_topic,
                page_number,
                page_text,
                company_name,
                total_pages,
            ): page_number
            for page_number, page_text in enumerate(pages, start=1)
        }

        for future in as_completed(future_to_page_number):
            page_number = future_to_page_number[future]
            results[page_number - 1] = future.result()

    return [entry for entry in results if entry is not None]


def ensure_pdf_page_index(
    pdf_file_path: str,
    company_name: str = "",
    force_rebuild: bool = PAGE_INDEX_FORCE_REBUILD_DEFAULT,
) -> str:
    """
    设计目的：对外提供稳定入口，优先复用页面索引，必要时再重建。
    模块功能：检查缓存、并发生成逐页主题、保存索引并设置 PDF 运行上下文。
    实现逻辑：先校验 PDF 和现有索引，再提取页面文本，最后走并发主题汇总和落盘。
    可调参数：PDF 路径、公司名和 `force_rebuild`。
    默认参数及原因：默认不强制重建，原因是已有索引通常已经足够且能减少重复模型调用。
    """

    pdf_path = Path(pdf_file_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

    index_path = default_page_index_path(pdf_path)
    if not force_rebuild and page_index_is_current(pdf_path, index_path):
        set_pdf_context(str(pdf_path), str(index_path))
        return str(index_path)

    pages = extract_pdf_pages(pdf_path)
    page_entries = summarize_pages_in_parallel(pages=pages, company_name=company_name)
    payload = build_page_index_payload(pdf_path, page_entries)
    saved_index_path = save_page_index(payload, index_path)
    set_pdf_context(str(pdf_path), saved_index_path)
    return saved_index_path


def reset_pdf_preprocessing_runtime_state() -> None:
    """
    设计目的：给 PDF 预处理阶段提供统一的运行时清理入口。
    模块功能：清空页面工具层缓存和上下文状态。
    实现逻辑：直接转调 `reset_pdf_page_tool_runtime_state()`。
    可调参数：无。
    默认参数及原因：不拆分更多粒度，原因是预处理阶段清掉整套状态最稳妥。
    """

    reset_pdf_page_tool_runtime_state()

