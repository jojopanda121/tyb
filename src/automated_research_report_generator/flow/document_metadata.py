from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

_logger = logging.getLogger(__name__)

from crewai import Agent
from pydantic import BaseModel, Field

from automated_research_report_generator.llm_config import get_heavy_llm
from automated_research_report_generator.tools.document_metadata_tools import (
    default_document_metadata_path,
    document_metadata_is_current,
    load_document_metadata,
    sample_document_metadata_pages,
    save_document_metadata,
)
from automated_research_report_generator.tools.pdf_page_tools import compute_pdf_fingerprint

# 设计目的：在 research flow 正式启动前，先从 PDF 里识别最基础的公司名称和行业名称，减少人工输入。
# 模块功能：抽样 PDF 页面、调用轻量 agent 识别基础信息，并把结果落盘复用。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：抽样页数、agent 参数，以及 `force_rebuild`。
# 默认参数及原因：默认优先复用已有 metadata，原因是基础信息通常变化很少，没必要重复调模型。

DOCUMENT_METADATA_AGENT_ROLE = "PDF 文档基础信息识别专员"
DOCUMENT_METADATA_AGENT_GOAL = (
    "读取 PDF 的关键页面，识别该文档对应的公司名称、所属行业和报告期间，供主流程自动补充输入参数。"
)
DOCUMENT_METADATA_AGENT_BACKSTORY = (
    "你专门负责在 PDF 的封面、目录、业务介绍、公司概览和财务报表等关键页面中识别公司名称、行业和报告期间。"
    "你不做长篇分析，只输出最核心的结构化基础信息。"
)
DOCUMENT_METADATA_AGENT_TEMPERATURE = 0.1
DOCUMENT_METADATA_AGENT_VERBOSE = True
DOCUMENT_METADATA_AGENT_ALLOW_DELEGATION = False
DOCUMENT_METADATA_AGENT_REASONING = False
DOCUMENT_METADATA_AGENT_CACHE = True

DOCUMENT_METADATA_UNKNOWN_COMPANY = "未知公司"
DOCUMENT_METADATA_UNKNOWN_INDUSTRY = "未知行业"
DOCUMENT_METADATA_SAMPLE_MAX_PAGES = 15
DOCUMENT_METADATA_SAMPLE_MAX_CHARS_PER_PAGE = 2500
DOCUMENT_METADATA_FORCE_REBUILD_DEFAULT = False
DOCUMENT_METADATA_PROMPT_RULES = (
    "只能根据给定页面内容判断，不得猜测。",
    "输出必须严格符合给定结构。",
    "company_name 使用公司标准名称。",
    "industry 使用尽量简洁的行业名称。",
    "报告期间识别规则：从财务数据、审计报告、封面日期等线索中提取。"
    "period_1/2/3 是报告期覆盖的三个完整会计年度（如 '2021'、'2022'、'2023'）。"
    "period_4 是最近一季的上一年可比期间（如 '2024H1'），period_5 是最近一季（如 '2025H1'）。"
    "如果文档只覆盖年度报告没有季度数据，period_4 和 period_5 留空。"
    "如果无法判断某个期间，该字段留空字符串。",
    f"如果材料里无法明确判断公司或行业，就返回“{DOCUMENT_METADATA_UNKNOWN_COMPANY}”或“{DOCUMENT_METADATA_UNKNOWN_INDUSTRY}”。",
    "不要输出解释、前后缀、Markdown 代码块或额外文本。",
)


class PdfDocumentMetadata(BaseModel):
    """
    设计目的：定义 metadata 识别任务的结构化返回格式。
    模块功能：保存公司名、行业名和报告期间。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：字段值由模型输出填写。
    默认参数及原因：无默认业务值，原因是识别结果必须来自当前 PDF。
    """

    company_name: str = Field(..., description="PDF 对应公司的标准名称")
    industry: str = Field(..., description="PDF 对应公司的所属行业")
    period_1: str = Field("", description="报告期第1年，如 '2021'")
    period_2: str = Field("", description="报告期第2年，如 '2022'")
    period_3: str = Field("", description="报告期第3年，如 '2023'")
    period_4: str = Field("", description="报告期最近一季的上一年可比期间，如 '2024H1'")
    period_5: str = Field("", description="报告期最近一季，如 '2025H1'")


class PdfDocumentMetadataPayload(BaseModel):
    """
    设计目的：定义 metadata 落盘时使用的完整载荷格式。
    模块功能：保存 PDF 路径、时间戳、指纹、识别结果和来源页码。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：各字段由识别流程写入。
    默认参数及原因：无默认业务值，原因是每次识别都应明确绑定具体 PDF 和来源页。
    """

    pdf_file_path: str
    generated_at: str
    fingerprint: str
    company_name: str
    industry: str
    source_pages: list[int]
    periods: dict[str, str] = Field(
        default_factory=lambda: {
            "{期间1}": "",
            "{期间2}": "",
            "{期间3}": "",
            "{期间4}": "",
            "{期间5}": "",
            "{预测期1}": "",
            "{预测期2}": "",
            "{预测期3}": "",
        },
        description="报告期间占位符到实际期间的映射，如 {'{期间1}': '2021', '{期间3}': '2023'}",
    )


def _normalize_metadata_value(value: str, fallback: str) -> str:
    """
    设计目的：统一清洗 metadata 文本，避免写入多余符号或空值。
    模块功能：压缩空白、去掉首尾符号，并在空结果时回退到默认值。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`value` 和 `fallback`。
    默认参数及原因：空结果回退到 `fallback`，原因是主流程需要稳定得到可用字段。
    """

    normalized = " ".join((value or "").split()).strip(" ,.;:()[]{}")
    return normalized or fallback


def _extract_metadata_from_raw(raw: str) -> tuple[str, str, dict[str, str]]:
    """
    设计目的：兼容模型没有正确返回 `pydantic` 结果时的兜底解析。
    模块功能：从原始文本里尝试解析 JSON，并提取公司名、行业名和报告期间。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`raw`。
    默认参数及原因：解析失败返回空串和空字典，原因是后续还有统一回退逻辑接手。
    """

    cleaned = (raw or "").strip()
    if not cleaned:
        return "", "", {}

    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()

    try:
        data = json.loads(cleaned)
    except Exception:
        return "", "", {}

    if not isinstance(data, dict):
        return "", "", {}

    periods = {}
    for i in range(1, 6):
        val = str(data.get(f"period_{i}", "") or "").strip()
        if val:
            periods[f"{{期间{i}}}"] = val
    return str(data.get("company_name", "") or ""), str(data.get("industry", "") or ""), periods


def build_document_metadata_task_prompt(sampled_pages: list[tuple[int, str]]) -> str:
    """
    设计目的：把抽样页面整理成稳定 prompt，减少模型输出漂移。
    模块功能：写入任务规则、页面编号和页面文本。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：抽样页面列表。
    默认参数及原因：按页顺序直接拼接，因为这里更看重可追溯性而不是复杂提示技巧。
    """

    prompt_parts = [
        "你是负责识别 PDF 基础信息的分析助手。请根据下面抽样页面内容识别公司名称和行业名称。",
        "",
        "规则：",
    ]
    prompt_parts.extend(f"- {rule}" for rule in DOCUMENT_METADATA_PROMPT_RULES)
    prompt_parts.extend(["", "抽样页面内容："])

    for page_number, page_text in sampled_pages:
        prompt_parts.append(f"[第 {page_number} 页]")
        prompt_parts.append(page_text)
        prompt_parts.append("")

    return "\n".join(prompt_parts).strip()


def create_document_metadata_agent() -> Agent:
    """
    设计目的：把 metadata agent 的构造逻辑集中管理。
    模块功能：按模块常量创建一个稳定的识别 agent。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：由模块常量控制 temperature、verbose、reasoning 等设置。
    默认参数及原因：默认低温度且不启用 reasoning，原因是这里只需要稳定抽取。
    """

    return Agent(
        role=DOCUMENT_METADATA_AGENT_ROLE,
        goal=DOCUMENT_METADATA_AGENT_GOAL,
        backstory=DOCUMENT_METADATA_AGENT_BACKSTORY,
        llm=get_heavy_llm(temperature=DOCUMENT_METADATA_AGENT_TEMPERATURE),
        verbose=DOCUMENT_METADATA_AGENT_VERBOSE,
        allow_delegation=DOCUMENT_METADATA_AGENT_ALLOW_DELEGATION,
        reasoning=DOCUMENT_METADATA_AGENT_REASONING,
        cache=DOCUMENT_METADATA_AGENT_CACHE,
    )


def summarize_document_metadata(
    agent: Agent,
    pdf_file_path: str | Path,
    sampled_pages: list[tuple[int, str]],
) -> PdfDocumentMetadataPayload:
    """
    设计目的：统一处理 metadata 识别、回退策略和最终结构化输出。
    模块功能：调用 agent、解析结构化结果，并在失败时走安全回退。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：agent、PDF 路径和抽样页面列表。
    默认参数及原因：识别失败时回退到文件名和“未知行业”，因为比抛错更适合主流程继续运行。
    """

    pdf_path = Path(pdf_file_path).expanduser().resolve()

    if not sampled_pages:
        return PdfDocumentMetadataPayload(
            pdf_file_path=str(pdf_path),
            generated_at=datetime.now(timezone.utc).isoformat(),
            fingerprint=compute_pdf_fingerprint(pdf_path),
            company_name=pdf_path.stem,
            industry=DOCUMENT_METADATA_UNKNOWN_INDUSTRY,
            source_pages=[],
        )

    prompt = build_document_metadata_task_prompt(sampled_pages)

    periods: dict[str, str] = {}
    try:
        result = agent.kickoff(prompt, response_format=PdfDocumentMetadata)
        company_name = ""
        industry = ""
        if getattr(result, "pydantic", None):
            pyd = result.pydantic
            company_name = str(pyd.company_name or "")
            industry = str(pyd.industry or "")
            for i in range(1, 6):
                val = str(getattr(pyd, f"period_{i}", "") or "").strip()
                if val:
                    periods[f"{{期间{i}}}"] = val
        if not company_name or not industry:
            company_name, industry, fallback_periods = _extract_metadata_from_raw(getattr(result, "raw", "") or "")
            if not periods:
                periods = fallback_periods
    except Exception as exc:
        _logger.warning(
            "Document metadata extraction failed, falling back to filename. pdf=%s error=%s",
            pdf_path,
            exc,
            exc_info=True,
        )
        company_name = pdf_path.stem
        industry = DOCUMENT_METADATA_UNKNOWN_INDUSTRY

    return PdfDocumentMetadataPayload(
        pdf_file_path=str(pdf_path),
        generated_at=datetime.now(timezone.utc).isoformat(),
        fingerprint=compute_pdf_fingerprint(pdf_path),
        company_name=_normalize_metadata_value(company_name, pdf_path.stem or DOCUMENT_METADATA_UNKNOWN_COMPANY),
        industry=_normalize_metadata_value(industry, DOCUMENT_METADATA_UNKNOWN_INDUSTRY),
        source_pages=[page_number for page_number, _ in sampled_pages],
        periods=periods,
    )


def ensure_pdf_document_metadata(
    pdf_file_path: str,
    force_rebuild: bool = DOCUMENT_METADATA_FORCE_REBUILD_DEFAULT,
) -> dict[str, str]:
    """
    设计目的：对外提供一个稳定入口，优先复用 metadata，必要时再重建。
    模块功能：检查缓存、抽样 PDF、调用识别 agent，并保存结果。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：PDF 路径和 `force_rebuild`。
    默认参数及原因：默认不强制重建，因为已有 metadata 通常已经足够，而且能减少重复模型调用。
    默认参数及原因补充：调用前必须先激活 run 级 `indexing/` 目录，原因是 metadata 不再允许落到项目级公共缓存目录。
    """

    pdf_path = Path(pdf_file_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

    metadata_path = default_document_metadata_path(pdf_path)
    if not force_rebuild and document_metadata_is_current(pdf_path, metadata_path):
        data = load_document_metadata(metadata_path)
        return {
            "company_name": str(data.get("company_name", "")).strip(),
            "industry": str(data.get("industry", "")).strip(),
            "document_metadata_file_path": str(metadata_path),
        }

    sampled_pages = sample_document_metadata_pages(
        pdf_path,
        max_pages=DOCUMENT_METADATA_SAMPLE_MAX_PAGES,
        max_chars_per_page=DOCUMENT_METADATA_SAMPLE_MAX_CHARS_PER_PAGE,
    )
    metadata_agent = create_document_metadata_agent()
    payload = summarize_document_metadata(metadata_agent, pdf_path, sampled_pages)
    saved_metadata_path = save_document_metadata(payload, metadata_path)
    return {
        "company_name": payload.company_name,
        "industry": payload.industry,
        "document_metadata_file_path": saved_metadata_path,
    }


def resolve_pdf_document_metadata_payload(
    pdf_file_path: str,
    force_rebuild: bool = DOCUMENT_METADATA_FORCE_REBUILD_DEFAULT,
) -> PdfDocumentMetadataPayload:
    """
    目的：在 metadata 正式落盘前，先得到一份稳定的结构化结果。
    功能：直接在内存中完成 metadata 识别，交给调用方决定最终 run 内落盘位置。
    实现逻辑：不再读取项目级默认缓存路径，而是抽样页面后直接调用 agent 生成 payload。
    可调参数：PDF 路径和 `force_rebuild`。
    默认参数及原因：默认不复用项目级缓存，原因是 metadata 现在必须从一开始就落在当前 run 的 `indexing/` 目录里。
    """

    pdf_path = Path(pdf_file_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

    sampled_pages = sample_document_metadata_pages(
        pdf_path,
        max_pages=DOCUMENT_METADATA_SAMPLE_MAX_PAGES,
        max_chars_per_page=DOCUMENT_METADATA_SAMPLE_MAX_CHARS_PER_PAGE,
    )
    metadata_agent = create_document_metadata_agent()
    return summarize_document_metadata(metadata_agent, pdf_path, sampled_pages)
