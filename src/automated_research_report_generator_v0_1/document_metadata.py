from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from crewai import Agent
from pydantic import BaseModel, Field

from automated_research_report_generator_v0_1.crew import get_llm
from automated_research_report_generator_v0_1.tools.document_metadata_tools import (
    default_document_metadata_path,
    document_metadata_is_current,
    load_document_metadata,
    sample_document_metadata_pages,
    save_document_metadata,
)
from automated_research_report_generator_v0_1.tools.pdf_page_tools import (
    compute_pdf_fingerprint,
)


"""元数据识别参数"""

DOCUMENT_METADATA_AGENT_ROLE = "PDF 文档基础信息识别专员"  # 默认角色名；功能：约束 agent 职责；原因：贴合当前任务边界。
DOCUMENT_METADATA_AGENT_GOAL = (  # 默认目标；功能：限定只识别公司名和行业；原因：避免扩写成长分析。
    "读取 PDF 的关键页面，识别该文档对应的公司名称和所属行业，供主流程自动填充输入参数。"
)
DOCUMENT_METADATA_AGENT_BACKSTORY = (  # 默认背景；功能：强化结构化抽取语境；原因：减少模型跑偏。
    "你专门负责在 PDF 的封面、概览、业务介绍等关键页面中识别公司名称和行业。"
    "你不做长篇分析，只输出最核心的结构化基础信息。"
)
DOCUMENT_METADATA_AGENT_TEMPERATURE = 0.1  # 可调：常用 0-0.3；默认 0.1，原因：结构化抽取更稳。
DOCUMENT_METADATA_AGENT_VERBOSE = True  # 可调：True/False；默认 True，原因：便于调试识别过程。
DOCUMENT_METADATA_AGENT_ALLOW_DELEGATION = False  # 可调：True/False；默认 False，原因：单点任务无需委派。
DOCUMENT_METADATA_AGENT_REASONING = False  # 可调：True/False；默认 False，原因：短抽取任务不必展开推理。
DOCUMENT_METADATA_AGENT_CACHE = True  # 可调：True/False；默认 True，原因：相同样本可复用结果。


"""元数据任务参数"""

DOCUMENT_METADATA_UNKNOWN_COMPANY = "未知公司"  # 默认兜底公司名；功能：识别失败时保底；原因：主流程不断。
DOCUMENT_METADATA_UNKNOWN_INDUSTRY = "未知行业"  # 默认兜底行业；功能：识别失败时保底；原因：输入字段完整。
DOCUMENT_METADATA_SAMPLE_MAX_PAGES = 15  # 可调：建议 >=1；默认 15，原因：覆盖关键信息页且控制 token。
DOCUMENT_METADATA_SAMPLE_MAX_CHARS_PER_PAGE = 2500  # 可调：建议 >=200；默认 2500，原因：兼顾线索与成本。
DOCUMENT_METADATA_FORCE_REBUILD_DEFAULT = False  # 可调：True/False；默认 False，原因：优先复用缓存。
DOCUMENT_METADATA_PROMPT_RULES = (  # 默认规则集；功能：收紧输出格式；原因：方便稳定解析。
    "只能根据给定页面内容判断，不得猜测。",
    "输出必须严格符合给定结构。",
    "company_name 使用公司标准名称。",
    "industry 使用尽量简洁的行业名称。",
    f"如果材料里无法明确判断，就返回“{DOCUMENT_METADATA_UNKNOWN_COMPANY}”或“{DOCUMENT_METADATA_UNKNOWN_INDUSTRY}”。",
    "不要输出解释、前后缀、Markdown 代码块或额外文本。",
)


"""元数据模型"""

class PdfDocumentMetadata(BaseModel):  # 设计：定义 LLM 结构化输出；功能：只保留公司名与行业；默认靠字段约束确保结果最小化。
    company_name: str = Field(..., description="PDF 对应公司的标准名称")
    industry: str = Field(..., description="PDF 对应公司的所属行业")


class PdfDocumentMetadataPayload(BaseModel):  # 设计：定义落盘载荷；功能：把识别结果与来源信息一起缓存；默认保留指纹以校验新旧文件。
    pdf_file_path: str
    generated_at: str
    fingerprint: str
    company_name: str
    industry: str
    source_pages: list[int]


"""元数据辅助函数"""

def _normalize_metadata_value(value: str, fallback: str) -> str:  # 设计：统一清洗识别值；功能：去空白和杂标点；默认空值回退 fallback，原因：输出字段必须稳定。
    normalized = " ".join((value or "").split()).strip(" ,.;:()[]{}")
    return normalized or fallback


def _extract_metadata_from_raw(raw: str) -> tuple[str, str]:  # 设计：兼容兜底解析；功能：从 raw 中补抓 JSON；默认解析失败返回空串，原因：交给后续统一回退。
    cleaned = (raw or "").strip()
    if not cleaned:
        return "", ""

    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()

    try:
        data = json.loads(cleaned)
    except Exception:
        return "", ""

    if not isinstance(data, dict):
        return "", ""

    return str(data.get("company_name", "") or ""), str(data.get("industry", "") or "")


def build_document_metadata_task_prompt(sampled_pages: list[tuple[int, str]]) -> str:  # 设计：集中组装 prompt；功能：把规则与样本页拼成任务文本；默认逐页展开，原因：便于调试与复现。
    prompt_parts = [
        "你现在负责识别一份 PDF 对应的公司名称和所属行业。",
        "",
        "规则：",
    ]
    prompt_parts.extend(f"- {rule}" for rule in DOCUMENT_METADATA_PROMPT_RULES)
    prompt_parts.extend(["", "以下是候选页面内容："])

    for page_number, page_text in sampled_pages:
        prompt_parts.append(f"[第 {page_number} 页]")
        prompt_parts.append(page_text)
        prompt_parts.append("")

    return "\n".join(prompt_parts).strip()


"""元数据 Agent"""

def create_document_metadata_agent() -> Agent:  # 设计：独立创建识别 agent；功能：供缓存失效时单独复跑；默认低温结构化配置，原因：结果更稳。
    return Agent(
        role=DOCUMENT_METADATA_AGENT_ROLE,  # 默认“PDF 文档基础信息识别专员”；可改成更细分的内部角色名。
        goal=DOCUMENT_METADATA_AGENT_GOAL,  # 默认聚焦公司名和行业识别；可改成覆盖证券简称、交易所等字段。
        backstory=DOCUMENT_METADATA_AGENT_BACKSTORY,  # 默认强调“只做结构化识别”；可改得更行业化。
        llm=get_llm(temperature=DOCUMENT_METADATA_AGENT_TEMPERATURE),  # 默认 0.1；可调到 0 更稳，调高会更灵活。
        verbose=DOCUMENT_METADATA_AGENT_VERBOSE,  # 默认 True；调试时保留详细日志。
        allow_delegation=DOCUMENT_METADATA_AGENT_ALLOW_DELEGATION,  # 默认 False；通常不需要把识别任务再分派出去。
        reasoning=DOCUMENT_METADATA_AGENT_REASONING,  # 默认 False；简单抽取任务通常不需要显式 reasoning。
        cache=DOCUMENT_METADATA_AGENT_CACHE,  # 默认 True；相同 prompt 可复用缓存。
    )


"""元数据执行"""

def summarize_document_metadata(  # 设计：执行一次识别；功能：返回可落盘 payload；默认无样本或异常时回退文件名/未知行业，原因：主流程不断。
    agent: Agent,
    pdf_file_path: str | Path,
    sampled_pages: list[tuple[int, str]],
) -> PdfDocumentMetadataPayload:
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

    try:
        result = agent.kickoff(prompt, response_format=PdfDocumentMetadata)
        company_name = ""
        industry = ""
        if getattr(result, "pydantic", None):
            company_name = str(result.pydantic.company_name or "")
            industry = str(result.pydantic.industry or "")
        if not company_name or not industry:
            company_name, industry = _extract_metadata_from_raw(getattr(result, "raw", "") or "")
    except Exception:
        company_name = pdf_path.stem
        industry = DOCUMENT_METADATA_UNKNOWN_INDUSTRY

    return PdfDocumentMetadataPayload(
        pdf_file_path=str(pdf_path),
        generated_at=datetime.now(timezone.utc).isoformat(),
        fingerprint=compute_pdf_fingerprint(pdf_path),
        company_name=_normalize_metadata_value(company_name, pdf_path.stem),
        industry=_normalize_metadata_value(industry, DOCUMENT_METADATA_UNKNOWN_INDUSTRY),
        source_pages=[page_number for page_number, _ in sampled_pages],
    )


"""元数据入口"""

def ensure_pdf_document_metadata(  # 设计：元数据总入口；功能：优先复用缓存再按需重建；可调：force_rebuild=True/False；默认 False，原因：优先省时省调用。
    pdf_file_path: str,
    force_rebuild: bool = DOCUMENT_METADATA_FORCE_REBUILD_DEFAULT,  # 默认 False；改 True 会忽略缓存直接重跑。
) -> dict[str, str]:
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
