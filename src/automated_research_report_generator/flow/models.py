from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from automated_research_report_generator.flow.common import utc_timestamp

# 设计目的：集中定义 Flow、registry、内部校验摘要和 checkpoint 共用的数据模型。
# 模块功能：统一约束 registry entry、证据和整条 Flow 状态。
# 实现逻辑：使用 Pydantic 模型承接跨模块共享的数据，并只保留当前 deterministic registry 真正使用的字段。
# 可调参数：各类 `Literal` 枚举值、pack/topic 映射和字段默认值。
# 默认参数及原因：时间字段统一走 `utc_timestamp()`，列表字段统一走 `default_factory`，原因是多轮运行更稳定。

RegistryEntryType = Literal["fact", "data", "judgment"]
RegistryContentType = Literal["single", "table"]
RegistryEntryStatus = Literal["unchecked", "checked", "need_revision"]
RegistryEntryPriority = Literal["high", "medium", "low"]
RegistryTopic = Literal[
    "history",
    "industry",
    "business",
    "peer_info",
    "financial",
    "operating_metrics",
    "risk",
    "peers",
    "intrinsic_value",
    "valuation",
    "investment_thesis",
]
RegistryEvidenceStance = Literal["support", "conflict", "context"]
CrewOwner = Literal[
    "valuation_crew",
    "investment_thesis_crew",
    "history_background_crew",
    "industry_crew",
    "business_crew",
    "peer_info_crew",
    "financial_crew",
    "operating_metrics_crew",
    "risk_crew",
    "writeup_crew",
]
PACK_TO_REGISTRY_TOPIC: dict[str, RegistryTopic] = {
    "history_background_pack": "history",
    "industry_pack": "industry",
    "business_pack": "business",
    "peer_info_pack": "peer_info",
    "finance_pack": "financial",
    "operating_metrics_pack": "operating_metrics",
    "risk_pack": "risk",
    "peers_pack": "peers",
    "intrinsic_value_pack": "intrinsic_value",
    "valuation_pack": "valuation",
    "investment_thesis": "investment_thesis",
    "diligence_questions": "investment_thesis",
}
TOPIC_TO_OWNER_CREW: dict[RegistryTopic, CrewOwner] = {
    "history": "history_background_crew",
    "industry": "industry_crew",
    "business": "business_crew",
    "peer_info": "peer_info_crew",
    "financial": "financial_crew",
    "operating_metrics": "operating_metrics_crew",
    "risk": "risk_crew",
    "peers": "valuation_crew",
    "intrinsic_value": "valuation_crew",
    "valuation": "valuation_crew",
    "investment_thesis": "investment_thesis_crew",
}
OWNER_CREW_TO_DEFAULT_TOPIC: dict[CrewOwner, RegistryTopic] = {
    "history_background_crew": "history",
    "industry_crew": "industry",
    "business_crew": "business",
    "peer_info_crew": "peer_info",
    "financial_crew": "financial",
    "operating_metrics_crew": "operating_metrics",
    "risk_crew": "risk",
    "investment_thesis_crew": "investment_thesis",
}


class RegistryEntry(BaseModel):
    """
    目的：定义 registry 中统一的 entry 结构。
    功能：同时表达事实、数据和判断三类条目，并覆盖单值与表格两种内容形态。
    实现逻辑：以 `topic + owner_crew + content_type + status` 作为唯一主语义，不再保留旧版 question registry 的过渡字段。
    可调参数：entry 类型、topic、内容形态、状态、优先级和正文补充字段。
    默认参数及原因：默认按 `judgment/single/unchecked` 处理，原因是初始化模板里单值判断仍是主体。
    """

    model_config = ConfigDict(populate_by_name=True)

    entry_id: str
    entry_type: RegistryEntryType = "judgment"
    topic: RegistryTopic
    owner_crew: CrewOwner
    priority: RegistryEntryPriority = "high"
    title: str
    description: str = ""
    content_type: RegistryContentType = "single"
    content: str | list[dict[str, str]] = ""
    columns: list[str] = Field(default_factory=list)
    unit: str = ""
    period: str = ""
    source: str = ""
    confidence: str = ""
    status: RegistryEntryStatus = "unchecked"
    revision_detail: str = ""
    creator: str = "system"
    last_updated_at: str = Field(default_factory=utc_timestamp)

    @model_validator(mode="before")
    @classmethod
    def _normalize_payload(cls, raw_data: Any) -> Any:
        """
        目的：在模型加载前把输入压成当前 registry 的最小字段集。
        功能：补齐 topic/owner、description、content_type、content 和 creator 等基础字段。
        实现逻辑：先把输入转成字典，再按当前 deterministic registry 规则做最小规范化。
        可调参数：`raw_data`。
        默认参数及原因：缺失字段按当前模板语义补齐，原因是模板与工具都已经围绕这套字段工作。
        """

        if isinstance(raw_data, BaseModel):
            payload = raw_data.model_dump()
        elif isinstance(raw_data, dict):
            payload = dict(raw_data)
        else:
            return raw_data

        topic = str(payload.get("topic", "")).strip()
        owner_crew = str(payload.get("owner_crew", "")).strip()
        if not topic and owner_crew:
            inferred_topic = OWNER_CREW_TO_DEFAULT_TOPIC.get(owner_crew)  # type: ignore[arg-type]
            if inferred_topic:
                payload["topic"] = inferred_topic
        if topic and not owner_crew:
            payload["owner_crew"] = TOPIC_TO_OWNER_CREW.get(topic, owner_crew)

        if not str(payload.get("description", "")).strip():
            payload["description"] = str(payload.get("title", "")).strip()

        content = payload.get("content")
        columns = payload.get("columns") or []
        content_type = str(payload.get("content_type", "")).strip()
        if not content_type:
            content_type = "table" if columns or isinstance(content, list) else "single"
            payload["content_type"] = content_type

        if content is None:
            if content_type == "table":
                payload["content"] = []
            else:
                payload["content"] = ""

        if not payload.get("creator"):
            payload["creator"] = "system"

        if not str(payload.get("entry_type", "")).strip():
            entry_id = str(payload.get("entry_id", "")).upper()
            prefix = entry_id.split("_", maxsplit=1)[0]
            payload["entry_type"] = {
                "F": "fact",
                "D": "data",
                "J": "judgment",
            }.get(prefix, "data" if payload["content_type"] == "table" else "judgment")

        return payload

    @model_validator(mode="after")
    def _validate_content_shape(self) -> "RegistryEntry":
        """
        目的：确保新 registry 中 `single/table` 两种内容形态稳定可序列化。
        功能：校验表格列头、表格行结构，以及单值内容的字符串化。
        实现逻辑：表格型统一补齐缺失列；单值型统一转成字符串并清空无效列头。
        可调参数：当前无显式参数。
        默认参数及原因：表格型缺列时报错，原因是后续 Markdown 渲染和工具筛选都依赖列头一致。
        """

        if self.content_type == "table":
            if not self.columns:
                raise ValueError("table 类型 entry 必须提供 columns。")
            if not isinstance(self.content, list):
                raise ValueError("table 类型 entry 的 content 必须是 list[dict]。")

            normalized_rows: list[dict[str, str]] = []
            known_columns = set(self.columns)
            for row in self.content:
                if not isinstance(row, dict):
                    raise ValueError("table 类型 entry 的每一行都必须是 dict。")
                extra_columns = {str(key) for key in row} - known_columns
                if extra_columns:
                    raise ValueError(f"table 行中出现未定义列：{sorted(extra_columns)}")
                normalized_rows.append(
                    {column: str(row.get(column, "")) for column in self.columns}
                )
            self.content = normalized_rows
            if self.entry_type != "data":
                self.entry_type = "data"
            self.unit = ""
            self.period = ""
        else:
            if isinstance(self.content, list):
                raise ValueError("single 类型 entry 的 content 不能是列表。")
            self.content = "" if self.content is None else str(self.content)
            self.columns = []

        if not self.description:
            self.description = self.title

        if not self.source:
            self.source = ""
        if not self.confidence:
            self.confidence = ""
        if not self.revision_detail:
            self.revision_detail = ""

        return self


class EvidenceRecord(BaseModel):
    """
    目的：定义挂到 registry entry 上的单条证据记录。
    功能：保存证据标题、摘要、来源、立场和关联 entry。
    实现逻辑：统一使用 `entry_ids` 保存关联条目。
    可调参数：各字段都可以按证据来源和使用场景覆盖。
    默认参数及原因：`stance` 默认 `support`，原因是新增证据大多先用于支持判断。
    """

    evidence_id: str
    title: str
    summary: str
    source_type: str
    source_ref: str
    pack_name: str
    entry_ids: list[str] = Field(default_factory=list)
    stance: RegistryEvidenceStance = "support"
    captured_at: str = Field(default_factory=utc_timestamp)
    note: str = ""


class EvidenceRegistrySnapshot(BaseModel):
    """
    目的：表达 registry 当前完整快照。
    功能：统一保存公司信息、entry 列表、证据列表和备注。
    实现逻辑：把 entry 与证据一起放进同一快照模型，便于一次读写整份账本。
    可调参数：公司信息、entries、evidence、notes。
    默认参数及原因：列表字段默认空列表，原因是初始化时通常先搭框架再逐步补充。
    """

    model_config = ConfigDict(populate_by_name=True)

    company_name: str
    industry: str
    entries: list[RegistryEntry] = Field(default_factory=list)
    evidence: list[EvidenceRecord] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    updated_at: str = Field(default_factory=utc_timestamp)


class ResearchRegistryCheckIssue(BaseModel):
    """
    目的：定义 research 子 crew 内部 QA 检查中单条未通过问题的结构。
    功能：统一承载条目 ID、问题类型和详细说明，供 Flow 稳定消费。
    实现逻辑：使用固定字段和 `Literal` 枚举约束问题类型，减少自由文本漂移。
    可调参数：`issue_type` 的枚举范围和 `detail` 的具体描述内容。
    默认参数及原因：本模型不提供业务默认值，原因是每条问题都必须显式来自真实检查结果。
    """

    entry_id: str = Field(description="未通过检查的条目 ID")
    issue_type: Literal[
        "missing_content",
        "need_revision",
        "source_conflict",
        "incomplete_table",
    ] = Field(description="问题类型")
    detail: str = Field(description="问题描述")


class ResearchRegistryCheckResult(BaseModel):
    """
    目的：定义 research 子 crew 内部 QA 检查结果的结构化输出。
    功能：统一表达整体就绪状态、未通过条目、返工建议和简要结论。
    实现逻辑：用固定字段约束 `check_registry` 输出，便于 Flow 直接判定是否阻断下游。
    可调参数：问题列表、修订建议列表和建议回退阶段。
    默认参数及原因：列表字段默认空列表，原因是 ready 场景下本就不应凭空生成问题或建议。
    """

    pack_name: str = Field(description="当前分析包名称")
    overall_status: Literal["ready", "not_ready"] = Field(description="整体就绪状态")
    issues: list[ResearchRegistryCheckIssue] = Field(
        default_factory=list,
        description="未通过的条目列表，ready 时应为空",
    )
    revision_suggestions: list[str] = Field(
        default_factory=list,
        description="修订建议",
    )
    recommended_rework_stage: Literal["extract", "search", "none"] = Field(
        description="建议回退阶段，ready 时应为 none",
    )
    summary: str = Field(description="QA 检查摘要")


class ResearchFlowState(BaseModel):
    """
    目的：统一保存整条 Flow 在各阶段共享的运行状态。
    功能：承接输入 PDF、各 pack 路径、内部校验摘要、checkpoint 产物和最终报告路径。
    实现逻辑：把跨阶段需要传递的路径、结果和循环计数集中到一个状态模型里。
    可调参数：各路径字段、内部摘要路径和循环计数器。
    默认参数及原因：默认从空值起步，原因是不同阶段会逐步补齐状态，不应预设业务内容。
    """

    model_config = ConfigDict(populate_by_name=True)

    pdf_file_path: str = ""
    company_name: str = ""
    industry: str = ""
    document_metadata_file_path: str = ""
    page_index_file_path: str = ""
    run_debug_manifest_path: str = ""
    run_slug: str = ""
    run_cache_dir: str = ""
    run_output_dir: str = ""
    evidence_registry_path: str = ""

    history_background_pack_path: str = ""
    industry_pack_path: str = ""
    business_pack_path: str = ""
    peer_info_pack_path: str = ""
    finance_pack_path: str = ""
    operating_metrics_pack_path: str = ""
    risk_pack_path: str = ""

    peers_pack_path: str = ""
    intrinsic_value_pack_path: str = ""
    valuation_pack_path: str = ""

    investment_thesis_path: str = ""
    diligence_questions_path: str = ""
    research_internal_review_summary_path: str = ""
    registry_snapshot_markdown_path: str = ""

    final_report_markdown_path: str = ""
    final_report_pdf_path: str = ""
    failed_stage: str = ""
    failed_crew: str = ""
    error_message: str = ""
    blocked_packs: list[str] = Field(default_factory=list)
    block_reason: str = ""

    valuation_loop_count: int = 0
    thesis_loop_count: int = 0
