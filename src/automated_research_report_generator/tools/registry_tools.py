from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Literal

from crewai.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field

from automated_research_report_generator.flow.models import (
    OWNER_CREW_TO_DEFAULT_TOPIC,
    PACK_TO_REGISTRY_TOPIC,
    TOPIC_TO_OWNER_CREW,
    EvidenceRegistrySnapshot,
    RegistryEntryPriority,
    RegistryEntryStatus,
    RegistryEvidenceStance,
    RegistryContentType,
    RegistryEntry,
    RegistryEntryType,
)
from automated_research_report_generator.flow.registry import (
    add_discovered_entry,
    load_registry,
    record_registry_review,
    register_evidence,
    render_registry_markdown,
    update_entry_fields,
    update_entry_status,
)

# 设计目的：把 registry 工具从“旧 question/judgment 账本”升级成“模板化 entry 账本”的单职责工具集合。
# 模块功能：提供 add_entry、update_entry、add_evidence、status_update、read_registry 和 registry_review 等工具。
# 实现逻辑：路径上下文仍保存在线程本地；读操作默认返回 Markdown，写操作统一落到 JSON registry。
# 可调参数：各工具的 args_schema、registry 上下文路径以及读取视图类型。
# 默认参数及原因：registry 路径继续保存在线程本地，原因是 Flow 当前的接线方式已经稳定且线程安全。

_reg_ctx = threading.local()
ENTRY_STATUS_PRIORITY = {
    "need_revision": 0,
    "unchecked": 1,
    "checked": 2,
}
ENTRY_PRIORITY_ORDER = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
ReadRegistryView = Literal[
    "markdown",
    "entry_list",
    "full",
    "entry_detail",
    "evidence_detail",
]
ENTRY_TYPE_VALUES = ("fact", "data", "judgment")
CONTENT_TYPE_VALUES = ("single", "table")
TOPIC_ALIAS_TO_CANONICAL = {
    **{pack_name.lower(): topic for pack_name, topic in PACK_TO_REGISTRY_TOPIC.items()},
    "history": "history",
    "history_background": "history",
    "历史": "history",
    "历史背景": "history",
    "历史与背景": "history",
    "历史背景分析包": "history",
    "历史与背景分析包": "history",
    "历史背景研究包": "history",
    "历史与背景研究包": "history",
    "industry": "industry",
    "行业": "industry",
    "行业分析包": "industry",
    "行业研究包": "industry",
    "business": "business",
    "业务": "business",
    "业务分析包": "business",
    "业务研究包": "business",
    "peer_info": "peer_info",
    "同行信息": "peer_info",
    "同行信息分析包": "peer_info",
    "同行信息研究包": "peer_info",
    "financial": "financial",
    "finance": "financial",
    "财务": "financial",
    "财务分析包": "financial",
    "财务研究包": "financial",
    "operating_metrics": "operating_metrics",
    "运营指标": "operating_metrics",
    "运营指标分析包": "operating_metrics",
    "运营指标研究包": "operating_metrics",
    "risk": "risk",
    "风险": "risk",
    "风险分析包": "risk",
    "风险研究包": "risk",
}


def _format_allowed_values(values: tuple[str, ...]) -> str:
    """
    目的：统一生成工具入参报错时使用的允许值列表文本。
    功能：把固定枚举值拼成稳定、可读的英文 token 列表。
    实现逻辑：直接按既定顺序用逗号拼接，避免不同调用点各自组织报错文案。
    可调参数：`values`。
    默认参数及原因：不做排序重排，原因是希望报错顺序与项目内实际枚举顺序一致。
    """

    return ", ".join(values)


def _normalize_registry_path(registry_path: str) -> str:
    """
    目的：统一 registry 路径的标准化方式。
    功能：把外部传入的 registry 路径展开并转换成绝对 POSIX 路径。
    实现逻辑：先去掉首尾空白，再用 `Path(...).expanduser().resolve()` 归一化。
    可调参数：`registry_path`。
    默认参数及原因：空字符串直接返回空字符串，原因是有些调用场景会先判断是否已配置上下文。
    """

    normalized_path = registry_path.strip()
    if not normalized_path:
        return ""
    return Path(normalized_path).expanduser().resolve().as_posix()


def set_evidence_registry_context(registry_path: str) -> None:
    """
    目的：给 Flow 和 crew 提供统一的 registry 上下文入口。
    功能：把当前运行应使用的 evidence registry 路径写到线程本地存储里。
    实现逻辑：接收外部路径后直接覆盖当前线程上下文，后续工具统一从这里取默认路径。
    可调参数：`registry_path`。
    默认参数及原因：没有内置默认路径，原因是每一轮运行的账本位置都应由外层流程显式指定。
    """

    _reg_ctx.path = _normalize_registry_path(registry_path)


class AddEntryInput(BaseModel):
    """
    目的：为新增 entry 工具提供明确、稳定的结构化输入。
    功能：支持 fact、data、judgment 三类 entry，以及 single/table 两种内容形态。
    实现逻辑：统一使用 entry 字段作为主接口；缺失 topic 或 owner 时仅在能唯一推断时自动补齐。
    可调参数：entry 类型、内容形态、归属 crew、优先级和正文补充字段。
    默认参数及原因：默认写 `judgment/single/unchecked/medium`，原因是运行中新增条目最常见的是待验证判断。
    """

    entry_id: str = Field(..., description="唯一 entry ID。")
    entry_type: str = Field(default="judgment", description="entry 类型；允许值仅有 fact / data / judgment。")
    topic: str = Field(default="", description="entry 所属主题；留空时仅在 owner_crew 可唯一映射时自动推断。")
    owner_crew: str = Field(default="", description="负责该 entry 的 crew；留空时仅在 topic 可唯一映射时自动推断。")
    priority: RegistryEntryPriority = Field(default="medium", description="entry 优先级。")
    title: str = Field(..., description="entry 标题。")
    description: str = Field(default="", description="对该条目期望输出的指引。")
    content_type: str = Field(default="single", description="内容形态；允许值仅有 single / table。")
    content: str | list[dict[str, str]] = Field(default="", description="single 为字符串，table 为行字典列表。")
    columns: list[str] = Field(default_factory=list, description="table 类型的列头定义。")
    unit: str = Field(default="", description="single 类型可选单位。")
    period: str = Field(default="", description="single 类型可选期间。")
    source: str = Field(default="", description="来源引用。")
    confidence: str = Field(default="", description="当前结论的置信度说明。")
    status: RegistryEntryStatus = Field(default="unchecked", description="entry 状态。")
    revision_detail: str = Field(default="", description="如果需要返工，这里写修订说明。")
    creator: str = Field(default="agent", description="创建者标识。")


class UpdateEntryInput(BaseModel):
    """
    目的：为更新既有 entry 的正文与元信息提供受控输入结构。
    功能：支持只改必要字段，而不是整条 entry 全量重写。
    实现逻辑：所有字段默认 `None` 或空串；工具只回写显式传入的字段。
    可调参数：entry 类型、内容形态、正文、来源、状态和修订说明等。
    默认参数及原因：默认只要求 `entry_id`，原因是模板化工作流里大多数更新都是局部补值。
    """

    entry_id: str = Field(..., description="要更新的 entry_id。")
    entry_type: str | None = Field(default=None, description="可选；如需修正 entry 类型时填写，仅允许 fact / data / judgment。")
    topic: str = Field(default="", description="可选；如需修正主题时填写。")
    owner_crew: str = Field(default="", description="可选；如需修正责任 crew 时填写。")
    priority: RegistryEntryPriority | None = Field(default=None, description="可选；更新优先级。")
    title: str = Field(default="", description="可选；如需修正标题时填写。")
    description: str = Field(default="", description="可选；如需修正说明时填写。")
    content_type: str | None = Field(default=None, description="可选；如需切换 single/table 时填写，仅允许 single / table。")
    content: str | list[dict[str, str]] | None = Field(default=None, description="可选；更新正文或表格内容。")
    columns: list[str] | None = Field(default=None, description="可选；更新 table 列头。")
    unit: str = Field(default="", description="可选；更新 single 类型单位。")
    period: str = Field(default="", description="可选；更新 single 类型期间。")
    source: str = Field(default="", description="可选；更新来源。")
    confidence: str = Field(default="", description="可选；更新置信度说明。")
    status: RegistryEntryStatus | None = Field(default=None, description="可选；更新状态。")
    revision_detail: str = Field(default="", description="可选；更新修订说明。")
    creator: str = Field(default="", description="可选；修正创建者。")


class AddEvidenceInput(BaseModel):
    """
    目的：把“追加新证据”做成 append-only 工具，避免和状态更新或 entry 新增混在一起。
    功能：接收证据标题、摘要、来源、关联 entry 和立场，并写入 registry。
    实现逻辑：按当前定义的输入、处理和返回顺序执行。
    可调参数：source_type、source_ref、pack_name、entry_ids、stance 和 note。
    默认参数及原因：source_type 默认 `agent_output`、stance 默认 `support`，原因是最常见场景是 agent 产出支持性证据。
    """

    title: str = Field(..., description="证据标题，要求短而直接。")
    summary: str = Field(..., description="证据摘要，要求只写关键事实。")
    pack_name: str = Field(..., description="该证据归属的 pack 或阶段名称。")
    entry_ids: list[str] = Field(..., description="该证据支撑或冲突了哪些 entry。")
    source_type: str = Field(default="agent_output", description="证据来源类型。")
    source_ref: str = Field(default="", description="证据来源引用。")
    stance: RegistryEvidenceStance = Field(default="support", description="证据立场。")
    note: str = Field(default="", description="补充备注。")


class StatusUpdateInput(BaseModel):
    """
    目的：把“更新 entry 状态”从通用修改动作里拆出来，只允许改受控字段。
    功能：批量更新 entry 状态，并按需补充 revision_detail。
    实现逻辑：按当前定义的输入、处理和返回顺序执行。
    可调参数：entry_ids、status 和 revision_detail。
    默认参数及原因：说明字段默认空字符串，原因是并不是每次状态变化都需要补充说明。
    """

    entry_ids: list[str] = Field(..., description="需要更新的 entry_id 列表。")
    status: RegistryEntryStatus = Field(..., description="目标状态。")
    revision_detail: str = Field(default="", description="修订说明。")


class RegistryReviewInput(BaseModel):
    """
    目的：给每个 agent 一个“即使没有改动也要留痕”的 registry 审阅输入结构。
    功能：记录本轮审阅人、对应 pack、是否有改动、涉及 entry 和后续动作。
    实现逻辑：把最小审阅结果约束成结构化字段，便于后续检索和审计。
    可调参数：reviewer、pack_name、summary、has_changes、new_entry_ids、touched_entry_ids 和 next_action。
    默认参数及原因：entry 列表默认空列表，原因是有些审阅确实只是确认当前无需改动。
    """

    reviewer: str = Field(..., description="执行本轮 registry 审阅的角色名。")
    pack_name: str = Field(..., description="本轮审阅对应的 pack 或阶段。")
    summary: str = Field(..., description="本轮审阅结论，要求简短直接。")
    has_changes: bool = Field(default=False, description="本轮是否对 entry 或 evidence 做了实际改动。")
    new_entry_ids: list[str] = Field(default_factory=list, description="本轮新增的 entry_id 列表。")
    touched_entry_ids: list[str] = Field(default_factory=list, description="本轮确认或更新过的 entry_id 列表。")
    next_action: str = Field(default="", description="如果仍需继续跟进，这里写下一步动作。")


class ReadRegistryInput(BaseModel):
    """
    目的：把读取 registry 的常见查询模式收口到一个只读工具里。
    功能：支持 Markdown、轻量 entry_list、完整 JSON、entry 详情和证据详情。
    实现逻辑：默认输出 Markdown，需要更轻或更细时再切换视图。
    可调参数：视图、状态过滤、owner/topic 过滤、标题关键词和详情 ID 列表。
    默认参数及原因：view 默认 `markdown`，原因是新版 agent 更适合先读结构化 Markdown 视图。
    """

    view: ReadRegistryView = Field(default="markdown", description="读取视图。")
    include_statuses: list[RegistryEntryStatus] = Field(default_factory=list, description="只返回这些状态的 entry。")
    exclude_statuses: list[RegistryEntryStatus] = Field(default_factory=list, description="排除这些状态的 entry。")
    owner_crew: str = Field(default="", description="按 owner_crew 精确筛选。")
    topic: str = Field(default="", description="按 topic 精确筛选。")
    title_contains: str = Field(default="", description="按标题关键字筛选。")
    filter_entry_type: RegistryEntryType | None = Field(default=None, description="按 entry 类型过滤。")
    has_supporting_evidence: bool | None = Field(default=None, description="是否有 supporting evidence。")
    has_conflicting_evidence: bool | None = Field(default=None, description="是否有 conflicting evidence。")
    has_context_evidence: bool | None = Field(default=None, description="是否有 context evidence。")
    entry_ids: list[str] = Field(default_factory=list, description="按 ID 读取 entry 详情时使用。")
    evidence_ids: list[str] = Field(default_factory=list, description="按 ID 读取证据详情时使用。")


class _RegistryToolBase(BaseTool):
    """
    目的：把 registry 路径校验和公共序列化逻辑集中起来，避免多个工具重复实现。
    功能：提供统一的上下文检查、快照读取、entry 排序和详情序列化帮助函数。
    实现逻辑：优先使用显式 registry_path，其次回退到线程上下文路径。
    可调参数：继承类可复用这些公共方法，不需要单独传运行时参数。
    默认参数及原因：没有额外实例默认参数，原因是权限边界和输入结构应由子类显式声明。
    """

    registry_path: str = Field(default="", exclude=True)

    def model_post_init(self, __context: Any) -> None:
        """
        目的：在工具实例创建时尽早固化本次运行的 registry 路径。
        功能：优先保留显式传入的 `registry_path`，否则尝试从当前线程上下文捕获路径。
        实现逻辑：先调用父类初始化收尾，再把实例路径或线程上下文路径标准化后写回实例字段。
        可调参数：`__context` 由 Pydantic 生命周期传入。
        默认参数及原因：实例字段默认留空，原因是部分测试会先创建工具、后注入上下文。
        """

        super().model_post_init(__context)
        current_path = self.registry_path or getattr(_reg_ctx, "path", "")
        if current_path:
            self.registry_path = _normalize_registry_path(current_path)

    def _remember_registry_path(self, registry_path: str) -> str:
        """
        目的：统一维护工具实例已经绑定的 registry 路径。
        功能：把传入路径标准化后保存到实例字段，并返回可直接读写的路径。
        实现逻辑：复用统一路径标准化函数，避免不同入口各自手写路径处理。
        可调参数：`registry_path`。
        默认参数及原因：每次写入都做标准化，原因是线程上下文和显式入参都可能带相对路径。
        """

        self.registry_path = _normalize_registry_path(registry_path)
        return self.registry_path

    def _require_registry_path(self) -> str:
        """
        目的：避免工具在没有上下文路径时误读或误写 registry。
        功能：返回当前线程的 registry 路径；如果未设置则立即报错。
        实现逻辑：先检查线程本地上下文，再决定返回路径还是抛出异常。
        可调参数：无。
        默认参数及原因：未设置路径时直接抛错，原因是账本路径缺失不能静默降级。
        """

        path = getattr(_reg_ctx, "path", "")
        if path:
            return self._remember_registry_path(path)
        if self.registry_path:
            return self.registry_path
        raise ValueError("Evidence registry context is not set. Call set_evidence_registry_context() first.")

    def _resolve_registry_path(self, registry_path: str) -> str:
        """
        目的：统一处理“显式路径优先，缺省时回退上下文”的规则。
        功能：返回可直接读写的标准化 registry 路径。
        实现逻辑：先清洗传入路径；有值就直接标准化，没值就回退当前上下文路径。
        可调参数：`registry_path`。
        默认参数及原因：默认回退到当前上下文路径，原因是大多数工具调用都不需要重复传路径。
        """

        normalized_path = registry_path.strip()
        if normalized_path:
            return self._remember_registry_path(normalized_path)
        return self._require_registry_path()

    def _load_snapshot(self) -> EvidenceRegistrySnapshot:
        """
        目的：把读取当前 registry 快照的动作集中复用。
        功能：按当前上下文路径加载完整账本快照。
        实现逻辑：先取当前路径，再直接调用 `load_registry()` 返回结果。
        可调参数：无。
        默认参数及原因：默认总是读取最新文件内容，原因是工具调用之间可能已经更新了账本。
        """

        return load_registry(self._require_registry_path())

    def _resolve_owner_and_topic(self, *, owner_crew: str, topic: str) -> tuple[str, str]:
        """
        目的：把 owner_crew 和 topic 解析成当前 registry 能接受的最小组合。
        功能：在只传入 topic 或只传入 owner_crew 时做有限自动推断；无法唯一推断时直接报错。
        实现逻辑：优先用显式值，其次用 topic->owner 或 owner->default topic 映射补齐。
        可调参数：`owner_crew` 和 `topic`。
        默认参数及原因：valuation_crew 这类多 topic owner 不做模糊推断，原因是避免错误归类。
        """

        raw_topic = topic.strip()
        normalized_topic = self._normalize_topic_alias(raw_topic)
        normalized_owner_crew = owner_crew.strip()
        if normalized_topic and normalized_owner_crew and normalized_owner_crew not in OWNER_CREW_TO_DEFAULT_TOPIC:
            normalized_owner_crew = TOPIC_TO_OWNER_CREW.get(normalized_topic, normalized_owner_crew)
        if normalized_topic and not normalized_owner_crew:
            inferred_owner = TOPIC_TO_OWNER_CREW.get(normalized_topic)
            if not inferred_owner:
                raise ValueError(
                    "Unsupported registry topic: "
                    f"{raw_topic}. Allowed topics: {_format_allowed_values(tuple(TOPIC_TO_OWNER_CREW.keys()))}"
                )
            normalized_owner_crew = inferred_owner
        if normalized_owner_crew and not normalized_topic:
            inferred_topic = OWNER_CREW_TO_DEFAULT_TOPIC.get(normalized_owner_crew)  # type: ignore[arg-type]
            if not inferred_topic:
                raise ValueError(
                    "topic is required when owner_crew cannot be mapped to a unique default topic."
                )
            normalized_topic = inferred_topic
        if raw_topic and not normalized_topic and not normalized_owner_crew:
            raise ValueError(
                "Unsupported registry topic: "
                f"{raw_topic}. Allowed topics: {_format_allowed_values(tuple(TOPIC_TO_OWNER_CREW.keys()))}"
            )
        if not normalized_owner_crew or not normalized_topic:
            raise ValueError("owner_crew and topic must be provided, or one must uniquely infer the other.")
        return normalized_owner_crew, normalized_topic

    def _normalize_topic_alias(self, topic: str) -> str:
        """
        目的：在工具层把少量高频 topic 别名收敛为 registry 规范 token。
        功能：兼容 pack_name、中文包名和当前日志里已经确认出现过的少量别名。
        实现逻辑：先尝试命中规范 token，再按小写别名映射表查找；命不中则返回空串，由上层决定回退或报错。
        可调参数：`topic`。
        默认参数及原因：只覆盖已确认高频误填，原因是本轮修复只想消除已知噪音，不扩大模糊匹配范围。
        """

        normalized_topic = topic.strip()
        if not normalized_topic:
            return ""
        if normalized_topic in TOPIC_TO_OWNER_CREW:
            return normalized_topic
        return TOPIC_ALIAS_TO_CANONICAL.get(normalized_topic.lower(), "")

    def _normalize_entry_shape(
        self,
        *,
        entry_type: str | None,
        content_type: str | None,
    ) -> tuple[str | None, str | None]:
        """
        目的：在真正写 registry 前兜住少量已知的 entry/content 类型误填。
        功能：支持把误传到 `entry_type` 里的 `single/table` 转移回 `content_type`，其余非法值继续报错。
        实现逻辑：先标准化字符串，再执行一次有限纠偏，最后分别校验两个字段是否落在允许值集合内。
        可调参数：`entry_type` 和 `content_type`。
        默认参数及原因：只处理 `single/table` 误传，原因是这是日志里已经确认的高频噪音，其余值仍应暴露为真实错误。
        """

        normalized_entry_type = entry_type.strip() if isinstance(entry_type, str) else entry_type
        normalized_content_type = content_type.strip() if isinstance(content_type, str) else content_type

        if normalized_entry_type in CONTENT_TYPE_VALUES:
            if normalized_content_type:
                raise ValueError(
                    "entry_type cannot be single/table when content_type is already provided. "
                    f"Allowed entry_type values: {_format_allowed_values(ENTRY_TYPE_VALUES)}"
                )
            normalized_content_type = normalized_entry_type
            normalized_entry_type = None

        if normalized_entry_type == "":
            normalized_entry_type = None
        if normalized_content_type == "":
            normalized_content_type = None

        if normalized_entry_type is not None and normalized_entry_type not in ENTRY_TYPE_VALUES:
            raise ValueError(
                "Unsupported entry_type: "
                f"{normalized_entry_type}. Allowed values: {_format_allowed_values(ENTRY_TYPE_VALUES)}"
            )
        if normalized_content_type is not None and normalized_content_type not in CONTENT_TYPE_VALUES:
            raise ValueError(
                "Unsupported content_type: "
                f"{normalized_content_type}. Allowed values: {_format_allowed_values(CONTENT_TYPE_VALUES)}"
            )
        return normalized_entry_type, normalized_content_type

    def _invalid_request_payload(self, *, view: str, message: str, hint: str) -> str:
        """
        目的：让只读工具在无效调用时返回结构化结果，而不是把异常直接抛给 agent。
        功能：输出带 `status=invalid_request` 的 JSON，便于 agent 在同一轮里读懂问题并自我纠偏。
        实现逻辑：把视图名、报错信息和下一步建议统一序列化成固定 JSON 结构。
        可调参数：`view`、`message` 和 `hint`。
        默认参数及原因：固定返回 JSON 字符串，原因是 read tool 的其他视图也都走结构化文本返回。
        """

        return json.dumps(
            {
                "status": "invalid_request",
                "view": view,
                "message": message,
                "hint": hint,
            },
            ensure_ascii=False,
            indent=2,
        )

    def _build_entry_evidence_index(
        self,
        snapshot: EvidenceRegistrySnapshot,
    ) -> dict[str, dict[str, list[str]]]:
        """
        目的：在工具层统一派生 entry 对 evidence 的关联视图。
        功能：按 entry_id 汇总 support/conflict/context 三类 evidence ID。
        实现逻辑：遍历 evidence 列表，再把 evidence_id 写到对应 entry 的三类桶中。
        可调参数：`snapshot`。
        默认参数及原因：entry 不再持久化 evidence 反向索引，原因是避免 registry JSON 冗余和重复维护。
        """

        evidence_index: dict[str, dict[str, list[str]]] = {}
        for evidence in snapshot.evidence:
            for entry_id in evidence.entry_ids:
                linked = evidence_index.setdefault(
                    entry_id,
                    {
                        "support": [],
                        "conflict": [],
                        "context": [],
                    },
                )
                linked[evidence.stance].append(evidence.evidence_id)
        return evidence_index

    def _serialize_entry(
        self,
        entry: RegistryEntry,
        evidence_links: dict[str, list[str]] | None = None,
    ) -> dict[str, object]:
        """
        目的：把 RegistryEntry 的输出结构固定下来，避免不同读取模式各自拼字段。
        功能：把单条 entry 对象转成可直接写 JSON 的普通字典。
        实现逻辑：按账本里已有字段顺序逐项抄出，保持读取结果稳定。
        可调参数：`entry`。
        默认参数及原因：默认输出完整核心字段，原因是 entry 详情通常需要一次看全。
        """
        linked = evidence_links or {}
        return {
            "entry_id": entry.entry_id,
            "entry_type": entry.entry_type,
            "topic": entry.topic,
            "owner_crew": entry.owner_crew,
            "priority": entry.priority,
            "title": entry.title,
            "description": entry.description,
            "content_type": entry.content_type,
            "content": entry.content,
            "columns": entry.columns,
            "unit": entry.unit,
            "period": entry.period,
            "source": entry.source,
            "confidence": entry.confidence,
            "status": entry.status,
            "revision_detail": entry.revision_detail,
            "creator": entry.creator,
            "last_updated_at": entry.last_updated_at,
            "evidence_counts": {
                "support": len(linked.get("support", [])),
                "conflict": len(linked.get("conflict", [])),
                "context": len(linked.get("context", [])),
            },
        }

    def _serialize_evidence(self, evidence) -> dict[str, object]:
        """
        目的：把 EvidenceRecord 的输出结构固定下来。
        功能：把单条 evidence 对象转成可直接写 JSON 的普通字典。
        实现逻辑：按账本里已有字段顺序逐项抄出，保持读取结果稳定。
        可调参数：`evidence`。
        默认参数及原因：默认输出完整核心字段，原因是证据详情通常需要一次看全。
        """
        return {
            "evidence_id": evidence.evidence_id,
            "title": evidence.title,
            "summary": evidence.summary,
            "source_type": evidence.source_type,
            "source_ref": evidence.source_ref,
            "pack_name": evidence.pack_name,
            "entry_ids": evidence.entry_ids,
            "stance": evidence.stance,
            "captured_at": evidence.captured_at,
            "note": evidence.note,
        }

    def _sort_entries(self, entries: list[RegistryEntry]) -> list[RegistryEntry]:
        """
        目的：让 entry 列表输出顺序稳定，减少 agent 和测试看到的随机抖动。
        功能：按状态、优先级、责任 crew、topic 和 entry_id 对 entry 排序。
        实现逻辑：复用固定优先级字典构造排序键。
        可调参数：`entries`。
        默认参数及原因：状态优先把最需要处理的 need_revision/unchecked 提前，原因是这最符合实际阅读顺序。
        """
        return sorted(
            entries,
            key=lambda entry: (
                ENTRY_STATUS_PRIORITY.get(entry.status, 99),
                ENTRY_PRIORITY_ORDER.get(entry.priority, 99),
                entry.owner_crew,
                entry.topic,
                entry.entry_id,
            ),
        )

    def _filtered_entries(
        self,
        *,
        include_statuses: list[RegistryEntryStatus],
        exclude_statuses: list[RegistryEntryStatus],
        owner_crew: str,
        topic: str,
        title_contains: str,
        filter_entry_type: RegistryEntryType | None,
        has_supporting_evidence: bool | None,
        has_conflicting_evidence: bool | None,
        has_context_evidence: bool | None,
    ) -> list[RegistryEntry]:
        """
        目的：把 entry 过滤逻辑集中复用。
        功能：按状态、owner、topic、标题和证据关联返回筛选后的 entry 列表。
        实现逻辑：先读取快照，再逐条应用过滤条件并排序。
        可调参数：各类过滤条件。
        默认参数及原因：默认返回全部 entry，原因是多数读取动作需要先看全量再决定下钻。
        """

        snapshot = self._load_snapshot()
        evidence_index = self._build_entry_evidence_index(snapshot)
        include_status_set = set(include_statuses)
        exclude_status_set = set(exclude_statuses)
        normalized_owner_crew = owner_crew.strip()
        normalized_topic = self._normalize_topic_alias(topic) or topic.strip()
        keyword = title_contains.strip().lower()
        entries = []
        for entry in snapshot.entries:
            if include_status_set and entry.status not in include_status_set:
                continue
            if exclude_status_set and entry.status in exclude_status_set:
                continue
            if normalized_owner_crew and entry.owner_crew != normalized_owner_crew:
                continue
            if normalized_topic and entry.topic != normalized_topic:
                continue
            if keyword and keyword not in entry.title.lower():
                continue
            if filter_entry_type is not None and entry.entry_type != filter_entry_type:
                continue
            entry_evidence = evidence_index.get(entry.entry_id, {})
            if has_supporting_evidence is not None and bool(entry_evidence.get("support")) != has_supporting_evidence:
                continue
            if has_conflicting_evidence is not None and bool(entry_evidence.get("conflict")) != has_conflicting_evidence:
                continue
            if has_context_evidence is not None and bool(entry_evidence.get("context")) != has_context_evidence:
                continue
            entries.append(entry)
        return self._sort_entries(entries)

    def _read_entry_list(self, entries: list[RegistryEntry]) -> str:
        """
        目的：给高频筛选场景提供轻量 entry 清单，避免一次塞入完整 Markdown。
        功能：返回 entry_id、标题、状态、优先级、owner、topic 和置信度等轻量字段。
        实现逻辑：先对 entries 排序，再压成紧凑 JSON。
        可调参数：`entries`。
        默认参数及原因：默认不带正文内容，原因是 manager 和 QA 常常只需要先定位条目再下钻。
        """

        payload_out = {
            "entry_count": len(entries),
            "entries": [
                {
                    "entry_id": entry.entry_id,
                    "title": entry.title,
                    "status": entry.status,
                    "priority": entry.priority,
                    "owner_crew": entry.owner_crew,
                    "topic": entry.topic,
                    "content_type": entry.content_type,
                    "confidence": entry.confidence,
                }
                for entry in entries
            ],
        }
        return json.dumps(payload_out, ensure_ascii=False, indent=2)

    def _read_full_snapshot(self) -> str:
        """
        目的：给需要完整复核账本的场景提供一次性读取全量 registry 的稳定入口。
        功能：返回包含公司信息、entries、evidence、备注和更新时间的完整 registry JSON。
        实现逻辑：先读取当前 registry 快照，再把整个快照对象按 JSON 形式序列化输出。
        可调参数：无。
        默认参数及原因：默认直接返回完整快照，原因是 QA 和调试场景需要先看全貌。
        """

        snapshot = self._load_snapshot()
        return json.dumps(snapshot.model_dump(by_alias=True), ensure_ascii=False, indent=2)

    def _read_entry_detail(self, entry_ids: list[str]) -> str:
        """
        目的：给只读工具提供按 ID 钻取 entry 详情的入口。
        功能：返回指定 entry ID 的完整详情 JSON。
        实现逻辑：先校验 ID 列表非空，再读取快照并只挑出命中的 entry。
        可调参数：`entry_ids`。
        默认参数及原因：默认要求显式传入 ID，原因是详情查询不能模糊读取。
        """

        if not entry_ids:
            return self._invalid_request_payload(
                view="entry_detail",
                message="entry_ids is required when view=entry_detail.",
                hint="Use view=markdown or view=entry_list first to locate specific entry_id values, then call view=entry_detail.",
            )

        snapshot = self._load_snapshot()
        evidence_index = self._build_entry_evidence_index(snapshot)
        entry_id_set = set(entry_ids)
        payload_out = {
            "company_name": snapshot.company_name,
            "industry": snapshot.industry,
            "entry_count": len(entry_id_set),
            "entries": [
                self._serialize_entry(entry, evidence_index.get(entry.entry_id))
                for entry in snapshot.entries
                if entry.entry_id in entry_id_set
            ],
        }
        return json.dumps(payload_out, ensure_ascii=False, indent=2)

    def _read_evidence_detail(self, evidence_ids: list[str]) -> str:
        """
        目的：给只读工具提供按 ID 钻取证据详情的入口。
        功能：返回指定证据 ID 的完整详情 JSON。
        实现逻辑：先校验 ID 列表非空，再读取快照并只挑出命中的证据。
        可调参数：`evidence_ids`。
        默认参数及原因：默认要求显式传入 ID，原因是证据详情也不应做模糊读取。
        """

        if not evidence_ids:
            raise ValueError("evidence_ids is required when view=evidence_detail.")

        snapshot = self._load_snapshot()
        evidence_id_set = set(evidence_ids)
        payload_out = {
            "company_name": snapshot.company_name,
            "industry": snapshot.industry,
            "evidence_count": len(evidence_id_set),
            "evidence": [
                self._serialize_evidence(evidence)
                for evidence in snapshot.evidence
                if evidence.evidence_id in evidence_id_set
            ],
        }
        return json.dumps(payload_out, ensure_ascii=False, indent=2)


class AddEntryTool(_RegistryToolBase):
    """
    目的：给 agent 一个只能“追加新 entry”的工具，不允许顺手修改已有 entry。
    功能：把结构化输入转成 `RegistryEntry` 并安全追加到 registry。
    实现逻辑：先解析 owner/topic，再调用统一 entry 追加逻辑。
    可调参数：由 AddEntryInput 控制 entry 主键、类型、归属和说明字段。
    默认参数及原因：工具名固定为 `add_entry`，原因是让 agent 一眼看出这是统一 entry 入口。
    """

    name: str = "add_entry"
    description: str = (
        "Append a newly discovered entry to the evidence registry. "
        "Use this when the current template does not already contain the needed entry. "
        "When content_type=single, write a complete answer with key facts, scope, and basis, not a short label."
    )
    args_schema: type[BaseModel] = AddEntryInput

    def _run(
        self,
        entry_id: str,
        entry_type: str = "judgment",
        topic: str = "",
        owner_crew: str = "",
        priority: RegistryEntryPriority = "medium",
        title: str = "",
        description: str = "",
        content_type: str = "single",
        content: str | list[dict[str, str]] = "",
        columns: list[str] | None = None,
        unit: str = "",
        period: str = "",
        source: str = "",
        confidence: str = "",
        status: RegistryEntryStatus = "unchecked",
        revision_detail: str = "",
        creator: str = "agent",
    ) -> str:
        """
        目的：把新增 entry 的写入动作收口在单一工具方法里。
        功能：把输入参数组装成 `RegistryEntry`，然后追加到 registry。
        实现逻辑：先按输入创建 entry 对象，再调用 `add_discovered_entry()` 落到账本。
        可调参数：entry 类型、主键、标题、正文、归属、优先级和补充说明。
        默认参数及原因：默认状态是 `unchecked`、优先级是 `medium`，原因是这最符合运行中发现新条目的常见场景。
        """

        normalized_entry_type, normalized_content_type = self._normalize_entry_shape(
            entry_type=entry_type,
            content_type=content_type,
        )
        normalized_owner_crew, normalized_topic = self._resolve_owner_and_topic(
            owner_crew=owner_crew,
            topic=topic,
        )
        entry = RegistryEntry(
            entry_id=entry_id,
            entry_type=normalized_entry_type or "judgment",
            topic=normalized_topic,  # type: ignore[arg-type]
            owner_crew=normalized_owner_crew,  # type: ignore[arg-type]
            priority=priority,
            title=title,
            description=description,
            content_type=normalized_content_type or "single",
            content=content,
            columns=columns or [],
            unit=unit,
            period=period,
            source=source,
            confidence=confidence,
            status=status,
            revision_detail=revision_detail,
            creator=creator,
        )
        add_discovered_entry(self._require_registry_path(), entry)
        return json.dumps({"status": "ok", "entry_id": entry.entry_id}, ensure_ascii=False)


class UpdateEntryTool(_RegistryToolBase):
    """
    目的：给 agent 一个受控的“更新既有 entry”入口，避免模板条目长期停留在待补状态。
    功能：更新已有 entry 的正文、表格、来源、状态和修订说明，但不允许静默删除条目。
    实现逻辑：只回写显式传入的字段，并复用 registry 层的模型重建校验。
    可调参数：由 UpdateEntryInput 控制具体要覆盖的字段。
    默认参数及原因：工具名固定为 `update_entry`，原因是模板化工作流里这会是最常用的账本修改入口。
    """

    name: str = "update_entry"
    description: str = (
        "Update an existing evidence-registry entry. "
        "Use this to fill seeded template entries with validated content, tables, sources, confidence, and status. "
        "Single-content entries should be complete answers with concrete facts, numbers, scope, and support."
    )
    args_schema: type[BaseModel] = UpdateEntryInput

    def _run(
        self,
        entry_id: str,
        entry_type: str | None = None,
        topic: str = "",
        owner_crew: str = "",
        priority: RegistryEntryPriority | None = None,
        title: str = "",
        description: str = "",
        content_type: str | None = None,
        content: str | list[dict[str, str]] | None = None,
        columns: list[str] | None = None,
        unit: str = "",
        period: str = "",
        source: str = "",
        confidence: str = "",
        status: RegistryEntryStatus | None = None,
        revision_detail: str = "",
        creator: str = "",
    ) -> str:
        """
        目的：把既有 entry 的局部更新动作收口到单一工具方法里。
        功能：按需覆盖正文、表格、来源、置信度和状态等字段。
        实现逻辑：只把显式传入的字段交给 `update_entry_fields()`；未传字段保持原值。
        可调参数：各类可选更新字段。
        默认参数及原因：默认只要求 `entry_id`，原因是模板化补值大多是局部更新。
        """

        normalized_entry_type, normalized_content_type = self._normalize_entry_shape(
            entry_type=entry_type,
            content_type=content_type,
        )
        updates: dict[str, object] = {}
        if normalized_entry_type is not None:
            updates["entry_type"] = normalized_entry_type
        if topic:
            updates["topic"] = topic
        if title:
            updates["title"] = title
        if description:
            updates["description"] = description
        if normalized_content_type is not None:
            updates["content_type"] = normalized_content_type
        if content is not None:
            updates["content"] = content
        if columns is not None:
            updates["columns"] = columns
        if owner_crew or topic:
            normalized_owner_crew, normalized_topic = self._resolve_owner_and_topic(
                owner_crew=owner_crew,
                topic=topic,
            )
            updates["owner_crew"] = normalized_owner_crew
            updates["topic"] = normalized_topic
        if priority is not None:
            updates["priority"] = priority
        if unit:
            updates["unit"] = unit
        if period:
            updates["period"] = period
        if source:
            updates["source"] = source
        if confidence:
            updates["confidence"] = confidence
        if status is not None:
            updates["status"] = status
        if revision_detail:
            updates["revision_detail"] = revision_detail
        if creator:
            updates["creator"] = creator

        update_entry_fields(self._require_registry_path(), entry_id, **updates)
        return json.dumps({"status": "ok", "entry_id": entry_id}, ensure_ascii=False)


class AddEvidenceTool(_RegistryToolBase):
    """
    目的：把新增证据做成独立 append-only 工具，限制 agent 只能追加，不能覆盖旧证据。
    功能：把结构化证据写入 registry，并自动挂接到关联 entry。
    实现逻辑：按当前定义的输入、处理和返回顺序执行。
    可调参数：由 AddEvidenceInput 控制标题、摘要、来源、关联 entry 和证据立场。
    默认参数及原因：工具名固定为 `add_evidence`，原因是减少 agent 把它当成通用编辑器的概率。
    """

    name: str = "add_evidence"
    description: str = (
        "Append a new evidence record to the evidence registry and link it to one or more existing entries. "
        "Write the summary as a concrete 1-3 sentence evidence note with the key fact, scope/date, and why it matters, "
        "and always include a precise source_ref."
    )
    args_schema: type[BaseModel] = AddEvidenceInput

    def _run(
        self,
        title: str,
        summary: str,
        pack_name: str,
        entry_ids: list[str],
        source_type: str = "agent_output",
        source_ref: str = "",
        stance: RegistryEvidenceStance = "support",
        note: str = "",
    ) -> str:
        """
        目的：把新增证据的写入动作保持为 append-only。
        功能：把一条新证据写入 registry，并自动挂到关联 entry 下面。
        实现逻辑：直接调用 `register_evidence()` 写账本，再把返回的证据 ID 作为成功结果返回。
        可调参数：证据标题、摘要、来源、所属 pack、关联 entry、立场和备注。
        默认参数及原因：默认来源类型是 `agent_output`、立场是 `support`，原因是这就是最常见的写入场景。
        """

        evidence_id = register_evidence(
            self._require_registry_path(),
            title=title,
            summary=summary,
            source_type=source_type,
            source_ref=source_ref,
            pack_name=pack_name,
            entry_ids=entry_ids,
            stance=stance,
            note=note,
        )
        return json.dumps({"status": "ok", "evidence_id": evidence_id}, ensure_ascii=False)


class StatusUpdateTool(_RegistryToolBase):
    """
    目的：把可修改范围限制在 entry 状态相关字段，避免 agent 修改核心事实。
    功能：批量更新 entry 状态，并可附带 revision_detail。
    实现逻辑：把输入直接交给 `update_entry_status()`，然后返回统一成功结果。
    可调参数：由 StatusUpdateInput 控制目标 entry 列表、状态和值班说明字段。
    默认参数及原因：工具名固定为 `status_update`，原因是明确传达该工具只负责状态推进。
    """

    name: str = "status_update"
    description: str = (
        "Update the status of existing entries in the evidence registry. "
        "Use this for QA flags and revision routing, not for changing the core content."
    )
    args_schema: type[BaseModel] = StatusUpdateInput

    def _run(
        self,
        entry_ids: list[str],
        status: RegistryEntryStatus,
        revision_detail: str = "",
    ) -> str:
        """
        目的：把 entry 状态推进限定在一组受控字段上。
        功能：批量更新 entry 状态，并可选补充修订说明。
        实现逻辑：把输入直接交给 `update_entry_status()`，然后返回统一成功结果。
        可调参数：entry ID 列表、目标状态和 `revision_detail`。
        默认参数及原因：说明字段默认空字符串，原因是并不是每次状态变化都需要补充说明。
        """

        update_entry_status(
            self._require_registry_path(),
            entry_ids,
            status=status,
            revision_detail=revision_detail,
        )
        return json.dumps({"status": "ok"}, ensure_ascii=False)


class RegistryReviewTool(_RegistryToolBase):
    """
    目的：强制每个相关 agent 在本轮任务结束时留下 registry 审阅记录。
    功能：无论是否有改动，都把审阅结果写入 registry notes，形成可追踪审计线索。
    实现逻辑：把结构化输入直接交给 `record_registry_review()`，再返回统一成功结果。
    可调参数：由 RegistryReviewInput 控制 reviewer、pack、summary、是否改动和涉及 entry。
    默认参数及原因：工具名固定为 `registry_review`，原因是让 agent 明确知道这是收尾必做动作。
    """

    name: str = "registry_review"
    description: str = (
        "Record a registry review note for the current task. "
        "Call this once before finishing the task, even when no entry or evidence changed. "
        "Be specific about what changed, what is still missing, and the next recommended action."
    )
    args_schema: type[BaseModel] = RegistryReviewInput

    def _run(
        self,
        reviewer: str,
        pack_name: str,
        summary: str,
        has_changes: bool = False,
        new_entry_ids: list[str] | None = None,
        touched_entry_ids: list[str] | None = None,
        next_action: str = "",
    ) -> str:
        """
        目的：把 registry 审阅留痕收口到单一工具调用。
        功能：向 registry notes 追加一条结构化审阅记录。
        实现逻辑：把输入标准化后直接写入 notes，再返回最小成功结果。
        可调参数：reviewer、pack_name、summary、has_changes、entry 列表和 next_action。
        默认参数及原因：entry 列表默认空列表，原因是无改动场景也必须允许留痕。
        """

        record_registry_review(
            self._require_registry_path(),
            reviewer=reviewer,
            pack_name=pack_name,
            summary=summary,
            has_changes=has_changes,
            new_entry_ids=new_entry_ids or [],
            touched_entry_ids=touched_entry_ids or [],
            next_action=next_action,
        )
        return json.dumps({"status": "ok"}, ensure_ascii=False)


class ReadRegistryTool(_RegistryToolBase):
    """
    目的：给需要看账本内容的 agent 提供统一只读入口。
    功能：支持 Markdown、entry_list、entry 详情、证据详情和完整快照几种视图。
    实现逻辑：默认走 Markdown，需要轻量或细节时再切换视图。
    可调参数：由 ReadRegistryInput 控制视图类型、过滤条件和详情 ID 列表。
    默认参数及原因：工具名固定为 `read_registry`，原因是让所有只读角色都用同一套稳定语义入口。
    """

    name: str = "read_registry"
    description: str = (
        "Read the evidence registry. "
        "Use view=markdown for the grouped Markdown view, view=entry_list for a lightweight filtered list, "
        "view=full for the complete JSON snapshot, view=entry_detail for specific entries, and "
        "view=evidence_detail for specific evidence rows."
    )
    args_schema: type[BaseModel] = ReadRegistryInput

    def _run(
        self,
        view: ReadRegistryView = "markdown",
        include_statuses: list[RegistryEntryStatus] | None = None,
        exclude_statuses: list[RegistryEntryStatus] | None = None,
        owner_crew: str = "",
        topic: str = "",
        title_contains: str = "",
        filter_entry_type: RegistryEntryType | None = None,
        has_supporting_evidence: bool | None = None,
        has_conflicting_evidence: bool | None = None,
        has_context_evidence: bool | None = None,
        entry_ids: list[str] | None = None,
        evidence_ids: list[str] | None = None,
    ) -> str:
        """
        目的：把 registry 的几种常见读取模式统一到同一个只读入口。
        功能：根据 `view` 返回 Markdown、轻量 entry_list、entry 详情、证据详情或完整快照。
        实现逻辑：先判断视图类型，再分别调用对应的读取帮助函数。
        可调参数：`view`、过滤条件、entry ID 和证据 ID。
        默认参数及原因：默认 `view=markdown`，原因是新版 agent 更容易遵循 Markdown 账本视图。
        """

        filtered_entries = self._filtered_entries(
            include_statuses=include_statuses or [],
            exclude_statuses=exclude_statuses or [],
            owner_crew=owner_crew,
            topic=topic,
            title_contains=title_contains,
            filter_entry_type=filter_entry_type,
            has_supporting_evidence=has_supporting_evidence,
            has_conflicting_evidence=has_conflicting_evidence,
            has_context_evidence=has_context_evidence,
        )
        if view == "markdown":
            return render_registry_markdown(
                self._require_registry_path(),
                filter_entry_type=filter_entry_type,
                include_statuses=include_statuses or [],
                exclude_statuses=exclude_statuses or [],
                owner_crew=owner_crew,
                topic=topic,
                title_contains=title_contains,
            )
        if view == "entry_list":
            return self._read_entry_list(filtered_entries)
        if view == "full":
            return self._read_full_snapshot()
        if view == "entry_detail":
            return self._read_entry_detail(entry_ids or [])
        if view == "evidence_detail":
            return self._read_evidence_detail(evidence_ids or [])
        raise ValueError(f"Unsupported registry read view: {view}")

    model_config = ConfigDict(populate_by_name=True)
