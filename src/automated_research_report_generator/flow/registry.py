from __future__ import annotations

import json
import shutil
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import yaml

from automated_research_report_generator.flow.common import utc_timestamp
from automated_research_report_generator.flow.models import (
    EvidenceRecord,
    EvidenceRegistrySnapshot,
    PACK_TO_REGISTRY_TOPIC,
    RegistryEntry,
    RegistryEntryType,
)

_REGISTRY_LOCKS_GUARD = threading.Lock()
_REGISTRY_LOCKS: dict[str, threading.RLock] = {}

RESEARCH_PACK_NAMES = [
    "history_background_pack",
    "industry_pack",
    "business_pack",
    "peer_info_pack",
    "finance_pack",
    "operating_metrics_pack",
    "risk_pack",
]

# 设计目的：维护 Flow 共享的证据账本，作为 research、valuation、QA 和 writeup 的共同真相源。
# 模块功能：初始化 registry、加载固定模板、读写统一 entry、登记证据、渲染 Markdown 视图和生成快照。
# 实现逻辑：底层始终存 JSON，同时在每次写盘后自动刷新 Markdown 视图和可选快照文件。
# 可调参数：模板路径、pack 归一化规则和 Markdown 视图格式。
# 默认参数及原因：registry 固定保存为 UTF-8 JSON，原因是便于工具读写、调试和人工复查。


def _normalize_registry_path(registry_path: str | Path) -> str:
    """
    目的：统一 registry 路径的标准化方式。
    功能：把外部传入的路径转换成绝对 POSIX 路径。
    实现逻辑：统一走 `Path(...).expanduser().resolve()`，避免不同入口产生不同锁键。
    可调参数：`registry_path`。
    默认参数及原因：固定返回绝对路径，原因是同一个文件必须命中同一把锁。
    """

    return Path(registry_path).expanduser().resolve().as_posix()


def _get_registry_lock(registry_path: str | Path) -> threading.RLock:
    """
    目的：为每个 registry 文件提供稳定的可重入锁。
    功能：按标准化后的 registry 路径返回对应的 `RLock`。
    实现逻辑：先标准化路径，再在全局锁表里查找或创建锁实例。
    可调参数：`registry_path`。
    默认参数及原因：使用 `RLock` 而不是普通锁，原因是读改写函数内部会再次调用读写函数。
    """

    normalized_path = _normalize_registry_path(registry_path)
    with _REGISTRY_LOCKS_GUARD:
        existing_lock = _REGISTRY_LOCKS.get(normalized_path)
        if existing_lock is not None:
            return existing_lock
        created_lock = threading.RLock()
        _REGISTRY_LOCKS[normalized_path] = created_lock
        return created_lock


@contextmanager
def _registry_transaction(registry_path: str | Path):
    """
    目的：把同一路径 registry 的读写包进同一段临界区。
    功能：在进入临界区后返回标准化后的 `Path`，供读写函数复用。
    实现逻辑：先获取路径级 `RLock`，再在 `with` 作用域内保持锁，直到本次事务结束。
    可调参数：`registry_path`。
    默认参数及原因：默认返回标准化后的 `Path`，原因是后续读写函数都需要同一份已归一化路径。
    """

    normalized_path = Path(_normalize_registry_path(registry_path))
    registry_lock = _get_registry_lock(normalized_path)
    with registry_lock:
        yield normalized_path


def _default_template_path() -> Path:
    """
    目的：给固定 registry 模板提供稳定入口。
    功能：返回 `flow/config/registry_template.yaml` 的绝对路径。
    实现逻辑：基于当前模块目录拼接相对路径。
    可调参数：当前无显式参数。
    默认参数及原因：模板路径固定，原因是当前只维护一套全局 research 模板。
    """

    return Path(__file__).resolve().parent / "config" / "registry_template.yaml"


def load_registry_template(
    company_name: str,
    industry: str,
    template_path: str | Path | None = None,
    periods: dict[str, str] | None = None,
) -> list[RegistryEntry]:
    """
    目的：把固定 YAML 模板加载成可直接落盘的 registry entries。
    功能：读取模板、做占位符替换（公司名、行业、期间）、校验唯一性，并返回结构化 entry 列表。
    实现逻辑：先加载 YAML，再逐条做字符串插值和模型校验。columns 列表中的期间占位符也会被替换。
    可调参数：公司名、行业名、可选模板路径和期间映射。
    默认参数及原因：模板路径默认使用仓库内固定文件，原因是当前重构目标是确定性初始化。
    """

    resolved_template_path = Path(template_path or _default_template_path()).expanduser().resolve()
    raw_text = resolved_template_path.read_text(encoding="utf-8")
    raw_document = yaml.safe_load(raw_text) or {}

    # 兼容旧格式（顶层是列表）和新格式（顶层是字典，entries 在 document.entries 下）
    if isinstance(raw_document, list):
        raw_entries = raw_document
    elif isinstance(raw_document, dict):
        raw_entries = raw_document.get("entries", [])
    else:
        raise ValueError("registry_template.yaml 的顶层结构必须是列表或字典。")

    # 构建占位符替换映射
    format_values = {
        "company_name": company_name,
        "industry": industry,
    }
    period_map: dict[str, str] = periods or {}

    def _replace_periods(text: str) -> str:
        """替换字符串中的期间占位符。"""
        for placeholder, actual in period_map.items():
            if actual:
                text = text.replace(placeholder, actual)
        return text

    entry_ids: set[str] = set()
    entries: list[RegistryEntry] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            raise ValueError("registry_template.yaml 中的每个条目都必须是字典。")
        payload = dict(item)
        for key in ("title", "description", "content"):
            if isinstance(payload.get(key), str):
                payload[key] = _replace_periods(payload[key].format(**format_values))
        if isinstance(payload.get("columns"), list):
            payload["columns"] = [_replace_periods(col) for col in payload["columns"]]
        entry = RegistryEntry.model_validate(payload)
        if entry.entry_id in entry_ids:
            raise ValueError(f"registry 模板中存在重复 entry_id: {entry.entry_id}")
        entry_ids.add(entry.entry_id)
        entries.append(entry)
    return entries


def _entry_matches_filters(
    entry: RegistryEntry,
    *,
    filter_entry_type: RegistryEntryType | None = None,
    include_statuses: list[str] | None = None,
    exclude_statuses: list[str] | None = None,
    owner_crew: str = "",
    topic: str = "",
    title_contains: str = "",
) -> bool:
    """
    目的：把 registry 常用过滤条件集中到一个判断函数里。
    功能：按类型、状态、责任 crew、topic 和标题关键词过滤 entry。
    实现逻辑：逐个应用过滤条件；只要有一项不满足就返回 `False`。
    可调参数：各类过滤条件。
    默认参数及原因：空过滤条件不生效，原因是默认读取应尽量宽松。
    """

    include_set = {status.strip() for status in include_statuses or [] if status.strip()}
    exclude_set = {status.strip() for status in exclude_statuses or [] if status.strip()}
    normalized_owner = owner_crew.strip()
    normalized_topic = topic.strip()
    keyword = title_contains.strip().lower()

    if filter_entry_type and entry.entry_type != filter_entry_type:
        return False
    if include_set and entry.status not in include_set:
        return False
    if exclude_set and entry.status in exclude_set:
        return False
    if normalized_owner and entry.owner_crew != normalized_owner:
        return False
    if normalized_topic and entry.topic != normalized_topic:
        return False
    if keyword and keyword not in entry.title.lower():
        return False
    return True


def _build_entry_evidence_index(
    snapshot: EvidenceRegistrySnapshot,
) -> dict[str, dict[str, list[str]]]:
    """
    目的：在渲染和过滤前集中整理 entry 到 evidence 的派生关联。
    功能：按 entry_id 汇总 support/conflict/context 三类 evidence ID 列表。
    实现逻辑：遍历 evidence 列表，再把每条 evidence 挂到其关联的 entry_id 名下。
    可调参数：`snapshot`。
    默认参数及原因：缺失关联时返回空字典，原因是 registry entry 不再把 evidence 反向索引持久化到 JSON。
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


def _render_table_markdown(columns: list[str], rows: list[dict[str, str]]) -> list[str]:
    """
    目的：把 table 类型 entry 渲染成稳定的 Markdown 表格。
    功能：返回表头、分隔行和数据行组成的多行文本。
    实现逻辑：按 columns 固定顺序输出每一行，缺值时补空字符串。
    可调参数：列头定义和行数据。
    默认参数及原因：空表时返回占位行，原因是需要显式告诉 agent 当前还没补值。
    """

    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    if not rows:
        empty_row = "| " + " | ".join(["待补"] * len(columns)) + " |"
        return [header, divider, empty_row]
    return [
        header,
        divider,
        *[
            "| " + " | ".join(str(row.get(column, "")) for column in columns) + " |"
            for row in rows
        ],
    ]


def _render_entry_block(entry: RegistryEntry, evidence_links: dict[str, list[str]]) -> list[str]:
    """
    目的：把单条 entry 渲染成适合 LLM 和人工一起阅读的 Markdown 小节。
    功能：输出条目元信息、正文内容、修订说明和证据统计。
    实现逻辑：单值型直接输出正文；表格型额外插入 Markdown 表格。
    可调参数：`entry`。
    默认参数及原因：默认保留完整元信息，原因是 QA 和 sub-crew 都需要就地判断下一步动作。
    """

    evidence_count = sum(len(items) for items in evidence_links.values())
    lines = [
        f"### {entry.entry_id} | {entry.title}",
        f"- 类型：{entry.entry_type}",
        f"- 主题：{entry.topic}",
        f"- 责任 crew：{entry.owner_crew}",
        f"- 优先级：{entry.priority}",
        f"- 状态：{entry.status}",
        f"- 内容形态：{entry.content_type}",
        f"- 说明：{entry.description or '-'}",
        f"- 来源：{entry.source or '-'}",
        f"- 置信度：{entry.confidence or '-'}",
        f"- 修订说明：{entry.revision_detail or '-'}",
        f"- 证据数：{evidence_count}",
    ]
    if entry.content_type == "table":
        lines.extend(["- 内容：", *_render_table_markdown(entry.columns, entry.content)])  # type: ignore[arg-type]
    else:
        lines.extend(
            [
                f"- 内容：{entry.content or '-'}",
                f"- 单位：{entry.unit or '-'}",
                f"- 期间：{entry.period or '-'}",
            ]
        )
    return lines


def _render_markdown_from_snapshot(
    snapshot: EvidenceRegistrySnapshot,
    *,
    filter_entry_type: RegistryEntryType | None = None,
    include_statuses: list[str] | None = None,
    exclude_statuses: list[str] | None = None,
    owner_crew: str = "",
    topic: str = "",
    title_contains: str = "",
) -> str:
    """
    目的：把当前 registry 快照渲染成适合 LLM 和人工阅读的 Markdown。
    功能：按责任 crew 分组输出 entry 详情，并附最近证据与备注。
    实现逻辑：先过滤 entry，再按 owner_crew 和 entry_id 排序后分组渲染。
    可调参数：类型、状态、owner、topic 和标题关键词过滤条件。
    默认参数及原因：默认输出完整账本视图，原因是大多数审阅场景都需要先看全貌。
    """

    evidence_index = _build_entry_evidence_index(snapshot)
    filtered_entries = [
        entry
        for entry in snapshot.entries
        if _entry_matches_filters(
            entry,
            filter_entry_type=filter_entry_type,
            include_statuses=include_statuses,
            exclude_statuses=exclude_statuses,
            owner_crew=owner_crew,
            topic=topic,
            title_contains=title_contains,
        )
    ]
    filtered_entries.sort(
        key=lambda entry: (
            entry.owner_crew,
            entry.priority,
            entry.status,
            entry.entry_id,
        )
    )

    lines = [
        f"# 证据注册表：{snapshot.company_name} | {snapshot.industry}",
        f"更新时间：{snapshot.updated_at}",
        f"条目数：{len(filtered_entries)} / {len(snapshot.entries)}",
        f"证据数：{len(snapshot.evidence)}",
    ]
    if owner_crew:
        lines.append(f"责任 crew 过滤：{owner_crew}")
    if topic:
        lines.append(f"主题过滤：{topic}")
    lines.append("")

    if not filtered_entries:
        lines.append("当前过滤条件下没有匹配的 entry。")
    else:
        current_owner = ""
        for entry in filtered_entries:
            if entry.owner_crew != current_owner:
                current_owner = entry.owner_crew
                lines.extend(["", f"## {current_owner}"])
            lines.extend(_render_entry_block(entry, evidence_index.get(entry.entry_id, {})))
            lines.append("")

    recent_evidence = snapshot.evidence[-10:]
    if recent_evidence:
        lines.extend(["## 最近证据"])
        for evidence in recent_evidence:
            lines.append(
                f"- {evidence.evidence_id} | {evidence.title} | {evidence.stance} | "
                f"{evidence.pack_name} | {evidence.source_ref or '-'}"
            )

    if snapshot.notes:
        lines.extend(["", "## 最近备注", *[f"- {note}" for note in snapshot.notes[-10:]]])

    return "\n".join(lines).strip()


def render_registry_markdown(
    registry_path: str | Path,
    *,
    filter_entry_type: RegistryEntryType | None = None,
    include_statuses: list[str] | None = None,
    exclude_statuses: list[str] | None = None,
    owner_crew: str = "",
    topic: str = "",
    title_contains: str = "",
) -> str:
    """
    目的：给 QA、crew agent 和人工调试提供稳定的 Markdown 视图。
    功能：读取 registry 后输出分组后的 Markdown 条目清单。
    实现逻辑：先加载快照，再调用统一渲染函数。
    可调参数：类型、状态、owner、topic 和标题关键词过滤。
    默认参数及原因：默认输出完整账本视图，原因是大多数审阅场景都需要先看全貌。
    """

    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        return _render_markdown_from_snapshot(
            snapshot,
            filter_entry_type=filter_entry_type,
            include_statuses=include_statuses,
            exclude_statuses=exclude_statuses,
            owner_crew=owner_crew,
            topic=topic,
            title_contains=title_contains,
        )


def _write_markdown_snapshot(snapshot: EvidenceRegistrySnapshot, registry_path: Path) -> None:
    """
    目的：在每次 JSON 保存后同步刷新可读 Markdown 快照。
    功能：把 registry 当前内容写到 `registry_snapshot.md`。
    实现逻辑：复用统一的 Markdown 渲染器，并把结果写到 registry 所在目录。
    可调参数：快照对象和 JSON 路径。
    默认参数及原因：文件名固定为 `registry_snapshot.md`，原因是人工排查时需要一个稳定入口。
    """

    markdown_path = registry_path.with_name("registry_snapshot.md")
    markdown_path.write_text(_render_markdown_from_snapshot(snapshot), encoding="utf-8")


def save_registry(snapshot: EvidenceRegistrySnapshot, registry_path: str | Path) -> str:
    """
    目的：把 registry 快照安全落盘。
    功能：更新 `updated_at`，确保目录存在，并同时写 JSON 与 Markdown 视图。
    实现逻辑：在同一把路径锁里完成写盘，避免并发时出现半截 JSON 或视图错位。
    可调参数：`snapshot` 和 `registry_path`。
    默认参数及原因：写盘前总是刷新时间戳，原因是便于判断最后一次修改时点。
    """

    with _registry_transaction(registry_path) as path:
        path.parent.mkdir(parents=True, exist_ok=True)
        snapshot.updated_at = utc_timestamp()
        path.write_text(snapshot.model_dump_json(indent=2, by_alias=True), encoding="utf-8")
        _write_markdown_snapshot(snapshot, path)
        return path.as_posix()


def load_registry(registry_path: str | Path) -> EvidenceRegistrySnapshot:
    """
    目的：提供统一的 registry 读取入口。
    功能：读取 JSON 并还原成 `EvidenceRegistrySnapshot`。
    实现逻辑：固定按 UTF-8 读取，并由 Pydantic 还原统一 entry 结构。
    可调参数：`registry_path`。
    默认参数及原因：固定按 UTF-8 读取，原因是项目产物统一使用 UTF-8。
    """

    with _registry_transaction(registry_path) as path:
        return EvidenceRegistrySnapshot.model_validate_json(path.read_text(encoding="utf-8"))


def initialize_registry_from_template(
    company_name: str,
    industry: str,
    entries: list[RegistryEntry],
    registry_path: str | Path,
) -> str:
    """
    目的：把已校验好的模板 entries 写成一份完整的 registry。
    功能：创建公司信息、entry 列表和初始备注，并一次性落盘。
    实现逻辑：直接构造 `EvidenceRegistrySnapshot` 后调用统一保存入口。
    可调参数：公司名、行业名、entry 列表和 registry 路径。
    默认参数及原因：初始化时不预放证据，原因是证据必须随研究逐步进入账本。
    """

    snapshot = EvidenceRegistrySnapshot(
        company_name=company_name,
        industry=industry,
        entries=entries,
        notes=["Seeded registry from deterministic YAML template."],
    )
    return save_registry(snapshot, registry_path)


def initialize_registry(
    company_name: str,
    industry: str,
    registry_path: str | Path,
    *,
    template_path: str | Path | None = None,
    periods: dict[str, str] | None = None,
) -> str:
    """
    目的：为 Flow 初始化一份干净的 evidence registry。
    功能：加载固定模板并把模板条目落盘到 registry。
    实现逻辑：先读取模板，再调用统一的模板初始化函数。
    可调参数：公司名、行业名、registry 路径、可选模板路径和期间映射。
    默认参数及原因：默认使用仓库内模板文件，原因是当前 planning 已改为确定性初始化。
    """

    entries = load_registry_template(company_name, industry, template_path=template_path, periods=periods)
    return initialize_registry_from_template(company_name, industry, entries, registry_path)


def entry_ids_for_packs(
    registry_path: str | Path,
    pack_names: list[str],
    *,
    entry_types: list[RegistryEntryType] | None = None,
) -> list[str]:
    """
    目的：按当前 registry 里的真实 entry 集合动态查找 pack 对应的 entry ID。
    功能：读取账本后返回指定 pack 列表下的 entry ID，且自动去重保序。
    实现逻辑：先把 pack 映射到 registry topic，再按 topic 和类型筛选。
    可调参数：`pack_names` 和可选的 `entry_types` 过滤条件。
    默认参数及原因：找不到时返回空列表，原因是 QA 反馈可能只命中部分 pack。
    """

    with _registry_transaction(registry_path) as path:
        if not path.exists():
            return []
        snapshot = load_registry(path)
        topic_set = {
            PACK_TO_REGISTRY_TOPIC[pack_name]
            for pack_name in pack_names
            if pack_name.strip() and pack_name in PACK_TO_REGISTRY_TOPIC
        }
        entry_type_set = set(entry_types or [])
        ordered_entry_ids: list[str] = []
        seen_entry_ids: set[str] = set()
        for entry in snapshot.entries:
            if topic_set and entry.topic not in topic_set:
                continue
            if entry_type_set and entry.entry_type not in entry_type_set:
                continue
            if entry.entry_id in seen_entry_ids:
                continue
            seen_entry_ids.add(entry.entry_id)
            ordered_entry_ids.append(entry.entry_id)
        return ordered_entry_ids


def find_entry(snapshot: EvidenceRegistrySnapshot, entry_id: str) -> RegistryEntry | None:
    """
    目的：避免多个写操作各自手写 entry 查找逻辑。
    功能：按 `entry_id` 在快照里返回对应 entry。
    实现逻辑：线性扫描当前 entry 列表并返回首个命中项。
    可调参数：`snapshot` 和 `entry_id`。
    默认参数及原因：找不到时返回 `None`，原因是调用方通常需要自己决定跳过还是补救。
    """

    for entry in snapshot.entries:
        if entry.entry_id == entry_id:
            return entry
    return None


def register_evidence(
    registry_path: str | Path,
    *,
    title: str,
    summary: str,
    source_type: str,
    source_ref: str,
    pack_name: str,
    entry_ids: list[str],
    stance: str = "support",
    note: str = "",
) -> str:
    """
    目的：把 pack、QA 或人工整理出的新证据挂到 registry 上。
    功能：创建 `EvidenceRecord`，并把证据关联到对应 entry。
    实现逻辑：在同一事务里追加证据、更新证据 ID 关联和必要的修订状态，再统一保存。
    可调参数：证据标题、摘要、来源、pack、关联 entry 列表和 stance。
    默认参数及原因：`stance` 默认 `support`，因为大多数新增证据首先用于支持已有结论。
    """

    normalized_pack_name = pack_name.strip()
    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        evidence_id = f"ev_{uuid.uuid4().hex[:12]}"
        record = EvidenceRecord(
            evidence_id=evidence_id,
            title=title,
            summary=summary,
            source_type=source_type,
            source_ref=source_ref,
            pack_name=normalized_pack_name,
            entry_ids=entry_ids,
            stance=stance,  # type: ignore[arg-type]
            note=note,
        )
        snapshot.evidence.append(record)

        for entry_id in entry_ids:
            entry = find_entry(snapshot, entry_id)
            if not entry:
                continue
            if stance == "conflict":
                entry.status = "need_revision"
                if title and title not in entry.revision_detail:
                    detail_prefix = "新增冲突证据："
                    entry.revision_detail = (
                        f"{entry.revision_detail}；{detail_prefix}{title}"
                        if entry.revision_detail
                        else f"{detail_prefix}{title}"
                    )
            entry.last_updated_at = utc_timestamp()

        save_registry(snapshot, registry_path)
        return evidence_id


def add_discovered_entry(registry_path: str | Path, entry: RegistryEntry) -> None:
    """
    目的：把研究过程中新增的 entry 安全追加到 registry。
    功能：先检查是否重号，再把新 entry 写回快照。
    实现逻辑：命中重复 `entry_id` 时直接跳过，避免重复创建同一条记录。
    可调参数：`registry_path` 和 `entry`。
    默认参数及原因：重复时直接跳过，原因是运行期发现问题宁可保守也不要制造重复记录。
    """

    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        if find_entry(snapshot, entry.entry_id):
            return
        snapshot.entries.append(entry)
        save_registry(snapshot, registry_path)


def update_entry_fields(
    registry_path: str | Path,
    entry_id: str,
    **fields: Any,
) -> None:
    """
    目的：给已存在的 entry 提供受控的字段更新入口。
    功能：按 `entry_id` 更新正文、表格、来源、状态和修订说明等核心字段。
    实现逻辑：先取出现有 entry，再用“旧值 + 新字段”重建模型并回写。
    可调参数：`entry_id` 和任意允许覆盖的 entry 字段。
    默认参数及原因：只忽略值为 `None` 的字段，原因是空字符串和空列表也可能是合法更新。
    """

    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        for index, existing_entry in enumerate(snapshot.entries):
            if existing_entry.entry_id != entry_id:
                continue
            payload = existing_entry.model_dump()
            for key, value in fields.items():
                if value is None or key not in payload:
                    continue
                payload[key] = value
            payload["last_updated_at"] = utc_timestamp()
            snapshot.entries[index] = RegistryEntry.model_validate(payload)
            save_registry(snapshot, registry_path)
            return
        raise ValueError(f"Entry not found: {entry_id}")


def update_entry_status(
    registry_path: str | Path,
    entry_ids: list[str],
    *,
    status: str,
    revision_detail: str = "",
) -> None:
    """
    目的：统一更新 entry 状态，避免不同模块各自写状态回写逻辑。
    功能：批量更新状态、修订说明和更新时间。
    实现逻辑：在同一事务里逐条更新命中的 entry 后统一保存。
    可调参数：entry 列表、目标状态和 revision_detail。
    默认参数及原因：修订说明默认空串，原因是并不是每次状态变化都需要补文字。
    """

    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        for entry_id in entry_ids:
            entry = find_entry(snapshot, entry_id)
            if not entry:
                continue
            entry.status = status  # type: ignore[assignment]
            if revision_detail.strip():
                entry.revision_detail = revision_detail.strip()
            entry.last_updated_at = utc_timestamp()
        save_registry(snapshot, registry_path)


def append_registry_note(registry_path: str | Path, note: str) -> None:
    """
    目的：给 registry 提供最轻量的追加备注入口。
    功能：把新的说明文字追加到 `notes` 列表并立即落盘。
    实现逻辑：在同一事务里读取、追加并保存。
    可调参数：`registry_path` 和 `note`。
    默认参数及原因：追加后立即保存，原因是备注通常用于人工追踪，不适合只留在内存。
    """

    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        snapshot.notes.append(note)
        save_registry(snapshot, registry_path)


def record_registry_review(
    registry_path: str | Path,
    *,
    reviewer: str,
    pack_name: str,
    summary: str,
    has_changes: bool,
    new_entry_ids: list[str] | None = None,
    touched_entry_ids: list[str] | None = None,
    next_action: str = "",
) -> None:
    """
    目的：强制把各 agent 对 registry 的审阅动作留下可追踪记录。
    功能：无论本轮有没有实际改动，都向 registry 追加一条结构化 review 备注。
    实现逻辑：把 reviewer、pack、变更状态、entry 列表和摘要压成单行备注。
    可调参数：reviewer、pack_name、summary、变更标记、entry 列表和 next_action。
    默认参数及原因：entry 列表默认空列表，原因是有些审阅确实只是确认“当前无需改动”。
    """

    normalized_new_ids = [entry_id for entry_id in new_entry_ids or [] if entry_id]
    normalized_touched_ids = [entry_id for entry_id in touched_entry_ids or [] if entry_id]
    review_payload = {
        "reviewer": reviewer,
        "pack_name": pack_name.strip(),
        "status": "updated" if has_changes else "no_change",
        "new_entry_ids": normalized_new_ids,
        "touched_entry_ids": normalized_touched_ids,
        "summary": summary[:500],
        "next_action": next_action[:300],
    }
    append_registry_note(
        registry_path,
        f"registry_review: {json.dumps(review_payload, ensure_ascii=False, sort_keys=True)}",
    )


def summarize_registry(registry_path: str | Path) -> str:
    """
    目的：给 agent 和调试流程提供一份足够短但可用的 registry 摘要。
    功能：提取公司信息、entry 列表、证据数量和最近备注。
    实现逻辑：读取快照后压成精简 JSON 文本返回。
    可调参数：registry 路径。
    默认参数及原因：最近备注只保留最后 10 条，原因是避免 prompt 过长。
    """

    with _registry_transaction(registry_path):
        snapshot = load_registry(registry_path)
        payload = {
            "company_name": snapshot.company_name,
            "industry": snapshot.industry,
            "entry_count": len(snapshot.entries),
            "entries": [
                {
                    "entry_id": entry.entry_id,
                    "entry_type": entry.entry_type,
                    "content_type": entry.content_type,
                    "topic": entry.topic,
                    "owner_crew": entry.owner_crew,
                    "title": entry.title,
                    "status": entry.status,
                    "priority": entry.priority,
                    "source": entry.source,
                    "confidence": entry.confidence,
                }
                for entry in snapshot.entries
            ],
            "evidence_count": len(snapshot.evidence),
            "notes": snapshot.notes[-10:],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)


def save_registry_snapshot(registry_path: str | Path, snapshot_path: str | Path) -> str:
    """
    目的：把当前 registry 固化成一个可回溯的阶段快照。
    功能：复制当前 JSON 到指定 snapshot 路径。
    实现逻辑：在读取锁内拿到最新路径内容，再复制到目标位置。
    可调参数：当前 registry 路径和目标 snapshot 路径。
    默认参数及原因：直接复制原始 JSON，原因是后续 diff 更适合基于结构化原文。
    """

    target_path = Path(snapshot_path).expanduser().resolve()
    with _registry_transaction(registry_path) as source_path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, target_path)
    return target_path.as_posix()
