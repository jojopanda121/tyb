from __future__ import annotations

import json
from pathlib import Path

import pytest

from automated_research_report_generator.flow.registry import initialize_registry, load_registry
from automated_research_report_generator.tools.registry_tools import (
    AddEntryTool,
    ReadRegistryTool,
    UpdateEntryTool,
    set_evidence_registry_context,
)


def _prepare_registry(tmp_path: Path) -> str:
    """
    目的：为 registry 工具稳定性测试准备一份独立可写的临时账本。
    功能：初始化 deterministic registry，并把线程本地上下文指向该账本。
    实现逻辑：先在临时目录下创建 registry，再调用 `set_evidence_registry_context()` 绑定当前测试线程。
    可调参数：`tmp_path` 由 pytest 提供，用于隔离测试文件。
    默认参数及原因：公司名和行业名固定为测试值，原因是这里关注的是工具契约而不是业务内容。
    """

    registry_path = tmp_path / "registry.json"
    initialize_registry("Test Co", "Automation", registry_path)
    set_evidence_registry_context(registry_path.as_posix())
    return registry_path.as_posix()


def _entry_by_id(registry_path: str, entry_id: str):
    """
    目的：给本文件里的断言提供统一的 entry 定位入口。
    功能：从当前 registry 快照中按 `entry_id` 找到目标条目。
    实现逻辑：加载快照后顺序遍历 entries，命中即返回；找不到则抛断言错误。
    可调参数：`registry_path` 和 `entry_id`。
    默认参数及原因：找不到条目时直接失败，原因是这类测试本来就是围绕单个明确 entry 写的。
    """

    snapshot = load_registry(registry_path)
    for entry in snapshot.entries:
        if entry.entry_id == entry_id:
            return entry
    raise AssertionError(f"entry not found: {entry_id}")


def test_add_entry_normalizes_chinese_topic_alias_to_canonical_topic(tmp_path):
    """
    目的：锁定已知中文 topic 别名会被归一化到 registry 规范 token。
    功能：验证 `add_entry` 在只传中文包名别名时，仍能正确推断 topic 和 owner_crew。
    实现逻辑：初始化临时账本后调用 `AddEntryTool`，再回读 entry 断言归一化结果。
    可调参数：`tmp_path`。
    默认参数及原因：默认使用“历史背景分析包”这个高频误填别名，原因是这正是本轮要兜住的真实噪音。
    """

    registry_path = _prepare_registry(tmp_path)

    AddEntryTool()._run(
        entry_id="J_TEST_ALIAS_001",
        entry_type="judgment",
        topic="历史背景分析包",
        title="测试中文 topic 归一化",
        content_type="single",
        content="alias topic",
    )

    entry = _entry_by_id(registry_path, "J_TEST_ALIAS_001")

    assert entry.topic == "history"
    assert entry.owner_crew == "history_background_crew"


def test_add_entry_backfills_topic_from_owner_crew_when_topic_missing(tmp_path):
    """
    目的：锁定 topic 缺失但 owner_crew 存在时，会按默认映射自动回填 topic。
    功能：验证 `add_entry` 不需要 agent 额外猜测 topic，也不会把缺失 topic 直接写坏。
    实现逻辑：只传入 `owner_crew` 新建 entry，再回读条目确认 topic 已被自动补齐。
    可调参数：`tmp_path`。
    默认参数及原因：默认选择 `business_crew`，原因是其 topic 映射唯一且最适合做最小回填断言。
    """

    registry_path = _prepare_registry(tmp_path)

    AddEntryTool()._run(
        entry_id="J_TEST_OWNER_001",
        owner_crew="business_crew",
        title="测试 owner 回填 topic",
        content_type="single",
        content="owner inferred topic",
    )

    entry = _entry_by_id(registry_path, "J_TEST_OWNER_001")

    assert entry.owner_crew == "business_crew"
    assert entry.topic == "business"


def test_update_entry_shifts_single_from_entry_type_to_content_type_and_keeps_other_invalid_values_failing(tmp_path):
    """
    目的：锁定已知 `entry_type=single` 误填会被有限纠偏，而其他非法值仍然暴露真实错误。
    功能：验证 `update_entry` 会把误传到 `entry_type` 的 `single` 转移到 `content_type`，同时继续拒绝未收录的非法枚举。
    实现逻辑：先创建一个 table 条目，再用误填参数把它改回 single，最后断言非法值仍抛异常。
    可调参数：`tmp_path`。
    默认参数及原因：默认用 table -> single 的切换场景，原因是这样最容易直接观察纠偏是否生效。
    """

    registry_path = _prepare_registry(tmp_path)

    AddEntryTool()._run(
        entry_id="D_TEST_SHAPE_001",
        entry_type="data",
        owner_crew="risk_crew",
        title="测试 entry_type 误填纠偏",
        content_type="table",
        columns=["字段"],
        content=[{"字段": "初始值"}],
    )

    UpdateEntryTool()._run(
        entry_id="D_TEST_SHAPE_001",
        entry_type="single",
        content="修正后的单值",
        columns=[],
    )

    entry = _entry_by_id(registry_path, "D_TEST_SHAPE_001")

    assert entry.entry_type == "data"
    assert entry.content_type == "single"
    assert entry.content == "修正后的单值"

    with pytest.raises(ValueError):
        UpdateEntryTool()._run(
            entry_id="D_TEST_SHAPE_001",
            entry_type="invalid_entry_type",
        )


def test_read_registry_entry_detail_without_entry_ids_returns_structured_invalid_request(tmp_path):
    """
    目的：锁定 `view=entry_detail` 在缺少 `entry_ids` 时不再抛异常，而是返回结构化错误结果。
    功能：验证 read 工具能把错误原因和下一步提示一起返回给 agent。
    实现逻辑：初始化临时账本后直接调用 `ReadRegistryTool`，再解析返回 JSON 做字段级断言。
    可调参数：`tmp_path`。
    默认参数及原因：默认不传任何 `entry_ids`，原因是这正是本轮要降噪的高频错误调用形态。
    """

    _prepare_registry(tmp_path)

    payload = json.loads(ReadRegistryTool()._run(view="entry_detail", entry_ids=[]))

    assert payload["status"] == "invalid_request"
    assert payload["view"] == "entry_detail"
    assert payload["message"] == "entry_ids is required when view=entry_detail."
    assert "view=markdown" in payload["hint"]
    assert "view=entry_list" in payload["hint"]
