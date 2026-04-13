from __future__ import annotations

import inspect
from pathlib import Path

from crewai import Process
import yaml

from automated_research_report_generator.crews.business_crew.business_crew import BusinessCrew
from automated_research_report_generator.crews.crew_profile_loader import _PROFILE_KEYS_BY_TASK
from automated_research_report_generator.crews.financial_crew.financial_crew import FinancialCrew
from automated_research_report_generator.crews.history_background_crew.history_background_crew import (
    HistoryBackgroundCrew,
)
from automated_research_report_generator.crews.industry_crew.industry_crew import IndustryCrew
from automated_research_report_generator.crews.operating_metrics_crew.operating_metrics_crew import (
    OperatingMetricsCrew,
)
from automated_research_report_generator.crews.peer_info_crew.peer_info_crew import PeerInfoCrew
from automated_research_report_generator.crews.risk_crew.risk_crew import RiskCrew
from automated_research_report_generator.flow.common import PROJECT_ROOT
from automated_research_report_generator.flow.research_flow import ResearchReportFlow
from automated_research_report_generator.flow.registry import load_registry_template

EXPECTED_TASK_ORDER = [
    "extract_file_facts",
    "record_extract_registry",
    "search_facts",
    "record_search_registry",
    "check_registry",
    "synthesize_and_output",
]

EXPECTED_CONTEXT_CHAIN = {
    "extract_file_facts": [],
    "record_extract_registry": ["extract_file_facts"],
    "search_facts": ["record_extract_registry"],
    "record_search_registry": ["search_facts"],
    "check_registry": ["record_extract_registry", "record_search_registry"],
    "synthesize_and_output": ["record_extract_registry", "record_search_registry", "check_registry"],
}

EXPECTED_RECORDING_TOOLS = [
    "ReadRegistryTool",
    "UpdateEntryTool",
    "AddEntryTool",
    "AddEvidenceTool",
    "StatusUpdateTool",
    "RegistryReviewTool",
]

EXPECTED_QA_TOOLS = [
    "ReadRegistryTool",
    "StatusUpdateTool",
    "RegistryReviewTool",
]

EXPECTED_SYNTH_TOOLS = [
    "ReadRegistryTool",
    "RegistryReviewTool",
]


def _research_subcrew_instances() -> list[object]:
    """
    目的：集中返回 research 阶段真实使用的 7 个 sub-crew 实例。
    功能：避免每个测试重复手写同一组 crew。
    实现逻辑：直接实例化当前主 Flow 使用的 7 个 research sub-crews。
    可调参数：当前无。
    默认参数及原因：固定覆盖 7 个 research packs，原因是本轮改造要求全量核对而不是抽样。
    """

    return [
        HistoryBackgroundCrew(),
        IndustryCrew(),
        BusinessCrew(),
        PeerInfoCrew(),
        FinancialCrew(),
        OperatingMetricsCrew(),
        RiskCrew(),
    ]


def _crew_module_dir(crew_instance: object) -> Path:
    """
    目的：定位单个 crew 的模块目录。
    功能：为读取该 crew 的 `config/agents.yaml` 和 `config/tasks.yaml` 提供统一入口。
    实现逻辑：通过 `inspect.getfile()` 找到类定义文件，再回到其父目录。
    可调参数：`crew_instance`。
    默认参数及原因：固定读取实例所属模块，原因是不同 crew 的配置分散在各自目录下。
    """

    return Path(inspect.getfile(crew_instance.__class__)).resolve().parent


def _crew_yaml_payloads(crew_instance: object) -> tuple[dict[str, object], dict[str, object]]:
    """
    目的：统一读取单个 crew 的 agents/task YAML 配置。
    功能：减少测试体内重复的文件读取样板代码。
    实现逻辑：基于模块目录拼接配置路径，并以 UTF-8 方式加载 YAML。
    可调参数：`crew_instance`。
    默认参数及原因：固定返回 `(agents_yaml, tasks_yaml)`，原因是这两份配置始终成对校验。
    """

    module_dir = _crew_module_dir(crew_instance)
    agents_yaml = yaml.safe_load((module_dir / "config" / "agents.yaml").read_text(encoding="utf-8"))
    tasks_yaml = yaml.safe_load((module_dir / "config" / "tasks.yaml").read_text(encoding="utf-8"))
    return agents_yaml, tasks_yaml


def _tool_names(owner: object) -> list[str]:
    """
    目的：把 agent 或 task 上挂载的工具转换成稳定的工具名列表。
    功能：便于对 task 级工具隔离做精确断言。
    实现逻辑：遍历 `.tools` 属性并提取每个工具对象的类名。
    可调参数：`owner`，可以是 CrewAI Agent 或 Task。
    默认参数及原因：默认只看工具类名，原因是本轮验证关注的是权限边界而不是工具内部参数。
    """

    return [type(tool).__name__ for tool in getattr(owner, "tools", [])]


def _context_names(task: object) -> list[str]:
    """
    目的：提取 task 的上下文依赖链。
    功能：把 CrewAI Task 对象里的 context 转成可断言的 task 名列表。
    实现逻辑：仅在 context 真的是列表时读取每个上游 task 的 `.name`。
    可调参数：`task`。
    默认参数及原因：遇到空或未指定 context 返回空列表，原因是 `extract_file_facts` 本就应当是链路起点。
    """

    context = getattr(task, "context", None)
    if not isinstance(context, list):
        return []
    return [upstream_task.name for upstream_task in context]


def test_research_subcrew_yaml_configs_match_six_step_registry_pipeline():
    """
    目的：锁定 7 个 research sub-crew 的 YAML 结构已经切到 6-task 链路。
    功能：检查 task 顺序、manager/worker 配置和专题元数据字段是否与运行时代码保持一致。
    实现逻辑：全量遍历 7 个 crews，读取 YAML 后逐项断言关键键位与专题 profile。
    可调参数：当前无。
    默认参数及原因：固定对全部 crews 校验，原因是本轮改造不允许只抽样验证。
    """

    assert tuple(_PROFILE_KEYS_BY_TASK) == (
        "search_facts",
        "extract_file_facts",
        "check_registry",
        "synthesize_and_output",
    )

    for crew_instance in _research_subcrew_instances():
        agents_yaml, tasks_yaml = _crew_yaml_payloads(crew_instance)

        assert list(tasks_yaml.keys()) == EXPECTED_TASK_ORDER
        assert set(agents_yaml.keys()) == {
            "manager_agent",
            "search_fact_agent",
            "extract_file_fact_agent",
            "qa_check_agent",
            "synthesizing_agent",
        }
        assert tasks_yaml["search_facts"]["crew_name"] == crew_instance.crew_name
        assert tasks_yaml["search_facts"]["pack_name"] == crew_instance.pack_name
        assert tasks_yaml["search_facts"]["pack_title"] == crew_instance.pack_title
        assert tasks_yaml["search_facts"]["pack_focus"] == crew_instance.pack_focus
        assert tasks_yaml["search_facts"]["output_title"] == crew_instance.output_title
        assert tasks_yaml["search_facts"]["search_guidance"] == crew_instance.search_guidance
        assert tasks_yaml["extract_file_facts"]["extract_guidance"] == crew_instance.extract_guidance
        assert tasks_yaml["check_registry"]["qa_guidance"] == crew_instance.qa_guidance
        assert tasks_yaml["synthesize_and_output"]["synthesize_guidance"] == crew_instance.synthesize_guidance
        assert tasks_yaml["synthesize_and_output"]["output_skeleton"] == crew_instance.output_skeleton
        assert tasks_yaml["synthesize_and_output"]["output_file"] == "{pack_output_path}"


def test_research_subcrew_runtime_builds_six_step_pipeline_with_custom_manager():
    """
    目的：确认 7 个 research sub-crew 在运行时都能构建出新的 6-task 管线。
    功能：验证 hierarchical process、自定义 manager agent 和 4 个 worker agent 的装配结果。
    实现逻辑：逐个实例化 runtime crew，并断言任务数量、顺序和 manager 边界。
    可调参数：当前无。
    默认参数及原因：固定构建全部 crews，原因是要验证改造后的真实运行对象，而不是只看静态 YAML。
    """

    for crew_instance in _research_subcrew_instances():
        runtime_crew = crew_instance.crew()

        assert runtime_crew.process in {Process.hierarchical, "hierarchical"}
        assert len(runtime_crew.agents) == 4
        assert len(runtime_crew.tasks) == 6
        assert [task.name for task in runtime_crew.tasks] == EXPECTED_TASK_ORDER
        assert runtime_crew.manager_agent is not None
        assert runtime_crew.manager_agent not in runtime_crew.agents
        assert runtime_crew.manager_llm is None
        assert runtime_crew.manager_agent.allow_delegation is True
        assert _tool_names(runtime_crew.manager_agent) == []
        assert all(_tool_names(agent) == [] for agent in runtime_crew.agents)


def test_research_subcrew_runtime_uses_task_level_tool_isolation_and_context_chain():
    """
    目的：锁定 task 级工具隔离和上下文链条已经在运行时生效。
    功能：检查 6 个 tasks 的 context 依赖与工具权限是否符合 collect/record/qa/synth 分层。
    实现逻辑：逐个 crew 构建 runtime task map，再按 task 名断言工具名和上游依赖。
    可调参数：当前无。
    默认参数及原因：固定全量验证 7 个 crews，原因是工具权限回退会直接破坏 registry 回填约束。
    """

    for crew_instance in _research_subcrew_instances():
        runtime_crew = crew_instance.crew()
        task_map = {task.name: task for task in runtime_crew.tasks}

        assert _context_names(task_map["extract_file_facts"]) == EXPECTED_CONTEXT_CHAIN["extract_file_facts"]
        assert _context_names(task_map["record_extract_registry"]) == EXPECTED_CONTEXT_CHAIN["record_extract_registry"]
        assert _context_names(task_map["search_facts"]) == EXPECTED_CONTEXT_CHAIN["search_facts"]
        assert _context_names(task_map["record_search_registry"]) == EXPECTED_CONTEXT_CHAIN["record_search_registry"]
        assert _context_names(task_map["check_registry"]) == EXPECTED_CONTEXT_CHAIN["check_registry"]
        assert _context_names(task_map["synthesize_and_output"]) == EXPECTED_CONTEXT_CHAIN["synthesize_and_output"]

        assert _tool_names(task_map["extract_file_facts"]) == [
            "ReadPdfPageIndexTool",
            "ReadPdfPagesTool",
            "ReadRegistryTool",
        ]
        assert _tool_names(task_map["record_extract_registry"]) == EXPECTED_RECORDING_TOOLS
        assert _tool_names(task_map["record_search_registry"]) == EXPECTED_RECORDING_TOOLS
        assert _tool_names(task_map["check_registry"]) == EXPECTED_QA_TOOLS
        assert _tool_names(task_map["synthesize_and_output"]) == EXPECTED_SYNTH_TOOLS

        search_tools = _tool_names(task_map["search_facts"])
        assert "SerperDevTool" in search_tools
        assert "ReadRegistryTool" in search_tools
        for forbidden_tool in [
            "ReadPdfPageIndexTool",
            "ReadPdfPagesTool",
            "UpdateEntryTool",
            "AddEntryTool",
            "AddEvidenceTool",
            "StatusUpdateTool",
        ]:
            assert forbidden_tool not in search_tools


def test_research_subcrew_prompts_enforce_collect_record_and_read_only_boundaries():
    """
    目的：锁定 prompt 层已经把“采集”“登记”“检查”“只读综合”四类职责拆开。
    功能：检查 manager 强制完整 6 段链路，collect task 输出候选 patch，record task 只做回填，synth task 严格只读。
    实现逻辑：逐个 crew 读取 YAML 描述和 expected_output，再断言关键 task 名、工具名和输出段落标记。
    可调参数：当前无。
    默认参数及原因：固定检查全部 crews，原因是 prompt 漏改会导致 agent 在正确工具边界下仍然乱做事。
    """

    for crew_instance in _research_subcrew_instances():
        agents_yaml, tasks_yaml = _crew_yaml_payloads(crew_instance)
        manager_backstory = str(agents_yaml["manager_agent"]["backstory"])

        for task_name in EXPECTED_TASK_ORDER:
            assert task_name in manager_backstory
        assert "qa_feedback" in manager_backstory
        assert "loop_reason" in manager_backstory

        for task_name in ("extract_file_facts", "search_facts"):
            description = str(tasks_yaml[task_name]["description"])
            expected_output = str(tasks_yaml[task_name]["expected_output"])

            assert 'owner_crew="{owner_crew}"' in description
            assert "read_registry" in description
            assert "update_entry" in description
            assert "add_entry" in description
            assert "add_evidence" in description
            assert "status_update" in description
            assert "existing_entry_updates" in expected_output
            assert "new_entry_candidates" in expected_output
            assert "unresolved_gaps" in expected_output

        for task_name in ("record_extract_registry", "record_search_registry"):
            description = str(tasks_yaml[task_name]["description"])

            assert 'owner_crew="{owner_crew}"' in description
            for tool_name in EXPECTED_RECORDING_TOOLS:
                needle = tool_name.removesuffix("Tool").lower()
                if tool_name == "ReadRegistryTool":
                    needle = "read_registry"
                elif tool_name == "UpdateEntryTool":
                    needle = "update_entry"
                elif tool_name == "AddEntryTool":
                    needle = "add_entry"
                elif tool_name == "AddEvidenceTool":
                    needle = "add_evidence"
                elif tool_name == "StatusUpdateTool":
                    needle = "status_update"
                elif tool_name == "RegistryReviewTool":
                    needle = "registry_review"
                assert needle in description

        synth_description = str(tasks_yaml["synthesize_and_output"]["description"])
        assert 'owner_crew="{owner_crew}"' in synth_description
        assert 'view="entry_detail"' in synth_description
        assert "update_entry" in synth_description
        assert "add_entry" in synth_description
        assert "add_evidence" in synth_description
        assert "status_update" in synth_description
        assert "registry_review" in synth_description


def test_research_registry_template_is_deterministic_and_covers_all_subcrews():
    """
    目的：锁定 research registry 仍然由固定模板初始化，而不是回退到 planner 生成。
    功能：检查模板条目 ID 唯一，并覆盖当前 7 个 research sub-crews。
    实现逻辑：直接加载模板条目后按 owner_crew 和 entry_id 做集合断言。
    可调参数：当前无。
    默认参数及原因：固定使用测试公司名和行业名，原因是模板里包含占位符替换逻辑。
    """

    entries = load_registry_template("Test Co", "Automation")
    owner_crews = {entry.owner_crew for entry in entries}
    entry_ids = [entry.entry_id for entry in entries]

    assert len(entry_ids) == len(set(entry_ids))
    assert "planning_crew" not in owner_crews
    assert {
        "history_background_crew",
        "industry_crew",
        "business_crew",
        "peer_info_crew",
        "financial_crew",
        "operating_metrics_crew",
        "risk_crew",
    }.issubset(owner_crews)
    assert "D_OPS_001" in entry_ids
    assert any("Test Co" in entry.title for entry in entries)


def test_research_subcrew_inputs_cover_pack_metadata_and_upstream_pack_texts():
    """
    目的：验证 Flow 会把 research sub-crew 真实需要的 pack 元数据和上游 pack 文本补齐。
    功能：同时检查 peer_info、financial 和 operating_metrics 三类依赖 pack 文本的输入装配。
    实现逻辑：构造最小 flow state，并用实例级 `_read()` 桩函数返回上游 pack 文本，再调用 `_research_subcrew_inputs()` 做断言。
    可调参数：当前无。
    默认参数及原因：选择这三个 crew，原因是它们正好覆盖本轮新增的上游 pack 文本依赖，同时避免受限环境中的文件写入噪声。
    """

    flow = ResearchReportFlow()
    flow.state.company_name = "Test Co"
    flow.state.industry = "Automation"
    flow.state.pdf_file_path = "workspace/sample.pdf"
    flow.state.page_index_file_path = "workspace/page_index.json"
    flow.state.document_metadata_file_path = "workspace/document_metadata.md"
    flow.state.run_cache_dir = "workspace/.cache/test-run"
    flow.state.run_output_dir = "workspace/.cache/test-run"
    flow.state.evidence_registry_path = "workspace/registry.json"
    flow.state.industry_pack_path = "workspace/industry_pack.md"
    flow.state.business_pack_path = "workspace/business_pack.md"
    flow.state.peer_info_pack_path = "workspace/peer_info_pack.md"

    read_map = {
        flow.state.document_metadata_file_path: "metadata",
        flow.state.industry_pack_path: "industry pack body",
        flow.state.business_pack_path: "business pack body",
        flow.state.peer_info_pack_path: "peer info pack body",
    }
    flow._read = lambda path: read_map.get(path, "")

    peer_info_crew = PeerInfoCrew()
    peer_inputs = flow._research_subcrew_inputs(
        crew_instance=peer_info_crew,
        pack_name=peer_info_crew.pack_name,
        pack_title=peer_info_crew.pack_title,
        output_path="workspace/peer_info_output.md",
        loop_reason="initial",
        qa_feedback="补同行可比性限制。",
    )
    assert peer_inputs["owner_crew"] == peer_info_crew.crew_name
    assert peer_inputs["search_guidance"] == peer_info_crew.search_guidance
    assert peer_inputs["extract_guidance"] == peer_info_crew.extract_guidance
    assert peer_inputs["qa_guidance"] == peer_info_crew.qa_guidance
    assert peer_inputs["synthesize_guidance"] == peer_info_crew.synthesize_guidance
    assert peer_inputs["output_skeleton"] == peer_info_crew.output_skeleton
    assert peer_inputs["industry_pack_text"] == "industry pack body"
    assert peer_inputs["business_pack_text"] == "business pack body"

    financial_crew = FinancialCrew()
    financial_inputs = flow._research_subcrew_inputs(
        crew_instance=financial_crew,
        pack_name=financial_crew.pack_name,
        pack_title=financial_crew.pack_title,
        output_path="workspace/financial_output.md",
        loop_reason="qa_retry",
        qa_feedback="补财务口径说明。",
    )
    assert financial_inputs["owner_crew"] == financial_crew.crew_name
    assert financial_inputs["peer_info_pack_text"] == "peer info pack body"

    operating_metrics_crew = OperatingMetricsCrew()
    operating_inputs = flow._research_subcrew_inputs(
        crew_instance=operating_metrics_crew,
        pack_name=operating_metrics_crew.pack_name,
        pack_title=operating_metrics_crew.pack_title,
        output_path="workspace/operating_output.md",
        loop_reason="qa_retry",
        qa_feedback="补运营指标口径。",
    )
    assert operating_inputs["owner_crew"] == operating_metrics_crew.crew_name
    assert operating_inputs["peer_info_pack_text"] == "peer info pack body"


def test_research_subcrew_base_module_is_removed():
    """
    目的：锁住旧的 shared research sub-crew base 设计没有被重新引回主路径。
    功能：防止代码回退到已删除的 `research_subcrew_base.py`。
    实现逻辑：直接检查旧模块路径不存在。
    可调参数：当前无。
    默认参数及原因：固定检查历史路径，原因是这个文件一旦回归就意味着结构边界被破坏。
    """

    legacy_base_file = PROJECT_ROOT / "src" / "automated_research_report_generator" / "crews" / "research_subcrew_base.py"
    assert not legacy_base_file.exists()
