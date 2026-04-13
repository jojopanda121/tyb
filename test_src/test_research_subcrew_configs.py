from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from crewai import Process
import yaml

from automated_research_report_generator.crews.business_crew.business_crew import BusinessCrew
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
from automated_research_report_generator.flow.registry import initialize_registry, load_registry_template

pytestmark = pytest.mark.skip(reason="Superseded by 6-task research registry pipeline tests.")


def _research_subcrew_instances() -> list[object]:
    """
    目的：集中返回当前 research flow 使用的 7 个 sub-crew 实例。
    功能：避免每个测试重复手写同一组 crew 列表。
    实现逻辑：直接实例化 7 个独立 crew，并交给调用方做后续断言。
    可调参数：当前无。
    默认参数及原因：固定覆盖 flow 主路径中的 7 个 research sub-crew，原因是这正是本轮重构的边界。
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


def test_research_subcrews_have_yaml_agent_and_task_configs():
    """
    目的：锁住 7 个 research sub-crew 的 YAML 配置没有缺口。
    功能：检查每个 sub-crew 都带有独立的 `agents.yaml` 和 `tasks.yaml`。
    实现逻辑：遍历 7 个 crew，按类文件目录定位配置文件并校验关键键位。
    可调参数：当前无。
    默认参数及原因：只检查最关键的 agent 和最终输出 task，原因是这能最快证明配置已接线。
    """

    for crew_instance in _research_subcrew_instances():
        module_dir = Path(inspect.getfile(crew_instance.__class__)).resolve().parent
        agent_config_path = module_dir / "config" / "agents.yaml"
        task_config_path = module_dir / "config" / "tasks.yaml"

        assert agent_config_path.exists()
        assert task_config_path.exists()

        agent_config = yaml.safe_load(agent_config_path.read_text(encoding="utf-8"))
        task_config = yaml.safe_load(task_config_path.read_text(encoding="utf-8"))

        assert set(agent_config.keys()) == {
            "search_fact_agent",
            "extract_file_fact_agent",
            "qa_check_agent",
            "synthesizing_agent",
        }
        assert str(agent_config["search_fact_agent"]["role"]).strip() == "{pack_title}外部搜索分析师"
        assert task_config["synthesize_and_output"]["output_file"] == "{pack_output_path}"
        assert task_config["search_facts"]["crew_name"] == crew_instance.crew_name
        assert task_config["search_facts"]["pack_name"] == crew_instance.pack_name
        assert task_config["search_facts"]["pack_title"] == crew_instance.pack_title
        assert task_config["search_facts"]["pack_focus"] == crew_instance.pack_focus
        assert task_config["search_facts"]["output_title"] == crew_instance.output_title
        assert task_config["search_facts"]["search_guidance"] == crew_instance.search_guidance
        assert task_config["extract_file_facts"]["extract_guidance"] == crew_instance.extract_guidance
        assert task_config["check_registry"]["qa_guidance"] == crew_instance.qa_guidance
        assert task_config["synthesize_and_output"]["synthesize_guidance"] == crew_instance.synthesize_guidance
        assert task_config["synthesize_and_output"]["output_skeleton"] == crew_instance.output_skeleton


def test_research_subcrew_can_build_runtime_crew_from_yaml_configs():
    """
    目的：确认独立 crew 文件可以直接构建 CrewAI runtime 对象。
    功能：验证 `BusinessCrew` 在新结构下能正常实例化 agents、tasks 和 crew。
    实现逻辑：构建一个代表性 sub-crew，并断言 process 类型与 agent/task 数量。
    可调参数：当前无。
    默认参数及原因：默认选 `BusinessCrew`，原因是它依赖链较短，足够覆盖主路径。
    """

    runtime_crew = BusinessCrew().crew()

    assert runtime_crew.process in {Process.sequential, "sequential"}
    assert len(runtime_crew.agents) == 4
    assert len(runtime_crew.tasks) == 6


def test_research_subcrew_runtime_uses_sequential_dispatch_without_manager():
    """
    目的：锁定 research sub-crew 已切换到顺序执行，不再依赖 manager agent 调度。
    功能：检查运行时 crew 不再带独立 manager agent，且任务归属仍然正确。
    实现逻辑：遍历 7 个 research sub-crew，分别构建 runtime crew 后断言调度边界和任务归属都正确。
    可调参数：当前无。
    默认参数及原因：默认覆盖全部 7 个 research sub-crew，原因是这条调度边界需要全局一致。
    """

    for crew_instance in _research_subcrew_instances():
        runtime_crew = crew_instance.crew()

        assert getattr(runtime_crew, "manager_agent", None) is None
        assert getattr(runtime_crew, "manager_llm", None) is None
        assert "文件提取分析师" in str(runtime_crew.tasks[0].agent.role)
        assert "外部搜索分析师" in str(runtime_crew.tasks[1].agent.role)
        assert "注册表检查员" in str(runtime_crew.tasks[4].agent.role)
        assert "综合分析师" in str(runtime_crew.tasks[5].agent.role)


def test_research_registry_template_is_deterministic_and_covers_all_subcrews():
    """
    目的：锁住 research registry 已从 planner 输出切换为固定模板初始化。
    功能：检查模板条目覆盖全部 7 个 research sub-crew，且不再出现 `planning_crew`。
    实现逻辑：直接加载模板条目，再按 owner_crew 和条目 ID 做集合断言。
    可调参数：当前无。
    默认参数及原因：默认使用测试公司名和行业名，原因是模板里包含占位符替换逻辑。
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


def test_research_subcrew_synthesize_prompts_enforce_registry_backfill_rules():
    """
    目的：锁定 7 个 research sub-crew 的综合任务都带有统一的 registry 回填约束。
    功能：检查 `synthesize_and_output` prompt 要求按 owner_crew 读取、使用 `update_entry` 回填，并避免重复建条。
    实现逻辑：遍历 7 个 sub-crew 的 `tasks.yaml`，逐个断言关键提示词存在。
    可调参数：当前无。
    默认参数及原因：默认只检验共用核心短语，原因是这些短语正是本次重构想稳定下来的行为边界。
    """

    for crew_instance in _research_subcrew_instances():
        module_dir = Path(inspect.getfile(crew_instance.__class__)).resolve().parent
        task_config_path = module_dir / "config" / "tasks.yaml"
        task_config = yaml.safe_load(task_config_path.read_text(encoding="utf-8"))
        description = task_config["synthesize_and_output"]["description"]

        assert 'owner_crew="{owner_crew}"' in description
        assert 'view="entry_detail"' in description
        assert "update_entry" in description
        assert "F_HIS_001" in description
        assert "不得新建与已有 entry 内容重复的条目" in description


def test_writeup_compile_prompt_is_non_destructive_preflight():
    """
        目的：锁定 writeup 阶段的 compile_report prompt 已退化为非破坏性确认，不再重新生成最终 Markdown。
        功能：检查 `writeup_crew/config/tasks.yaml` 中明确声明“不要重写正文”，且 compile task 不再配置 `output_file`。
        实现逻辑：直接读取 writeup crew 的 `tasks.yaml`，再断言 compile/export 两个任务都围绕既有最终 Markdown 路径工作。
        可调参数：当前无。
        默认参数及原因：默认只检查最关键的固定短语，原因是这能稳定覆盖行为边界，同时避免测试对整段 prompt 过度脆弱。
    """

    task_config_path = (
        PROJECT_ROOT
        / "src"
        / "automated_research_report_generator"
        / "crews"
        / "writeup_crew"
        / "config"
        / "tasks.yaml"
    )
    task_config = yaml.safe_load(task_config_path.read_text(encoding="utf-8"))
    compile_task = task_config["compile_report"]
    export_task = task_config["export_final_report"]
    description = compile_task["description"]

    assert "不要重新生成 Markdown 正文" in description
    assert "不要覆盖或改写 {final_report_markdown_path}" in description
    assert "registry snapshot" in description
    assert "output_file" not in compile_task
    assert compile_task["expected_output"] == "Markdown 已确认就绪，可直接导出 PDF。\n"
    assert "markdown_path={final_report_markdown_path}" in export_task["description"]


def test_research_subcrew_inputs_include_pack_metadata_and_upstream_pack_text(tmp_path):
    """
    目的：验证 flow 会把 crew YAML 占位符需要的输入补齐。
    功能：检查 peer_info sub-crew 输入同时包含 pack 元信息、责任 crew 和上游 pack 文本。
    实现逻辑：构造最小 flow 状态与临时 pack 文件后，直接调用 `_research_subcrew_inputs()` 断言结果。
    可调参数：`tmp_path`。
    默认参数及原因：默认选择 `peer_info_crew`，原因是它同时依赖 pack 元信息和两个上游 pack。
    """

    flow = ResearchReportFlow()
    flow.state.company_name = "Test Co"
    flow.state.industry = "Automation"
    flow.state.pdf_file_path = (tmp_path / "sample.pdf").as_posix()
    flow.state.page_index_file_path = (tmp_path / "page_index.json").as_posix()
    flow.state.document_metadata_file_path = (tmp_path / "document_metadata.md").as_posix()
    flow.state.run_cache_dir = (tmp_path / ".cache" / "test-run").as_posix()
    flow.state.run_output_dir = (tmp_path / ".cache" / "test-run").as_posix()
    flow.state.evidence_registry_path = (tmp_path / "registry.json").as_posix()
    flow.state.industry_pack_path = (tmp_path / "industry_pack.md").as_posix()
    flow.state.business_pack_path = (tmp_path / "business_pack.md").as_posix()

    for path, text in [
        (flow.state.pdf_file_path, "pdf placeholder"),
        (flow.state.page_index_file_path, "{}"),
        (flow.state.document_metadata_file_path, "metadata"),
        (flow.state.industry_pack_path, "industry pack body"),
        (flow.state.business_pack_path, "business pack body"),
    ]:
        Path(path).write_text(text, encoding="utf-8")

    initialize_registry("Test Co", "Automation", flow.state.evidence_registry_path)
    peer_info_crew = PeerInfoCrew()

    inputs = flow._research_subcrew_inputs(
        crew_instance=peer_info_crew,
        pack_name=peer_info_crew.pack_name,
        pack_title=peer_info_crew.pack_title,
        output_path=(tmp_path / "peer_info_pack.md").as_posix(),
        qa_feedback="补同行可比性限制。",
    )

    assert inputs["pack_title"] == "同行信息分析包"
    assert inputs["output_title"] == "同行信息分析包"
    assert inputs["owner_crew"] == peer_info_crew.crew_name
    assert inputs["search_guidance"] == peer_info_crew.search_guidance
    assert inputs["extract_guidance"] == peer_info_crew.extract_guidance
    assert inputs["qa_guidance"] == peer_info_crew.qa_guidance
    assert inputs["synthesize_guidance"] == peer_info_crew.synthesize_guidance
    assert inputs["output_skeleton"] == peer_info_crew.output_skeleton
    assert inputs["industry_pack_text"] == "industry pack body"
    assert inputs["business_pack_text"] == "business pack body"


def test_research_subcrew_base_module_is_removed():
    """
    目的：锁住共享 research sub-crew 基类文件已经被移除。
    功能：防止后续回退到共享 `research_subcrew_base.py` 设计。
    实现逻辑：直接断言旧文件路径不存在。
    可调参数：当前无。
    默认参数及原因：固定检查旧文件路径，原因是用户明确要求彻底删除该设计。
    """

    legacy_base_file = PROJECT_ROOT / "src" / "automated_research_report_generator" / "crews" / "research_subcrew_base.py"

    assert not legacy_base_file.exists()


# --- 以下为 prompt 精修后新增的断言 ---


_CREW_DOMAIN_KEYWORDS: dict[str, str] = {
    "history_background_crew": "治理",
    "industry_crew": "行业",
    "business_crew": "商业",
    "peer_info_crew": "可比",
    "financial_crew": "财务",
    "operating_metrics_crew": "运营",
    "risk_crew": "风险",
}

_CREW_SKELETON_HEADINGS: dict[str, list[str]] = {
    "history_background_crew": ["公司基本信息与定位", "发展时间线与关键里程碑", "股权结构与实际控制人", "董监高与治理结构"],
    "industry_crew": ["市场规模与增长驱动因素", "产业链结构分析", "竞争格局与主要参与者", "行业壁垒、替代品与进入门槛", "监管环境与政策导向"],
    "business_crew": ["产品与解决方案", "客户与需求场景", "收入模式与商业闭环", "供应链与交付能力", "技术与竞争优势"],
    "peer_info_crew": ["可比公司筛选标准与样本清单", "经营与产品可比性", "财务与运营指标对比", "估值倍数对比"],
    "financial_crew": ["核心财务数据总表", "收入与成本结构", "盈利质量与利润率趋势", "现金流与营运资本", "资产负债与资本结构"],
    "operating_metrics_crew": ["核心 KPI 定义与口径", "产能、产量与 ASP", "订单、交付与客户验证", "效率趋势及财务印证"],
    "risk_crew": ["风险总览矩阵", "经营与客户风险", "供应链与产能风险", "技术与治理风险", "政策与外部环境风险"],
}


def test_research_subcrew_agents_have_domain_persona_backstories():
    """
    目的：锁定 agents.yaml 的 backstory 包含领域关键词而非纯操作步骤。
    功能：检查 search_fact_agent 和 synthesizing_agent 的 backstory 包含该 crew 的领域关键词。
    """

    for crew_instance in _research_subcrew_instances():
        crew_name = getattr(crew_instance, "crew_name", "")
        keyword = _CREW_DOMAIN_KEYWORDS.get(crew_name, "")
        if not keyword:
            continue

        module_dir = Path(inspect.getfile(crew_instance.__class__)).resolve().parent
        agent_config = yaml.safe_load((module_dir / "config" / "agents.yaml").read_text(encoding="utf-8"))

        search_backstory = str(agent_config["search_fact_agent"]["backstory"])
        synth_backstory = str(agent_config["synthesizing_agent"]["backstory"])
        qa_backstory = str(agent_config["qa_check_agent"]["backstory"])

        assert keyword in search_backstory, f"{crew_name} search_fact_agent backstory 缺少领域关键词 '{keyword}'"
        assert keyword in synth_backstory, f"{crew_name} synthesizing_agent backstory 缺少领域关键词 '{keyword}'"
        assert keyword in qa_backstory, f"{crew_name} qa_check_agent backstory 缺少领域关键词 '{keyword}'"


def test_research_subcrew_task_placeholders_appear_at_most_once():
    """
    目的：防止 prompt 中出现多重注入。
    功能：检查每个 task description 中 {pack_focus}、{qa_feedback}、{owner_crew} 最多出现一次。
    """

    for crew_instance in _research_subcrew_instances():
        crew_name = getattr(crew_instance, "crew_name", "")
        module_dir = Path(inspect.getfile(crew_instance.__class__)).resolve().parent
        task_config = yaml.safe_load((module_dir / "config" / "tasks.yaml").read_text(encoding="utf-8"))

        for task_name, task_data in task_config.items():
            description = str(task_data.get("description", ""))
            for placeholder in ["{pack_focus}", "{qa_feedback}"]:
                count = description.count(placeholder)
                assert count <= 1, (
                    f"{crew_name}/{task_name} 中 {placeholder} 出现了 {count} 次，应最多 1 次"
                )
            owner_crew_count = description.count("{owner_crew}")
            assert owner_crew_count <= 2, (
                f"{crew_name}/{task_name} 中 {{owner_crew}} 出现了 {owner_crew_count} 次，应最多 2 次"
            )


def test_research_subcrew_synthesize_output_skeleton_has_fixed_headings():
    """
    目的：锁定 synthesize_and_output 的 expected_output 使用 {output_skeleton} 占位符。
    功能：检查每个 crew 的 output_skeleton 包含该 pack 的固定一级标题。
    """

    for crew_instance in _research_subcrew_instances():
        crew_name = getattr(crew_instance, "crew_name", "")
        skeleton = getattr(crew_instance, "output_skeleton", "")

        assert skeleton, f"{crew_name} 缺少 output_skeleton 属性"

        expected_headings = _CREW_SKELETON_HEADINGS.get(crew_name, [])
        for heading in expected_headings:
            assert heading in skeleton, f"{crew_name} output_skeleton 缺少标题 '{heading}'"


def test_research_subcrew_temperature_hierarchy():
    """
    目的：锁定温度分层：synthesis > search > extract，qa 保持最低。
    功能：构建 runtime crew 后检查各 agent 的 LLM 温度设置。
    """

    for crew_instance in _research_subcrew_instances():
        crew_name = getattr(crew_instance, "crew_name", "")
        runtime_crew = crew_instance.crew()

        agent_temps = {}
        for a in runtime_crew.agents:
            role = str(a.role).strip()
            if "综合分析师" in role:
                agent_temps["synthesis"] = a.llm.temperature
            elif "外部搜索分析师" in role:
                agent_temps["search"] = a.llm.temperature
            elif "文件提取分析师" in role:
                agent_temps["extract"] = a.llm.temperature
            elif "注册表检查员" in role:
                agent_temps["qa"] = a.llm.temperature

        assert agent_temps.get("qa", 0) <= 0.1, f"{crew_name} qa 温度应 <= 0.1"
        assert agent_temps.get("extract", 0) <= agent_temps.get("search", 0), (
            f"{crew_name} extract 温度应 <= search 温度"
        )
        assert agent_temps.get("search", 0) < agent_temps.get("synthesis", 0), (
            f"{crew_name} search 温度应 < synthesis 温度"
        )
