from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import automated_research_report_generator.flow.research_flow as research_flow_module
from automated_research_report_generator.flow.common import CREW_LOG_NAMES
from automated_research_report_generator.flow.document_metadata import PdfDocumentMetadataPayload
from automated_research_report_generator.flow.registry import (
    initialize_registry,
    load_registry,
    load_registry_template,
)
from automated_research_report_generator.flow.research_flow import (
    ResearchReportFlow,
    THESIS_STAGE_COMPLETED_NO_GATE_EVENT,
    VALUATION_STAGE_COMPLETED_NO_GATE_EVENT,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _build_flow(tmp_path) -> ResearchReportFlow:
    """
    目的：给 flow 测试提供一份最小可运行的 Flow 状态。
    功能：填充 run slug、输入路径、缓存路径和 registry 路径，避免测试依赖真实运行目录。
    实现逻辑：直接实例化 `ResearchReportFlow`，再把测试需要的状态字段写入临时目录路径。
    可调参数：`tmp_path` 由 pytest 提供，用于隔离测试过程中的临时路径。
    默认参数及原因：默认会初始化一份最小 registry，原因是新 flow 的 checkpoint 和 QA 都依赖真实账本。
    """

    flow = ResearchReportFlow()
    flow.state.run_slug = "test-run"
    flow.state.company_name = "Test Co"
    flow.state.industry = "Automation"
    flow.state.pdf_file_path = (tmp_path / "sample.pdf").as_posix()
    flow.state.page_index_file_path = (tmp_path / "page_index.json").as_posix()
    flow.state.document_metadata_file_path = (tmp_path / "document_metadata.md").as_posix()
    flow.state.run_cache_dir = (tmp_path / ".cache" / "test-run").as_posix()
    flow.state.run_output_dir = (tmp_path / ".cache" / "test-run").as_posix()
    flow.state.final_report_markdown_path = (tmp_path / ".cache" / "test-run" / "report.md").as_posix()
    flow.state.final_report_pdf_path = (tmp_path / ".cache" / "test-run" / "report.pdf").as_posix()
    Path(flow.state.pdf_file_path).write_text("pdf placeholder", encoding="utf-8")
    Path(flow.state.page_index_file_path).write_text("{}", encoding="utf-8")
    Path(flow.state.document_metadata_file_path).write_text("metadata", encoding="utf-8")
    registry_path = tmp_path / "registry.json"
    initialize_registry("Test Co", "Automation", registry_path)
    flow.state.evidence_registry_path = registry_path.as_posix()
    return flow


def test_prepare_evidence_generates_document_metadata_directly_in_run_indexing(tmp_path, monkeypatch):
    """
    目的：锁住 document metadata 首次落盘就写进当前 run 的 `indexing/` 目录。
    功能：验证 `prepare_evidence()` 会先拿到内存中的 metadata payload，再直接把 JSON 写入当前 run 的 `indexing/`。
    实现逻辑：替换 metadata payload 解析函数和其他预处理依赖后执行 `prepare_evidence()`，再断言 run 内生成结果和路径回写。
    可调参数：`tmp_path` 用于隔离临时路径，`monkeypatch` 用于替换真实预处理依赖。
    默认参数及原因：默认不创建公共 metadata 缓存，原因是这个修复的目标就是避免先写到 run 外路径。
    """

    flow = ResearchReportFlow()
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_text("pdf placeholder", encoding="utf-8")
    flow.state.pdf_file_path = pdf_path.as_posix()

    run_root_dir = tmp_path / ".cache" / "20260409_测试公司"
    cache_dir = run_root_dir / "md"
    log_dir = run_root_dir / "logs"
    captured_manifest: dict[str, str] = {}

    def fake_resolve_pdf_document_metadata_payload(pdf_file_path: str) -> PdfDocumentMetadataPayload:
        """
        目的：替换真实 metadata 识别，避免测试触发模型调用和 run 外落盘。
        功能：返回一份尚未落盘的固定 metadata payload。
        实现逻辑：直接构造 `PdfDocumentMetadataPayload` 并返回。
        可调参数：`pdf_file_path`。
        默认参数及原因：默认忽略 PDF 内容，原因是这个测试只关心 metadata 的落盘位置。
        """

        return PdfDocumentMetadataPayload(
            pdf_file_path=pdf_file_path,
            generated_at="2026-04-09T13:11:28+08:00",
            fingerprint="fake-fingerprint",
            company_name="测试公司",
            industry="电气设备",
            source_pages=[1, 2, 3],
        )

    def fake_build_run_directories(company_name: str) -> dict[str, Path]:
        """
        目的：给 `prepare_evidence()` 提供稳定的临时 run 目录。
        功能：返回测试专用的 run 根目录、产物目录和日志目录。
        实现逻辑：先创建所需目录，再按真实接口结构返回路径映射。
        可调参数：`company_name`。
        默认参数及原因：默认 run slug 固定，原因是测试需要稳定断言最终路径。
        """

        cache_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        return {
            "run_slug": Path("20260409_测试公司"),
            "run_root_dir": run_root_dir,
            "cache_dir": cache_dir,
            "log_dir": log_dir,
        }

    def fake_ensure_pdf_page_index(pdf_file_path: str, company_name: str) -> str:
        """
        目的：替换真实页索引生成逻辑，避免测试触发大模型和并发预处理。
        功能：在 run 内 `indexing/` 写入一份最小页索引文件，并返回其路径。
        实现逻辑：直接创建固定文件名的 JSON 文件。
        可调参数：`pdf_file_path` 和 `company_name`。
        默认参数及原因：默认写空 JSON，原因是这里不关心页索引内容本身。
        """

        index_path = (run_root_dir / "indexing" / "sample_page_index.json").resolve()
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text("{}", encoding="utf-8")
        return index_path.as_posix()

    def fake_write_run_debug_manifest(**kwargs) -> str:
        """
        目的：拦截 manifest 落盘参数，避免测试写入项目真实缓存目录。
        功能：记录 `prepare_evidence()` 传给 manifest 的关键路径。
        实现逻辑：把传入参数更新到外层捕获字典，再返回一个临时 manifest 路径。
        可调参数：`kwargs`。
        默认参数及原因：默认只记录参数不生成真实 manifest 内容，原因是这里关注的是路径值是否正确。
        """

        for key, value in kwargs.items():
            captured_manifest[key] = value
        manifest_path = (cache_dir / "run_manifest.json").resolve()
        return manifest_path.as_posix()

    monkeypatch.setattr(
        research_flow_module,
        "resolve_pdf_document_metadata_payload",
        fake_resolve_pdf_document_metadata_payload,
    )
    monkeypatch.setattr(research_flow_module, "build_run_directories", fake_build_run_directories)
    monkeypatch.setattr(research_flow_module, "activate_run_preprocess_log", lambda run_slug: None)
    monkeypatch.setattr(research_flow_module, "ensure_pdf_page_index", fake_ensure_pdf_page_index)
    monkeypatch.setattr(research_flow_module, "set_pdf_context", lambda pdf_file_path, page_index_path: None)
    monkeypatch.setattr(
        research_flow_module,
        "initialize_registry",
        lambda company_name, industry, registry_path, periods=None: None,
    )
    monkeypatch.setattr(
        research_flow_module,
        "set_evidence_registry_context",
        lambda registry_path: None,
    )
    monkeypatch.setattr(research_flow_module, "write_run_debug_manifest", fake_write_run_debug_manifest)
    monkeypatch.setattr(flow, "_write_checkpoint", lambda checkpoint_code, payload: "checkpoint.json")
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    flow.prepare_evidence()

    run_metadata_path = (run_root_dir / "indexing" / "sample_document_metadata.json").resolve()
    public_metadata_path = tmp_path / ".cache" / "pdf_page_indexes" / "sample_document_metadata.json"
    saved_payload = json.loads(run_metadata_path.read_text(encoding="utf-8"))

    assert run_metadata_path.exists()
    assert not public_metadata_path.exists()
    assert saved_payload["company_name"] == "测试公司"
    assert saved_payload["industry"] == "电气设备"
    assert saved_payload["source_pages"] == [1, 2, 3]
    assert flow.state.document_metadata_file_path == run_metadata_path.as_posix()
    assert captured_manifest["document_metadata_file_path"] == run_metadata_path.as_posix()


def test_build_research_plan_reinitializes_registry_from_deterministic_template(tmp_path, monkeypatch):
    """
    目的：验证 `build_research_plan()` 已切到固定模板初始化，而不是 planner 动态出题。
    功能：检查 registry 会按模板重建，并把条目数和 owner 分布写入 checkpoint。
    实现逻辑：构造最小 flow，替换日志和 checkpoint 落盘后执行 `build_research_plan()`，再回读 registry 断言。
    可调参数：`tmp_path` 和 `monkeypatch`。
    默认参数及原因：默认直接使用真实模板加载函数，原因是这个测试要锁住模板驱动的真实行为。
    """

    flow = _build_flow(tmp_path)
    captured_checkpoint: dict[str, object] = {}

    def fake_write_checkpoint(checkpoint_code: str, payload: dict[str, object]) -> str:
        """
        目的：拦截 checkpoint 输出，避免测试写入额外文件。
        功能：记录 `build_research_plan()` 写出的关键字段，供测试断言。
        实现逻辑：把 payload 写入外层字典，再返回一个占位路径。
        可调参数：`checkpoint_code` 和 `payload`。
        默认参数及原因：默认只记录最近一次 planning checkpoint，原因是本测试只关注 `cp01_planned`。
        """

        captured_checkpoint["code"] = checkpoint_code
        captured_checkpoint["payload"] = payload
        return "checkpoint.json"

    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")
    monkeypatch.setattr(flow, "_write_checkpoint", fake_write_checkpoint)

    result = flow.build_research_plan()

    template_entries = load_registry_template("Test Co", "Automation")
    snapshot = load_registry(flow.state.evidence_registry_path)

    assert result == flow.state.evidence_registry_path
    assert len(snapshot.entries) == len(template_entries)
    assert captured_checkpoint["code"] == "cp01_planned"
    assert captured_checkpoint["payload"]["entry_count"] == len(template_entries)
    assert captured_checkpoint["payload"]["owner_distribution"]["industry_crew"] > 0
    assert "planning_crew" not in captured_checkpoint["payload"]["owner_distribution"]


def test_run_research_stage_writes_internal_review_summary_and_checkpoint(tmp_path, monkeypatch):
    """
    目的：验证 research 阶段会提取各 sub-crew 的 `check_registry` 输出并生成内部校验摘要。
    功能：检查 pack 输出路径、摘要文件 `08_research_internal_registry_checks.md` 和 `cp03_research_internal_checks` 同步写出。
    实现逻辑：替换 `RESEARCH_SUB_CREW_SPECS` 为假 crew 列表，构造带 `tasks_output` 的假结果后执行 `_run_research_stage()` 断言状态与文件内容。
    可调参数：`tmp_path` 用于隔离临时路径，`monkeypatch` 用于替换真实 crew。
    默认参数及原因：默认让一个 pack 缺失 `check_registry` 输出，原因是要同时锁住正常汇总和缺口占位行为。
    """

    flow = _build_flow(tmp_path)
    captured_runs: list[dict[str, str]] = []
    captured_checkpoints: list[tuple[str, dict[str, object]]] = []

    class FakeSubCrew:
        """
        目的：替换真实 sub-crew，避免测试触发模型调用。
        功能：记录 kickoff 输入，并返回带固定 `tasks_output` 的假结果。
        实现逻辑：按照当前 6-task 链路构造 6 个输出对象，其中第 5 个对应 `check_registry`。
        可调参数：无。
        默认参数及原因：默认只返回最小必要任务输出，原因是本测试关注的是编排与摘要提取。
        """

        output_log_file_path = None
        crew_name = "fake_crew"
        pack_title = "假包"
        pack_focus = ""
        output_title = "假包"
        search_guidance = ""
        extract_guidance = ""
        qa_guidance = ""
        synthesize_guidance = ""
        output_skeleton = ""

        def crew(self):
            class FakeRunner:
                def kickoff(self, inputs):
                    captured_runs.append(inputs.copy())
                    pack_name = inputs["pack_name"]
                    if pack_name == "business_pack":
                        task_outputs = []
                    else:
                        task_outputs = [
                            SimpleNamespace(name="extract_file_facts", raw="extract"),
                            SimpleNamespace(name="record_extract_registry", raw="record extract"),
                            SimpleNamespace(name="search_facts", raw="search"),
                            SimpleNamespace(name="record_search_registry", raw="record search"),
                            SimpleNamespace(name="check_registry", raw=f"{pack_name} check memo"),
                            SimpleNamespace(name="synthesize_and_output", raw="synth"),
                        ]
                    return SimpleNamespace(tasks_output=task_outputs)

            return FakeRunner()

    fake_specs = [
        {
            "pack_name": "history_background_pack",
            "crew_name": "history_background_crew",
            "crew_cls": FakeSubCrew,
            "output_file_name": "01_history_background_pack.md",
            "state_attr": "history_background_pack_path",
            "title": "历史与背景分析包",
            "checkpoint_code": "cp02a_history_background_pack",
        },
        {
            "pack_name": "industry_pack",
            "crew_name": "industry_crew",
            "crew_cls": FakeSubCrew,
            "output_file_name": "02_industry_pack.md",
            "state_attr": "industry_pack_path",
            "title": "行业分析包",
            "checkpoint_code": "cp02b_industry_pack",
        },
        {
            "pack_name": "business_pack",
            "crew_name": "business_crew",
            "crew_cls": FakeSubCrew,
            "output_file_name": "03_business_pack.md",
            "state_attr": "business_pack_path",
            "title": "业务分析包",
            "checkpoint_code": "cp02c_business_pack",
        },
    ]

    def fake_write_checkpoint(checkpoint_code: str, payload: dict[str, object]) -> str:
        """
        目的：记录 research 阶段写出的 checkpoint 内容。
        功能：把 checkpoint 代号和 payload 收集到列表，供测试断言。
        实现逻辑：直接追加到外层列表并返回占位路径。
        可调参数：`checkpoint_code` 和 `payload`。
        默认参数及原因：默认不落真实 checkpoint 文件，原因是这里只关注字段值本身。
        """

        captured_checkpoints.append((checkpoint_code, payload))
        return "checkpoint.json"

    monkeypatch.setattr(research_flow_module, "RESEARCH_SUB_CREW_SPECS", fake_specs)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_checkpoint", fake_write_checkpoint)
    monkeypatch.setattr(flow, "_register_pack_output", lambda path, pack_name, title: None)
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    result = flow._run_research_stage("initial")

    research_dir = (Path(flow.state.run_cache_dir) / "research" / "iter_01").resolve()
    summary_path = research_dir / "08_research_internal_registry_checks.md"
    summary_text = summary_path.read_text(encoding="utf-8")

    assert [run["pack_name"] for run in captured_runs] == [
        "history_background_pack",
        "industry_pack",
        "business_pack",
    ]
    assert captured_runs[0]["pack_output_path"] == f"{research_dir.as_posix()}/01_history_background_pack.md"
    assert flow.state.history_background_pack_path == f"{research_dir.as_posix()}/01_history_background_pack.md"
    assert flow.state.business_pack_path == f"{research_dir.as_posix()}/03_business_pack.md"
    assert result == summary_path.as_posix()
    assert flow.state.research_internal_review_summary_path == summary_path.as_posix()
    assert "# Research 内部校验摘要" in summary_text
    assert "history_background_pack check memo" in summary_text
    assert "industry_pack check memo" in summary_text
    assert "本 pack 未返回 `check_registry` 输出。" in summary_text
    assert captured_checkpoints[-1][0] == "cp03_research_internal_checks"
    assert captured_checkpoints[-1][1]["summary_path"] == summary_path.as_posix()
    assert captured_checkpoints[-1][1]["covered_packs"] == [
        "history_background_pack",
        "industry_pack",
        "business_pack",
    ]
    assert captured_checkpoints[-1][1]["missing_packs"] == ["business_pack"]


def test_publish_if_passed_uses_internal_review_summary_file(tmp_path, monkeypatch):
    """
    目的：验证 writeup 输入的 `final_qa_summary` 已改为 research 内部校验摘要文件内容。
    功能：检查 `publish_if_passed()` 不再读取旧外部 QA 汇总，而是直接注入摘要 Markdown 正文。
    实现逻辑：构造最小 flow 和假 writeup crew，写入摘要文件后执行 `publish_if_passed()` 断言输入。
    可调参数：`tmp_path` 用于隔离临时文件，`monkeypatch` 用于替换 writeup crew。
    默认参数及原因：默认只校验关键输入，不关心最终 Markdown/PDF 的真实生成。
    """

    flow = _build_flow(tmp_path)
    summary_path = tmp_path / "08_research_internal_registry_checks.md"
    summary_text = "# Research 内部校验摘要\n\n来自 check_registry 的汇总。"
    summary_path.write_text(summary_text, encoding="utf-8")
    flow.state.research_internal_review_summary_path = summary_path.as_posix()
    captured_inputs: dict[str, str] = {}

    class FakeWriteupCrew:
        """
        目的：替换真实 writeup crew，避免测试触发模型调用和 PDF 导出。
        功能：记录 kickoff 输入，供测试断言。
        实现逻辑：提供与真实 crew 兼容的 `crew().kickoff()` 接口。
        可调参数：无。
        默认参数及原因：默认只记录输入不生成产物，原因是这里只验证 publish 接线。
        """

        output_log_file_path = None

        def crew(self):
            class FakeRunner:
                def kickoff(self, inputs):
                    captured_inputs.update(inputs)

            return FakeRunner()

    monkeypatch.setattr(research_flow_module, "WriteupCrew", FakeWriteupCrew)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_manifest_from_state", lambda status: "manifest.json")
    monkeypatch.setattr(flow, "_write_checkpoint", lambda checkpoint_code, payload: "checkpoint.json")
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    flow.publish_if_passed()

    assert captured_inputs["final_qa_summary"] == summary_text


def test_publish_if_passed_uses_placeholder_when_internal_review_summary_missing(tmp_path, monkeypatch):
    """
    目的：验证 research 内部校验摘要缺失时，writeup 仍会收到明确占位文本。
    功能：检查 `final_qa_summary` 不会留空，而是写入“本轮未生成内部校验摘要”占位。
    实现逻辑：不设置摘要文件路径，替换 writeup crew 后执行 `publish_if_passed()` 断言输入。
    可调参数：`tmp_path` 用于隔离临时文件，`monkeypatch` 用于替换 writeup crew。
    默认参数及原因：默认只验证占位回退行为，原因是这是新设计的重要兜底语义。
    """

    flow = _build_flow(tmp_path)
    captured_inputs: dict[str, str] = {}

    class FakeWriteupCrew:
        """
        目的：替换真实 writeup crew，避免测试触发模型调用和 PDF 导出。
        功能：记录 kickoff 输入，供测试断言。
        实现逻辑：提供与真实 crew 兼容的 `crew().kickoff()` 接口。
        可调参数：无。
        默认参数及原因：默认只记录输入不生成产物，原因是这里只验证 publish 接线。
        """

        output_log_file_path = None

        def crew(self):
            class FakeRunner:
                def kickoff(self, inputs):
                    captured_inputs.update(inputs)

            return FakeRunner()

    monkeypatch.setattr(research_flow_module, "WriteupCrew", FakeWriteupCrew)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_manifest_from_state", lambda status: "manifest.json")
    monkeypatch.setattr(flow, "_write_checkpoint", lambda checkpoint_code, payload: "checkpoint.json")
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    flow.publish_if_passed()

    assert captured_inputs["final_qa_summary"] == "# Research 内部校验摘要\n\n本轮未生成内部校验摘要。\n"


def test_run_valuation_stage_uses_peer_info_and_operating_metrics_inputs(tmp_path, monkeypatch):
    """
    目的：验证 valuation 阶段会接收新的 `peer_info_pack_text` 和 `operating_metrics_pack_text` 输入。
    功能：检查 `_run_valuation_stage()` 传给 valuation crew 的输入不再依赖 business pack。
    实现逻辑：构造最小上游 pack，替换 valuation crew 为假对象后直接调用 `_run_valuation_stage()` 断言输入。
    可调参数：`tmp_path` 用于隔离临时文件，`monkeypatch` 用于替换 valuation crew。
    默认参数及原因：默认只校验关键输入，原因是这个测试关注的是新设计的接线变化。
    """

    flow = _build_flow(tmp_path)
    flow.state.peer_info_pack_path = (tmp_path / "peer_info.md").as_posix()
    flow.state.finance_pack_path = (tmp_path / "finance.md").as_posix()
    flow.state.operating_metrics_pack_path = (tmp_path / "metrics.md").as_posix()
    flow.state.risk_pack_path = (tmp_path / "risk.md").as_posix()
    for path, text in [
        (flow.state.peer_info_pack_path, "peer info body"),
        (flow.state.finance_pack_path, "finance body"),
        (flow.state.operating_metrics_pack_path, "metrics body"),
        (flow.state.risk_pack_path, "risk body"),
    ]:
        Path(path).write_text(text, encoding="utf-8")

    captured_inputs: dict[str, str] = {}

    class FakeValuationCrew:
        output_log_file_path = None

        def crew(self):
            class FakeRunner:
                def kickoff(self, inputs):
                    captured_inputs.update(inputs)

            return FakeRunner()

    monkeypatch.setattr(research_flow_module, "ValuationCrew", FakeValuationCrew)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_register_pack_output", lambda path, pack_name, title: None)
    monkeypatch.setattr(flow, "_write_checkpoint", lambda checkpoint_code, payload: "checkpoint.json")
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    flow._run_valuation_stage("initial")

    assert captured_inputs["peer_info_pack_text"] == "peer info body"
    assert captured_inputs["finance_pack_text"] == "finance body"
    assert captured_inputs["operating_metrics_pack_text"] == "metrics body"
    assert captured_inputs["risk_pack_text"] == "risk body"


def test_run_valuation_crew_returns_no_gate_event(tmp_path, monkeypatch):
    """
    目的：验证新链路下估值完成后会直接进入 thesis，不再走外部 valuation gate。
    功能：检查 `run_valuation_crew()` 返回新的无 gate 事件标签。
    实现逻辑：替换 `_run_valuation_stage()` 后直接调用 `run_valuation_crew()` 断言返回值。
    可调参数：`tmp_path` 用于构造最小 Flow 状态，`monkeypatch` 用于替换阶段执行函数。
    默认参数及原因：默认不跑真实 crew，原因是这里只验证 flow 路由标签。
    """

    flow = _build_flow(tmp_path)
    monkeypatch.setattr(flow, "_run_valuation_stage", lambda loop_reason: "valuation.md")

    assert flow.run_valuation_crew() == VALUATION_STAGE_COMPLETED_NO_GATE_EVENT


def test_run_investment_thesis_crew_routes_with_no_gate_event(tmp_path, monkeypatch):
    """
    目的：验证 thesis 阶段会把无 gate 事件作为路由标签发给 writeup。
    功能：同时检查 `run_investment_thesis_crew()` 的装饰器类型和返回事件标签。
    实现逻辑：替换 `_run_thesis_stage()` 后直接调用该方法，并断言其已注册为 router。
    可调参数：`tmp_path` 用于构造最小 Flow 状态，`monkeypatch` 用于替换阶段执行函数。
    默认参数及原因：默认不跑真实 crew，原因是这里只验证 Flow 事件分发边界。
    """

    flow = _build_flow(tmp_path)
    monkeypatch.setattr(flow, "_run_thesis_stage", lambda: "thesis.md")

    assert getattr(ResearchReportFlow.run_investment_thesis_crew, "__is_router__", False) is True
    assert flow.run_investment_thesis_crew() == THESIS_STAGE_COMPLETED_NO_GATE_EVENT


def test_external_research_qa_cleanup_is_complete():
    """
    目的：锁住 research 外部 QA gate 的关键接线和日志位点已经被彻底清理。
    功能：检查 `qa_research` 日志槽位、旧 qa_crew 文件和旧路由方法都不再存在。
    实现逻辑：直接读取当前模块常量、文件路径和类属性做静态断言。
    可调参数：当前无。
    默认参数及原因：固定检查最关键的死代码入口，原因是这能防止旧 gate 骨架被悄悄带回。
    """

    legacy_qa_crew_file = (
        PROJECT_ROOT
        / "src"
        / "automated_research_report_generator"
        / "crews"
        / "qa_crew"
        / "qa_crew.py"
    )

    assert "qa_research" not in CREW_LOG_NAMES
    assert not legacy_qa_crew_file.exists()
    assert not hasattr(ResearchReportFlow, "review_research_gate")
    assert not hasattr(ResearchReportFlow, "route_research_gate")
    assert not hasattr(ResearchReportFlow, "rerun_research")
