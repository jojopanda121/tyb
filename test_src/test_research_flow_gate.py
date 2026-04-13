from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import automated_research_report_generator.flow.research_flow as research_flow_module
import pytest
import yaml
from automated_research_report_generator.flow.common import CREW_LOG_NAMES
from automated_research_report_generator.flow.document_metadata import PdfDocumentMetadataPayload
from automated_research_report_generator.flow.models import ResearchRegistryCheckIssue, ResearchRegistryCheckResult
from automated_research_report_generator.flow.registry import (
    initialize_registry,
    load_registry,
    load_registry_template,
)
from automated_research_report_generator.flow.research_flow import (
    RESEARCH_STAGE_COMPLETED_EVENT,
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
    flow.state.registry_snapshot_markdown_path = registry_path.with_name("registry_snapshot.md").as_posix()
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
                    elif pack_name == "history_background_pack":
                        task_outputs = [
                            SimpleNamespace(name="extract_file_facts", raw="extract"),
                            SimpleNamespace(name="record_extract_registry", raw="record extract"),
                            SimpleNamespace(name="search_facts", raw="search"),
                            SimpleNamespace(name="record_search_registry", raw="record search"),
                            SimpleNamespace(
                                name="check_registry",
                                raw="",
                                pydantic=ResearchRegistryCheckResult(
                                    pack_name=pack_name,
                                    overall_status="ready",
                                    issues=[],
                                    revision_suggestions=[],
                                    recommended_rework_stage="none",
                                    summary="history_background_pack structured summary",
                                ),
                            ),
                            SimpleNamespace(name="synthesize_and_output", raw="synth"),
                        ]
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

    result = flow._run_research_stage()

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
    assert "history_background_pack structured summary" in summary_text
    assert "- 整体就绪状态：`ready`" in summary_text
    assert "- 建议回退阶段：`none`" in summary_text
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


def test_parse_research_review_status_recognizes_current_ready_variants(tmp_path):
    """
    目的：锁定 research readiness 解析器能识别本轮修复明确支持的状态写法。
    功能：验证 `Ready`、`**Ready**`、`Ready (就绪)` 和 `Not Ready` 都会被稳定归类。
    实现逻辑：构造最小 flow 后直接调用 `_parse_research_review_status()`，逐个断言返回值。
    可调参数：`tmp_path` 仅用于复用最小 flow 夹具。
    默认参数及原因：默认覆盖当前日志里已出现的几种固定写法，原因是本轮不引入宽泛文本分类。
    """

    flow = _build_flow(tmp_path)

    assert flow._parse_research_review_status("整体就绪状态：Ready") == "ready"
    assert flow._parse_research_review_status("Ready") == "ready"
    assert flow._parse_research_review_status("**Ready**") == "ready"
    assert flow._parse_research_review_status("Ready (就绪)") == "ready"
    assert flow._parse_research_review_status("Not Ready") == "not_ready"
    assert flow._parse_research_review_status("Pending manual review") == "unknown"


def test_run_research_crew_keeps_downstream_open_when_any_pack_is_not_ready(tmp_path, monkeypatch):
    """
    目的：锁定 research 内部 `not_ready` 结果只作为 advisory 摘要，不再阻断后续阶段。
    功能：验证 `run_research_crew()` 仍返回 research 完成事件，且不会写 `blocked` 状态或阻断 checkpoint。
    实现逻辑：用假 sub-crew 产出结构化 `ResearchRegistryCheckResult`，再执行 research 主入口并断言摘要、状态和下游接线。
    可调参数：`tmp_path` 和 `monkeypatch`。
    默认参数及原因：默认只模拟两个 pack，原因是这里关注的是 advisory 语义而不是完整 7 crew 编排。
    """

    flow = _build_flow(tmp_path)
    captured_manifests: list[dict[str, object]] = []
    captured_checkpoints: list[tuple[str, dict[str, object]]] = []

    class FakeSubCrew:
        """
        目的：为 advisory 场景提供可控的 research sub-crew 假实现。
        功能：按 pack 名返回固定结构化 `check_registry` 结果，避免测试触发真实模型调用。
        实现逻辑：复用当前 6-task 输出顺序，只改 `check_registry` 的 `pydantic` 结果。
        可调参数：无。
        默认参数及原因：默认让 `business_pack` 返回 `not_ready`，原因是这样最容易验证 advisory 链路。
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
                    pack_name = inputs["pack_name"]
                    overall_status = "not_ready" if pack_name == "business_pack" else "ready"
                    issues = (
                        [
                            ResearchRegistryCheckIssue(
                                entry_id="B_TEST_001",
                                issue_type="missing_content",
                                detail="关键条目仍缺少可引用证据。",
                            )
                        ]
                        if pack_name == "business_pack"
                        else []
                    )
                    return SimpleNamespace(
                        tasks_output=[
                            SimpleNamespace(name="extract_file_facts", raw="extract"),
                            SimpleNamespace(name="record_extract_registry", raw="record extract"),
                            SimpleNamespace(name="search_facts", raw="search"),
                            SimpleNamespace(name="record_search_registry", raw="record search"),
                            SimpleNamespace(
                                name="check_registry",
                                raw="",
                                pydantic=ResearchRegistryCheckResult(
                                    pack_name=pack_name,
                                    overall_status=overall_status,
                                    issues=issues,
                                    revision_suggestions=["补齐核心条目的外部证据。"] if issues else [],
                                    recommended_rework_stage="search" if issues else "none",
                                    summary=f"{pack_name} structured review",
                                ),
                            ),
                            SimpleNamespace(name="synthesize_and_output", raw="synth"),
                        ]
                    )

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
            "pack_name": "business_pack",
            "crew_name": "business_crew",
            "crew_cls": FakeSubCrew,
            "output_file_name": "03_business_pack.md",
            "state_attr": "business_pack_path",
            "title": "业务分析包",
            "checkpoint_code": "cp02c_business_pack",
        },
    ]

    def fake_write_run_debug_manifest(**kwargs) -> str:
        """
        目的：拦截 advisory 场景下可能出现的 manifest 写入参数。
        功能：记录状态字段，避免测试写入项目真实缓存目录。
        实现逻辑：把 manifest 参数追加到外层列表，再返回一个临时路径。
        可调参数：`kwargs`。
        默认参数及原因：默认只记录最后一次状态，原因是本测试只关心是否误写 `blocked`。
        """

        captured_manifests.append(dict(kwargs))
        return (tmp_path / "run_manifest.json").as_posix()

    def fake_write_checkpoint(checkpoint_code: str, payload: dict[str, object]) -> str:
        """
        目的：收集 research advisory 场景写出的 checkpoint。
        功能：记录 checkpoint 代号和关键载荷，供测试断言。
        实现逻辑：把参数追加到外层列表，再返回占位路径。
        可调参数：`checkpoint_code` 和 `payload`。
        默认参数及原因：默认不落真实 checkpoint 文件，原因是这里只关心状态语义。
        """

        captured_checkpoints.append((checkpoint_code, payload))
        return "checkpoint.json"

    monkeypatch.setattr(research_flow_module, "RESEARCH_SUB_CREW_SPECS", fake_specs)
    monkeypatch.setattr(research_flow_module, "write_run_debug_manifest", fake_write_run_debug_manifest)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_checkpoint", fake_write_checkpoint)
    monkeypatch.setattr(flow, "_register_pack_output", lambda path, pack_name, title: None)
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")
    captured_valuation_calls: list[str] = []
    monkeypatch.setattr(flow, "_run_valuation_stage", lambda: captured_valuation_calls.append("valuation"))

    result = flow.run_research_crew()
    summary_text = Path(flow.state.research_internal_review_summary_path).read_text(encoding="utf-8")

    assert result == RESEARCH_STAGE_COMPLETED_EVENT
    assert flow.state.blocked_packs == []
    assert flow.state.block_reason == ""
    assert "整体就绪状态：`not_ready`" in summary_text
    assert "business_pack structured review" in summary_text
    assert captured_manifests == []
    assert captured_checkpoints[-1][0] == "cp03_research_internal_checks"
    assert all(code != "cp03_research_blocked" for code, _ in captured_checkpoints)
    assert flow.run_valuation_crew() == VALUATION_STAGE_COMPLETED_NO_GATE_EVENT
    assert captured_valuation_calls == ["valuation"]


def test_run_research_crew_falls_back_to_raw_memo_when_structured_check_output_missing(tmp_path, monkeypatch):
    """
    目的：锁定 `check_registry` 缺少结构化结果时仍会降级到 raw memo 解析。
    功能：验证 fail-open 降级路径下，`Not Ready` 仍只会进入内部校验摘要，不会阻断 research 后续阶段。
    实现逻辑：让 fake sub-crew 只返回 raw memo、不返回 `pydantic`，再执行 research 主入口并断言状态与摘要。
    可调参数：`tmp_path` 和 `monkeypatch`。
    默认参数及原因：默认只模拟两个 pack，原因是这里关注的是降级判定而不是完整 7 crew 编排。
    """

    flow = _build_flow(tmp_path)
    captured_manifests: list[dict[str, object]] = []
    captured_checkpoints: list[tuple[str, dict[str, object]]] = []

    class FakeSubCrew:
        """
        目的：为 raw memo 降级路径提供可控的 research sub-crew 假实现。
        功能：按 pack 名返回固定 `check_registry` 原始文本，避免测试触发真实模型调用。
        实现逻辑：复用当前 6-task 输出顺序，只设置 `check_registry.raw`，不提供结构化结果。
        可调参数：无。
        默认参数及原因：默认让 `business_pack` 返回 `Not Ready`，原因是这样能直接验证 regex 降级路径。
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
                    pack_name = inputs["pack_name"]
                    memo = "Not Ready" if pack_name == "business_pack" else "Ready"
                    return SimpleNamespace(
                        tasks_output=[
                            SimpleNamespace(name="extract_file_facts", raw="extract"),
                            SimpleNamespace(name="record_extract_registry", raw="record extract"),
                            SimpleNamespace(name="search_facts", raw="search"),
                            SimpleNamespace(name="record_search_registry", raw="record search"),
                            SimpleNamespace(name="check_registry", raw=memo, pydantic=None),
                            SimpleNamespace(name="synthesize_and_output", raw="synth"),
                        ]
                    )

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
            "pack_name": "business_pack",
            "crew_name": "business_crew",
            "crew_cls": FakeSubCrew,
            "output_file_name": "03_business_pack.md",
            "state_attr": "business_pack_path",
            "title": "业务分析包",
            "checkpoint_code": "cp02c_business_pack",
        },
    ]

    def fake_write_run_debug_manifest(**kwargs) -> str:
        """
        目的：拦截 raw memo 降级路径下可能出现的 manifest 写入参数。
        功能：记录状态字段，避免测试写入项目真实缓存目录。
        实现逻辑：把 manifest 参数追加到外层列表，再返回一个临时路径。
        可调参数：`kwargs`。
        默认参数及原因：默认只记录最后一次状态，原因是本测试只关心是否误写 `blocked`。
        """

        captured_manifests.append(dict(kwargs))
        return (tmp_path / "run_manifest.json").as_posix()

    def fake_write_checkpoint(checkpoint_code: str, payload: dict[str, object]) -> str:
        """
        目的：收集 raw memo 降级路径写出的 checkpoint。
        功能：记录 checkpoint 代号和关键载荷，供测试断言。
        实现逻辑：把参数追加到外层列表，再返回占位路径。
        可调参数：`checkpoint_code` 和 `payload`。
        默认参数及原因：默认不落真实 checkpoint 文件，原因是这里只关心状态语义。
        """

        captured_checkpoints.append((checkpoint_code, payload))
        return "checkpoint.json"

    monkeypatch.setattr(research_flow_module, "RESEARCH_SUB_CREW_SPECS", fake_specs)
    monkeypatch.setattr(research_flow_module, "write_run_debug_manifest", fake_write_run_debug_manifest)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_checkpoint", fake_write_checkpoint)
    monkeypatch.setattr(flow, "_register_pack_output", lambda path, pack_name, title: None)
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    result = flow.run_research_crew()
    summary_text = Path(flow.state.research_internal_review_summary_path).read_text(encoding="utf-8")

    assert result == RESEARCH_STAGE_COMPLETED_EVENT
    assert flow.state.blocked_packs == []
    assert flow.state.block_reason == ""
    assert "Not Ready" in summary_text
    assert captured_manifests == []
    assert captured_checkpoints[-1][0] == "cp03_research_internal_checks"
    assert all(code != "cp03_research_blocked" for code, _ in captured_checkpoints)


def test_run_research_stage_failure_writes_failed_manifest_and_checkpoint(tmp_path, monkeypatch):
    """
    目的：锁定 research 子 crew 抛异常时，run 会被显式标记为 `failed` 而不是停留在旧状态。
    功能：验证失败阶段、失败 crew、错误信息、manifest 和 failure checkpoint 都会被同步写出。
    实现逻辑：让首个 fake sub-crew 在 kickoff 时直接抛异常，再断言 state 与落盘参数。
    可调参数：`tmp_path` 和 `monkeypatch`。
    默认参数及原因：默认只模拟单个失败 crew，原因是本测试关注的是异常边界记录而不是多 crew 编排。
    """

    flow = _build_flow(tmp_path)
    captured_manifests: list[dict[str, object]] = []
    captured_checkpoints: list[tuple[str, dict[str, object]]] = []

    class ExplodingSubCrew:
        """
        目的：模拟 research crew kickoff 直接抛异常的失败边界。
        功能：在进入真实模型前就抛出固定异常，方便测试 failure 记录逻辑。
        实现逻辑：提供与真实 crew 兼容的 `crew().kickoff()` 接口，并在调用时抛错。
        可调参数：无。
        默认参数及原因：异常文案固定为 `boom`，原因是便于做最小断言。
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
                    raise RuntimeError("boom")

            return FakeRunner()

    fake_specs = [
        {
            "pack_name": "history_background_pack",
            "crew_name": "history_background_crew",
            "crew_cls": ExplodingSubCrew,
            "output_file_name": "01_history_background_pack.md",
            "state_attr": "history_background_pack_path",
            "title": "历史与背景分析包",
            "checkpoint_code": "cp02a_history_background_pack",
        }
    ]

    def fake_write_run_debug_manifest(**kwargs) -> str:
        """
        目的：拦截 failed 场景下的 manifest 写入参数。
        功能：记录失败状态字段，供测试直接断言。
        实现逻辑：把 manifest 参数保存到外层列表，再返回一个临时路径。
        可调参数：`kwargs`。
        默认参数及原因：默认不写真实 manifest 文件，原因是这里只关注失败边界是否被正确记录。
        """

        captured_manifests.append(dict(kwargs))
        return (tmp_path / "run_manifest.json").as_posix()

    def fake_write_checkpoint(checkpoint_code: str, payload: dict[str, object]) -> str:
        """
        目的：收集 research failed 场景下的 checkpoint。
        功能：记录失败 checkpoint 的代号和载荷。
        实现逻辑：把参数追加到外层列表，再返回占位路径。
        可调参数：`checkpoint_code` 和 `payload`。
        默认参数及原因：默认不落真实文件，原因是这里只关心字段是否正确。
        """

        captured_checkpoints.append((checkpoint_code, payload))
        return "checkpoint.json"

    monkeypatch.setattr(research_flow_module, "RESEARCH_SUB_CREW_SPECS", fake_specs)
    monkeypatch.setattr(research_flow_module, "write_run_debug_manifest", fake_write_run_debug_manifest)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_checkpoint", fake_write_checkpoint)
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    with pytest.raises(RuntimeError, match="boom"):
        flow._run_research_stage()

    assert flow.state.failed_stage == "research"
    assert flow.state.failed_crew == "history_background_crew"
    assert "boom" in flow.state.error_message
    assert flow.state.blocked_packs == []
    assert flow.state.block_reason == ""
    assert captured_manifests[-1]["status"] == "failed"
    assert captured_manifests[-1]["failed_stage"] == "research"
    assert captured_manifests[-1]["failed_crew"] == "history_background_crew"
    assert "boom" in str(captured_manifests[-1]["error_message"])
    assert captured_checkpoints[-1][0] == "cp03_research_failed"
    assert captured_checkpoints[-1][1]["stage"] == "research"
    assert captured_checkpoints[-1][1]["crew_name"] == "history_background_crew"
    assert "boom" in str(captured_checkpoints[-1][1]["error_message"])


def test_write_final_report_markdown_includes_all_upstream_sources_and_registry_appendix(tmp_path):
    """
    目的：锁住最终 Markdown 会确定性纳入 thesis、research、valuation 和 registry snapshot。
    功能：检查 `_write_final_report_markdown()` 会按固定顺序完整拼接所有上游 md，并在末尾追加 registry appendix。
    实现逻辑：构造最小 flow，写入带唯一标记的上游文件后直接生成最终报告，再断言章节、顺序和关键文本都存在。
    可调参数：`tmp_path`。
    默认参数及原因：默认直接调用 flow 内部写盘函数，原因是这里要验证真正进入 PDF 之前的最终 markdown 真相。
    """

    flow = _build_flow(tmp_path)
    artifacts = [
        ("investment_thesis_path", tmp_path / "investment_thesis.md", "# 投资逻辑包\n\nTHESIS_MARKER\n"),
        ("diligence_questions_path", tmp_path / "diligence_questions.md", "# 尽调问题包\n\nDILIGENCE_MARKER\n"),
        ("history_background_pack_path", tmp_path / "history_background.md", "# 历史与背景分析包\n\nHISTORY_MARKER\n"),
        ("industry_pack_path", tmp_path / "industry.md", "# 行业分析包\n\nINDUSTRY_MARKER\n"),
        ("business_pack_path", tmp_path / "business.md", "# 业务分析包\n\nBUSINESS_MARKER\n"),
        ("operating_metrics_pack_path", tmp_path / "operating_metrics.md", "# 运营指标分析包\n\nOPS_MARKER\n"),
        ("finance_pack_path", tmp_path / "finance.md", "# 财务分析包\n\nFINANCE_MARKER\n"),
        ("risk_pack_path", tmp_path / "risk.md", "# 风险分析包\n\nRISK_MARKER\n"),
        ("peer_info_pack_path", tmp_path / "peer_info.md", "# 同行信息分析包\n\nPEER_INFO_MARKER\n"),
        ("peers_pack_path", tmp_path / "peers.md", "# 可比公司分析包\n\nPEERS_MARKER\n"),
        ("intrinsic_value_pack_path", tmp_path / "intrinsic_value.md", "# 内在价值分析包\n\nINTRINSIC_MARKER\n"),
        ("valuation_pack_path", tmp_path / "valuation.md", "# 综合估值分析包\n\nVALUATION_MARKER\n"),
        (
            "research_internal_review_summary_path",
            tmp_path / "research_internal_review_summary.md",
            "# Research 内部校验摘要\n\nSUMMARY_MARKER\n",
        ),
    ]
    for attr_name, path, text in artifacts:
        setattr(flow.state, attr_name, path.as_posix())
        path.write_text(text, encoding="utf-8")

    registry_snapshot_path = Path(flow.state.registry_snapshot_markdown_path)
    registry_snapshot_path.write_text("# 证据注册表\n\nREGISTRY_MARKER\n", encoding="utf-8")

    final_report_path = flow._write_final_report_markdown()
    report_text = Path(final_report_path).read_text(encoding="utf-8")

    assert final_report_path == Path(flow.state.final_report_markdown_path).resolve().as_posix()
    assert "## 10. 综合估值" in report_text
    assert "### 10.1 可比公司分析包" in report_text
    assert "### 10.2 内在价值分析包" in report_text
    assert "### 10.3 综合估值分析包" in report_text
    assert "## 11. Research 内部校验摘要与结论边界" in report_text
    assert "## 附录：Registry Snapshot" in report_text
    assert "### 投资逻辑包" in report_text
    assert "### 证据注册表" in report_text

    marker_order = [
        "THESIS_MARKER",
        "DILIGENCE_MARKER",
        "HISTORY_MARKER",
        "INDUSTRY_MARKER",
        "BUSINESS_MARKER",
        "OPS_MARKER",
        "FINANCE_MARKER",
        "RISK_MARKER",
        "PEER_INFO_MARKER",
        "PEERS_MARKER",
        "INTRINSIC_MARKER",
        "VALUATION_MARKER",
        "SUMMARY_MARKER",
        "REGISTRY_MARKER",
    ]
    assert [report_text.index(marker) for marker in marker_order] == sorted(
        report_text.index(marker) for marker in marker_order
    )


def test_publish_if_passed_writes_final_markdown_before_writeup_export(tmp_path, monkeypatch):
    """
    目的：验证 `publish_if_passed()` 会先写出确定性最终 Markdown，再调用 writeup crew 只做导出前确认。
    功能：检查最终报告内容已落盘、writeup 输入只保留稳定路径，而且 kickoff 时 Markdown 文件已经存在。
    实现逻辑：构造最小 flow 和假 writeup crew，写入上游文件后执行 `publish_if_passed()`，再断言落盘结果与输入边界。
    可调参数：`tmp_path` 用于隔离临时文件，`monkeypatch` 用于替换 writeup crew 和 manifest/checkpoint 写盘。
    默认参数及原因：默认不触发真实模型和 PDF 导出，原因是这里关注的是 publish 链路接线与产物先后顺序。
    """

    flow = _build_flow(tmp_path)
    artifacts = [
        ("investment_thesis_path", tmp_path / "investment_thesis.md", "# 投资逻辑包\n\nTHESIS_MARKER\n"),
        ("diligence_questions_path", tmp_path / "diligence_questions.md", "# 尽调问题包\n\nDILIGENCE_MARKER\n"),
        ("history_background_pack_path", tmp_path / "history_background.md", "# 历史与背景分析包\n\nHISTORY_MARKER\n"),
        ("industry_pack_path", tmp_path / "industry.md", "# 行业分析包\n\nINDUSTRY_MARKER\n"),
        ("business_pack_path", tmp_path / "business.md", "# 业务分析包\n\nBUSINESS_MARKER\n"),
        ("operating_metrics_pack_path", tmp_path / "operating_metrics.md", "# 运营指标分析包\n\nOPS_MARKER\n"),
        ("finance_pack_path", tmp_path / "finance.md", "# 财务分析包\n\nFINANCE_MARKER\n"),
        ("risk_pack_path", tmp_path / "risk.md", "# 风险分析包\n\nRISK_MARKER\n"),
        ("peer_info_pack_path", tmp_path / "peer_info.md", "# 同行信息分析包\n\nPEER_INFO_MARKER\n"),
        ("peers_pack_path", tmp_path / "peers.md", "# 可比公司分析包\n\nPEERS_MARKER\n"),
        ("intrinsic_value_pack_path", tmp_path / "intrinsic_value.md", "# 内在价值分析包\n\nINTRINSIC_MARKER\n"),
        ("valuation_pack_path", tmp_path / "valuation.md", "# 综合估值分析包\n\nVALUATION_MARKER\n"),
        (
            "research_internal_review_summary_path",
            tmp_path / "research_internal_review_summary.md",
            "# Research 内部校验摘要\n\nSUMMARY_MARKER\n",
        ),
    ]
    for attr_name, path, text in artifacts:
        setattr(flow.state, attr_name, path.as_posix())
        path.write_text(text, encoding="utf-8")

    registry_snapshot_path = Path(flow.state.registry_snapshot_markdown_path)
    registry_snapshot_path.write_text("# 证据注册表\n\nREGISTRY_MARKER\n", encoding="utf-8")

    captured_inputs: dict[str, object] = {}
    captured_manifest_statuses: list[str] = []

    class FakeWriteupCrew:
        """
        目的：替换真实 writeup crew，避免测试触发模型调用和 PDF 导出。
        功能：记录 kickoff 输入，并确认最终 Markdown 在 kickoff 时已经存在。
        实现逻辑：提供与真实 crew 兼容的 `crew().kickoff()` 接口，把关键状态写入外层字典。
        可调参数：无。
        默认参数及原因：默认不生成额外产物，原因是这里只验证 publish 阶段是否先写正文、再进入导出链路。
        """

        output_log_file_path = None

        def crew(self):
            class FakeRunner:
                def kickoff(self, inputs):
                    captured_inputs.update(inputs)
                    captured_inputs["report_exists_during_kickoff"] = Path(
                        inputs["final_report_markdown_path"]
                    ).exists()

            return FakeRunner()

    monkeypatch.setattr(research_flow_module, "WriteupCrew", FakeWriteupCrew)
    monkeypatch.setattr(flow, "_prepare_tool_context", lambda: None)
    monkeypatch.setattr(flow, "_configure_crew_log", lambda crew_instance, log_path: crew_instance)
    monkeypatch.setattr(flow, "_write_manifest_from_state", lambda status: captured_manifest_statuses.append(status) or "manifest.json")
    monkeypatch.setattr(flow, "_write_checkpoint", lambda checkpoint_code, payload: "checkpoint.json")
    monkeypatch.setattr(flow, "_log_flow", lambda message: "flow.log")

    flow.publish_if_passed()

    report_text = Path(flow.state.final_report_markdown_path).read_text(encoding="utf-8")

    assert captured_manifest_statuses == ["completed"]
    assert captured_inputs["final_report_markdown_path"] == Path(flow.state.final_report_markdown_path).resolve().as_posix()
    assert captured_inputs["final_report_pdf_path"] == flow.state.final_report_pdf_path
    assert captured_inputs["registry_snapshot_markdown_path"] == registry_snapshot_path.resolve().as_posix()
    assert captured_inputs["report_exists_during_kickoff"] is True
    assert "history_background_pack_text" not in captured_inputs
    assert "final_qa_summary" not in captured_inputs
    assert "PEERS_MARKER" in report_text
    assert "INTRINSIC_MARKER" in report_text
    assert "VALUATION_MARKER" in report_text
    assert "SUMMARY_MARKER" in report_text
    assert "REGISTRY_MARKER" in report_text


def test_write_final_report_markdown_keeps_registry_appendix_placeholder_when_snapshot_missing(tmp_path):
    """
    目的：验证 registry snapshot 缺失时，最终报告仍会保留明确附录占位。
    功能：检查 `_write_final_report_markdown()` 不会静默吞掉 registry appendix，而是写出带期望路径的缺失说明。
    实现逻辑：构造最小 flow，删除已初始化的 `registry_snapshot.md` 后直接生成最终 Markdown 并断言附录文本。
    可调参数：`tmp_path`。
    默认参数及原因：默认不补写其他上游材料，原因是这里专门只锁住 registry appendix 的缺失兜底行为。
    """

    flow = _build_flow(tmp_path)
    registry_snapshot_path = Path(flow.state.registry_snapshot_markdown_path)
    if registry_snapshot_path.exists():
        registry_snapshot_path.unlink()

    report_text = Path(flow._write_final_report_markdown()).read_text(encoding="utf-8")

    assert "## 附录：Registry Snapshot" in report_text
    assert "上游材料缺失：Registry Snapshot" in report_text
    assert registry_snapshot_path.resolve().as_posix() in report_text


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

    flow._run_valuation_stage()

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
    monkeypatch.setattr(flow, "_run_valuation_stage", lambda: "valuation.md")

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


def test_writeup_compile_task_is_non_destructive_preflight():
    """
    目的：锁住 writeup crew 的 compile 任务已退化为非破坏性确认，不再覆盖最终 Markdown。
    功能：检查 `writeup_crew/config/tasks.yaml` 中 compile 任务不再声明 `output_file`，并明确禁止重写正文。
    实现逻辑：直接读取 YAML 配置，断言关键描述短语与导出任务的 markdown_path 占位符仍保持稳定。
    可调参数：当前无。
    默认参数及原因：默认只校验最关键的静态短语，原因是这能稳定覆盖行为边界，又不会把整段 prompt 锁得过脆。
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

    assert "output_file" not in compile_task
    assert "不要重新生成 Markdown 正文" in compile_task["description"]
    assert "不要覆盖或改写 {final_report_markdown_path}" in compile_task["description"]
    assert "registry snapshot" in compile_task["description"]
    assert compile_task["expected_output"] == "Markdown 已确认就绪，可直接导出 PDF。\n"
    assert "markdown_path={final_report_markdown_path}" in export_task["description"]
