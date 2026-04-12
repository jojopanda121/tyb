from __future__ import annotations

# 设计目的：统一管理预处理、研究、估值、投资论点、质检和成文流程。
# 模块功能：预处理 PDF、顺序调度六类任务组、按 QA 结果路由，并维护全程状态。
# 实现逻辑：先准备证据底座，再依次执行模板初始化、research、valuation、thesis、QA 和 writeup。
# 可调参数：三类阶段的自动返工上限、各阶段输入拼接方式和 gate 路由标签。
# 默认参数及原因：三个阶段默认各允许 1 次自动返工，原因是总运行次数固定为 2 次。

import json
from pathlib import Path

from crewai.flow.flow import Flow, listen, or_, router, start

from automated_research_report_generator.crews.business_crew.business_crew import BusinessCrew
from automated_research_report_generator.crews.financial_crew.financial_crew import FinancialCrew
from automated_research_report_generator.crews.history_background_crew.history_background_crew import (
    HistoryBackgroundCrew,
)
from automated_research_report_generator.flow.common import (
    DEFAULT_PDF_PATH,
    activate_run_preprocess_log,
    append_text_log_line,
    build_run_directories,
    ensure_directory,
    read_text_if_exists,
    run_crew_log_path,
    run_flow_log_path,
    write_run_debug_manifest,
)
from automated_research_report_generator.crews.investment_thesis_crew.investment_thesis_crew import (
    InvestmentThesisCrew,
)
from automated_research_report_generator.crews.industry_crew.industry_crew import IndustryCrew
from automated_research_report_generator.crews.operating_metrics_crew.operating_metrics_crew import (
    OperatingMetricsCrew,
)
from automated_research_report_generator.crews.peer_info_crew.peer_info_crew import PeerInfoCrew
from automated_research_report_generator.crews.qa_crew.qa_crew import QACrew
from automated_research_report_generator.crews.risk_crew.risk_crew import RiskCrew
from automated_research_report_generator.crews.valuation_crew.valuation_crew import ValuationCrew
from automated_research_report_generator.crews.writeup_crew.writeup_crew import WriteupCrew
from automated_research_report_generator.flow.document_metadata import resolve_pdf_document_metadata_payload
from automated_research_report_generator.flow.models import GateReviewOutput, ResearchFlowState
from automated_research_report_generator.flow.pdf_indexing import (
    ensure_pdf_page_index,
    reset_pdf_preprocessing_runtime_state,
)
from automated_research_report_generator.flow.registry import (
    apply_gate_review,
    build_registry_diff_summary,
    entry_ids_for_packs,
    initialize_registry,
    initialize_registry_from_template,
    load_registry_template,
    render_registry_markdown,
    register_evidence,
    save_registry_snapshot,
)
from automated_research_report_generator.tools import set_evidence_registry_context
from automated_research_report_generator.tools.document_metadata_tools import save_document_metadata
from automated_research_report_generator.tools.pdf_page_tools import activate_page_index_directory, set_pdf_context

# 设计目的：标记当前主流程里会被真正消费的阶段事件，给后续节点提供稳定的 router outcome。
# 模块功能：统一用字符串事件连接研究 gate、估值和 thesis 阶段，避免直接监听方法引用。
# 实现逻辑：显式定义事件常量，让 Flow 在初轮和 research 定向重跑时都能正确触发后续监听。
# 可调参数：事件名称字符串；如需重命名，必须同步修改 return 语句和监听装饰器。
# 默认参数及原因：研究、估值、主线三个主节点各自一个事件常量，原因是便于追踪当前有效链路。
RESEARCH_STAGE_COMPLETED_EVENT = "research_stage_completed"
VALUATION_STAGE_COMPLETED_NO_GATE_EVENT = "valuation_stage_completed_no_gate"
THESIS_STAGE_COMPLETED_NO_GATE_EVENT = "thesis_stage_completed_no_gate"
RESEARCH_GATE_RETRY_EVENT = "research_gate_retry_requested"
RESEARCH_GATE_FORCE_PASS_EVENT = "research_gate_force_passed"
STAGE_LABELS = {
    "research": "研究阶段",
    "valuation": "估值阶段",
    "thesis": "投资主线阶段",
}
RESEARCH_SUB_CREW_SPECS = [
    {
        "pack_name": "history_background_pack",
        "crew_name": "history_background_crew",
        "crew_cls": HistoryBackgroundCrew,
        "output_file_name": "01_history_background_pack.md",
        "state_attr": "history_background_pack_path",
        "title": "历史与背景分析包",
        "checkpoint_code": "cp02a_history_background_pack",
    },
    {
        "pack_name": "industry_pack",
        "crew_name": "industry_crew",
        "crew_cls": IndustryCrew,
        "output_file_name": "02_industry_pack.md",
        "state_attr": "industry_pack_path",
        "title": "行业分析包",
        "checkpoint_code": "cp02b_industry_pack",
    },
    {
        "pack_name": "business_pack",
        "crew_name": "business_crew",
        "crew_cls": BusinessCrew,
        "output_file_name": "03_business_pack.md",
        "state_attr": "business_pack_path",
        "title": "业务分析包",
        "checkpoint_code": "cp02c_business_pack",
    },
    {
        "pack_name": "peer_info_pack",
        "crew_name": "peer_info_crew",
        "crew_cls": PeerInfoCrew,
        "output_file_name": "04_peer_info_pack.md",
        "state_attr": "peer_info_pack_path",
        "title": "同行信息分析包",
        "checkpoint_code": "cp02d_peer_info_pack",
    },
    {
        "pack_name": "finance_pack",
        "crew_name": "financial_crew",
        "crew_cls": FinancialCrew,
        "output_file_name": "05_finance_pack.md",
        "state_attr": "finance_pack_path",
        "title": "财务分析包",
        "checkpoint_code": "cp02e_finance_pack",
    },
    {
        "pack_name": "operating_metrics_pack",
        "crew_name": "operating_metrics_crew",
        "crew_cls": OperatingMetricsCrew,
        "output_file_name": "06_operating_metrics_pack.md",
        "state_attr": "operating_metrics_pack_path",
        "title": "运营指标分析包",
        "checkpoint_code": "cp02f_operating_metrics_pack",
    },
    {
        "pack_name": "risk_pack",
        "crew_name": "risk_crew",
        "crew_cls": RiskCrew,
        "output_file_name": "07_risk_pack.md",
        "state_attr": "risk_pack_path",
        "title": "风险分析包",
        "checkpoint_code": "cp02g_risk_pack",
    },
]


class ResearchReportFlow(Flow[ResearchFlowState]):
    """
    目的：把 PDF 预处理、模板初始化、7 个 research sub-crew、估值、投资主线和成文串成一条稳定主流程。
    功能：管理阶段执行顺序、research 外部 QA 重跑、运行状态落盘和最终产物输出。
    实现逻辑：先准备证据底座，再依次执行模板初始化、research、research gate、valuation、thesis、writeup。
    可调参数：`max_research_loops`、阶段输入拼接方式和输出路径。
    默认参数及原因：research 阶段默认允许 1 次自动返工，原因是总运行次数固定为 2 次。
    """
    max_research_loops = 1

    @start()
    def prepare_evidence(self):
        """
        目的：为整次 Flow 建立最小且真实的运行上下文。
        功能：解析 PDF、生成元数据与页索引、初始化 registry，并落盘 run 目录。
        实现逻辑：先校验 PDF 路径，再依次完成预处理、上下文注入和状态字段写回。
        可调参数：PDF 路径来自 `state.pdf_file_path` 或 `DEFAULT_PDF_PATH`。
        默认参数及原因：默认使用 `DEFAULT_PDF_PATH`，原因是本地直接运行时需要一个稳定入口。
        """

        pdf_path = Path(self.state.pdf_file_path or DEFAULT_PDF_PATH).expanduser().resolve()
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")

        reset_pdf_preprocessing_runtime_state()
        # 先在内存里解析 metadata，再用识别出的公司名创建 run 目录，
        # 避免 metadata 先落到 `.cache/pdf_page_indexes/` 这类 run 外公共路径。
        metadata_payload = resolve_pdf_document_metadata_payload(str(pdf_path))
        run_paths = build_run_directories(metadata_payload.company_name)
        registry_path = Path(run_paths["cache_dir"]) / "registry" / "evidence_registry.json"
        artifact_dir = Path(run_paths["cache_dir"])
        indexing_dir = Path(run_paths["run_root_dir"]) / "indexing"
        self.state.pdf_file_path = pdf_path.as_posix()
        self.state.run_slug = Path(run_paths["run_slug"]).name
        self.state.run_cache_dir = Path(run_paths["cache_dir"]).as_posix()
        self.state.run_output_dir = artifact_dir.as_posix()
        self.state.final_report_markdown_path = (artifact_dir / f"{pdf_path.stem}_v2_report.md").as_posix()
        self.state.final_report_pdf_path = (artifact_dir / f"{pdf_path.stem}_v2_report.pdf").as_posix()
        activate_run_preprocess_log(self.state.run_slug)
        activate_page_index_directory(indexing_dir)
        # metadata 首次落盘就直接写入当前 run 的 `indexing/`，
        # 让 metadata 和 page index 从一开始就在同一运行边界内。
        document_metadata_path = (
            Path(
                save_document_metadata(
                    metadata_payload,
                    indexing_dir / f"{pdf_path.stem}_document_metadata.json",
                )
            )
            .resolve()
            .as_posix()
        )
        self._log_flow(f"prepare_evidence started | pdf_file_path={pdf_path.as_posix()}")

        page_index_path = ensure_pdf_page_index(str(pdf_path), company_name=metadata_payload.company_name)
        set_pdf_context(str(pdf_path), page_index_path)
        initialize_registry(
            metadata_payload.company_name,
            metadata_payload.industry,
            registry_path,
            periods=metadata_payload.periods,
        )
        set_evidence_registry_context(registry_path.as_posix())

        self.state.company_name = metadata_payload.company_name
        self.state.industry = metadata_payload.industry
        self.state.document_metadata_file_path = document_metadata_path
        self.state.page_index_file_path = page_index_path
        self.state.evidence_registry_path = registry_path.as_posix()
        self._write_manifest_from_state("prepared")
        self._write_checkpoint(
            "cp00_prepared",
            {
                "pdf_file_path": self.state.pdf_file_path,
                "company_name": self.state.company_name,
                "industry": self.state.industry,
                "document_metadata_file_path": self.state.document_metadata_file_path,
                "page_index_file_path": self.state.page_index_file_path,
            },
        )
        self._log_flow(
            "prepare_evidence completed | "
            f"run_slug={self.state.run_slug} | "
            f"company_name={self.state.company_name} | "
            f"industry={self.state.industry}"
        )
        return {"company_name": self.state.company_name, "industry": self.state.industry}

    @listen(prepare_evidence)
    def build_research_plan(self):
        """
        目的：在正式研究前先用固定模板初始化 research registry。
        功能：加载 YAML 模板、完成占位符替换并把结果写回 registry。
        实现逻辑：直接读取模板并覆盖当前 registry，不再调用 planning crew 或生成额外 planning 产物。
        可调参数：模板文件路径和模板中 entry 的条目定义。
        默认参数及原因：默认使用仓库内固定模板，原因是当前 planning 已切换到确定性初始化。
        """

        self._log_flow("build_research_plan started | mode=deterministic_template")
        template_entries = load_registry_template(
            self.state.company_name,
            self.state.industry,
        )
        initialize_registry_from_template(
            self.state.company_name,
            self.state.industry,
            template_entries,
            self.state.evidence_registry_path,
        )
        self._log_flow(
            "build_research_plan completed | "
            f"entry_count={len(template_entries)} | "
            f"registry_path={self.state.evidence_registry_path}"
        )
        self._write_checkpoint(
            "cp01_planned",
            {
                "registry_path": self.state.evidence_registry_path,
                "entry_count": len(template_entries),
                "owner_distribution": {
                    spec["crew_name"]: len(
                        [entry for entry in template_entries if entry.owner_crew == spec["crew_name"]]
                    )
                    for spec in RESEARCH_SUB_CREW_SPECS
                },
            },
        )
        return self.state.evidence_registry_path

    @router(build_research_plan)
    def run_research_crew(self):
        """
        目的：触发研究阶段的首轮执行。
        功能：调用 `_run_research_stage("initial")`，顺序执行 7 个 research sub-crew。
        实现逻辑：首轮执行完成后返回 `RESEARCH_STAGE_COMPLETED_EVENT` 作为 router outcome。
        可调参数：当前无额外参数。
        默认参数及原因：固定使用 `initial`，原因是日志里需要区分首轮和返工。
        """

        self._run_research_stage("initial")
        return RESEARCH_STAGE_COMPLETED_EVENT

    @router(RESEARCH_GATE_RETRY_EVENT)
    def rerun_research(self):
        """
        目的：响应研究阶段 gate 的自动返工。
        功能：按 QA 指出的 `affected_packs` 定向重跑 research sub-crew，并在完成后重新回到 gate。
        实现逻辑：调用 `_run_research_stage("qa_revision", targeted_packs=...)`，再返回 `RESEARCH_STAGE_COMPLETED_EVENT`。
        可调参数：当前无额外参数。
        默认参数及原因：固定使用 `qa_revision`，原因是便于记录返工来源。
        """
        self._run_research_stage(
            "qa_revision",
            targeted_packs=(self.state.coverage_report_research.affected_packs if self.state.coverage_report_research else []),
            qa_feedback=self.state.last_research_qa_feedback,
        )
        return RESEARCH_STAGE_COMPLETED_EVENT

    @listen(RESEARCH_STAGE_COMPLETED_EVENT)
    def review_research_gate(self):
        """
        目的：对研究阶段结果做 QA 复核。
        功能：运行 research 外部 QA gate，并把结果同步到 state 和 registry。
        实现逻辑：监听研究完成事件，调用 `_run_qa_stage()` 的 coverage 任务，再按 affected_packs 定向回写。
        可调参数：研究阶段参与审查的 pack 列表和 `stage_focus` 文案。
        默认参数及原因：默认审查 7 个 research 包，原因是它们共同构成估值前研究底座。
        """
        coverage, consistency = self._run_qa_stage(
            stage_name="research",
            stage_focus="估值前的公司核心研究分析包，重点检查跨 pack 一致性、覆盖度和未关闭缺口。",
            stage_bundle=self._compose_stage_bundle(
                [
                    self.state.history_background_pack_path,
                    self.state.industry_pack_path,
                    self.state.business_pack_path,
                    self.state.peer_info_pack_path,
                    self.state.finance_pack_path,
                    self.state.operating_metrics_pack_path,
                    self.state.risk_pack_path,
                ]
            ),
            run_consistency_review=False,
        )
        self.state.coverage_report_research = coverage
        self.state.qa_report_research = consistency
        affected_packs = coverage.affected_packs or [spec["pack_name"] for spec in RESEARCH_SUB_CREW_SPECS]
        entry_ids = entry_ids_for_packs(
            self.state.evidence_registry_path,
            affected_packs,
        )
        apply_gate_review(
            self.state.evidence_registry_path,
            stage_name="research_qa",
            entry_ids=entry_ids,
            review=coverage,
        )
        self.state.last_research_qa_feedback = self._qa_feedback_text(coverage)
        self._write_checkpoint(
            "cp03_research_gate",
            {
                "coverage_status": coverage.status,
                "coverage_summary": coverage.summary,
                "affected_packs": affected_packs,
                "key_gaps": coverage.key_gaps,
                "priority_actions": coverage.priority_actions,
            },
        )
        return {"coverage_status": coverage.status, "consistency_status": consistency.status}

    @router(review_research_gate)
    def route_research_gate(self):
        """
        目的：根据研究阶段 gate 结果决定后续路径。
        功能：统一判断通过、重跑或强制放行。
        实现逻辑：把研究阶段的 QA 结果交给 `_route_gate()` 处理。
        可调参数：当前循环次数、最大循环次数和分支标签。
        默认参数及原因：默认最多自动返工 1 次，原因是总运行次数固定为 2 次。
        """

        return self._route_gate(
            stage_name="research",
            coverage=self.state.coverage_report_research,
            consistency=self.state.qa_report_research,
            current_loops=self.state.research_loop_count,
            max_loops=self.max_research_loops,
            rerun_label=RESEARCH_GATE_RETRY_EVENT,
            pass_label="research_gate_passed",
            force_pass_label=RESEARCH_GATE_FORCE_PASS_EVENT,
            loop_attr="research_loop_count",
        )

    @router(or_("research_gate_passed", RESEARCH_GATE_FORCE_PASS_EVENT))
    def run_valuation_crew(self):
        """
        目的：触发估值阶段的首轮执行。
        功能：调用 `_run_valuation_stage("initial")`，并直接进入 thesis 阶段。
        实现逻辑：估值阶段不再经过外部 QA gate，执行完成后返回无 gate 事件。
        可调参数：当前无额外参数。
        默认参数及原因：固定使用 `initial`，原因是日志里需要区分首轮和返工。
        """

        self._run_valuation_stage("initial")
        return VALUATION_STAGE_COMPLETED_NO_GATE_EVENT

    def _run_thesis_stage(self) -> None:
        """
        目的：封装 thesis 阶段的实际执行过程。
        功能：组合前序 pack 输入，运行 `InvestmentThesisCrew`，并登记 thesis 产物。
        实现逻辑：读取研究与估值阶段产物，执行 crew，再把 thesis 与尽调问题路径写回 state。
        可调参数：`thesis_output_dir` 与各类 pack 文本输入。
        默认参数及原因：thesis 产物默认写入 `thesis/iter_XX`，原因是每轮返工都需要保留单独版本。
        """

        iteration_number = self._stage_iteration_number("thesis")
        thesis_dir = self._stage_iteration_dir("thesis")
        self._log_flow(
            f"run_investment_thesis_crew started | iteration={iteration_number} | "
            f"thesis_output_dir={thesis_dir.as_posix()}"
        )
        inputs = self._base_inputs() | {
            "thesis_output_dir": thesis_dir.as_posix(),
            "history_background_pack_text": self._read(self.state.history_background_pack_path),
            "industry_pack_text": self._read(self.state.industry_pack_path),
            "business_pack_text": self._read(self.state.business_pack_path),
            "peer_info_pack_text": self._read(self.state.peer_info_pack_path),
            "finance_pack_text": self._read(self.state.finance_pack_path),
            "operating_metrics_pack_text": self._read(self.state.operating_metrics_pack_path),
            "risk_pack_text": self._read(self.state.risk_pack_path),
            "peers_pack_text": self._read(self.state.peers_pack_path),
            "valuation_pack_text": self._read(self.state.valuation_pack_path),
            "registry_full_text": self._read(self.state.evidence_registry_path),
        }
        self._prepare_tool_context()
        thesis_crew = self._configure_crew_log(
            InvestmentThesisCrew(),
            self._crew_log_path("investment_thesis_crew"),
        )
        thesis_crew.crew().kickoff(inputs=inputs)
        self.state.investment_thesis_path = (thesis_dir / "01_investment_thesis.md").as_posix()
        self.state.diligence_questions_path = (thesis_dir / "02_diligence_questions.md").as_posix()
        self._log_flow(
            "run_investment_thesis_crew completed | "
            f"investment_thesis_path={self.state.investment_thesis_path} | "
            f"diligence_questions_path={self.state.diligence_questions_path}"
        )
        self._write_checkpoint(
            "cp05_thesis",
            {
                "investment_thesis_path": self.state.investment_thesis_path,
                "diligence_questions_path": self.state.diligence_questions_path,
            },
        )

    @router(VALUATION_STAGE_COMPLETED_NO_GATE_EVENT)
    def run_investment_thesis_crew(self):
        """
        目的：触发 thesis 阶段的首轮执行。
        功能：调用 `_run_thesis_stage()`，并直接进入 writeup 阶段。
        实现逻辑：thesis 阶段不再经过外部 QA gate，执行完成后返回无 gate 事件。
        可调参数：当前无额外参数。
        默认参数及原因：默认输出写入 `thesis/iter_01`，原因是单次运行仍需要稳定产物目录。
        """

        self._run_thesis_stage()
        return THESIS_STAGE_COMPLETED_NO_GATE_EVENT

    @listen(THESIS_STAGE_COMPLETED_NO_GATE_EVENT)
    def publish_if_passed(self):
        """
        目的：在 thesis 阶段完成后生成最终报告。
        功能：汇总前序产物，运行 writeup crew，输出 Markdown 与 PDF。
        实现逻辑：把各阶段文本和最终 QA 摘要注入 writeup 输入，执行后更新 manifest。
        可调参数：最终报告路径、阶段产物文本和终检摘要。
        默认参数及原因：默认复用 state 中已经确定的路径，原因是保证产物位置稳定。
        """

        self._log_flow("publish_if_passed started")
        inputs = self._base_inputs() | {
            "history_background_pack_text": self._read(self.state.history_background_pack_path),
            "industry_pack_text": self._read(self.state.industry_pack_path),
            "business_pack_text": self._read(self.state.business_pack_path),
            "peer_info_pack_text": self._read(self.state.peer_info_pack_path),
            "finance_pack_text": self._read(self.state.finance_pack_path),
            "operating_metrics_pack_text": self._read(self.state.operating_metrics_pack_path),
            "risk_pack_text": self._read(self.state.risk_pack_path),
            "peers_pack_text": self._read(self.state.peers_pack_path),
            "intrinsic_value_pack_text": self._read(self.state.intrinsic_value_pack_path),
            "valuation_pack_text": self._read(self.state.valuation_pack_path),
            "investment_thesis_text": self._read(self.state.investment_thesis_path),
            "diligence_questions_text": self._read(self.state.diligence_questions_path),
            "final_qa_summary": self._final_qa_summary(),
            "final_report_markdown_path": self.state.final_report_markdown_path,
            "final_report_pdf_path": self.state.final_report_pdf_path,
        }
        self._prepare_tool_context()
        writeup_crew = self._configure_crew_log(WriteupCrew(), self._crew_log_path("writeup_crew"))
        writeup_crew.crew().kickoff(inputs=inputs)
        self._write_manifest_from_state("completed")
        self._write_checkpoint(
            "cp06_writeup",
            {
                "final_report_markdown_path": self.state.final_report_markdown_path,
                "final_report_pdf_path": self.state.final_report_pdf_path,
            },
        )
        self._log_flow(
            "publish_if_passed completed | "
            f"final_report_markdown_path={self.state.final_report_markdown_path} | "
            f"final_report_pdf_path={self.state.final_report_pdf_path}"
        )
        return {
            "final_report_markdown_path": self.state.final_report_markdown_path,
            "final_report_pdf_path": self.state.final_report_pdf_path,
            "run_debug_manifest_path": self.state.run_debug_manifest_path,
        }

    def _base_inputs(self) -> dict[str, str]:
        """
        目的：集中维护各阶段共享输入。
        功能：从 state 和已落盘文件组装公共上下文。
        实现逻辑：读取基础路径、公司信息和文档摘要后统一返回。
        可调参数：基础输入字段集合。
        默认参数及原因：优先使用 state，原因是避免阶段之间重复推断。
        """
        return {
            "company_name": self.state.company_name,
            "industry": self.state.industry,
            "pdf_file_path": self.state.pdf_file_path,
            "page_index_file_path": self.state.page_index_file_path,
            "document_metadata_file_path": self.state.document_metadata_file_path,
            "document_profile_summary": self._read(self.state.document_metadata_file_path),
        }

    def _prepare_tool_context(self) -> None:
        """
        目的：确保每轮阶段执行前的工具上下文一致。
        功能：同步设置 PDF 上下文和 registry 上下文。
        实现逻辑：直接从当前 state 读取路径并调用工具层上下文设置函数。
        可调参数：当前无显式参数。
        默认参数及原因：每轮都重新设置，原因是这样最稳，不依赖上一步残留状态。
        """

        set_pdf_context(self.state.pdf_file_path, self.state.page_index_file_path)
        set_evidence_registry_context(self.state.evidence_registry_path)

    def _stage_iteration_number(self, stage_name: str) -> int:
        """
        目的：为各阶段生成稳定的 iteration 编号。
        功能：根据当前阶段的循环计数，返回从 1 开始的本轮 iteration 序号。
        实现逻辑：把 `research/valuation/thesis` 映射到对应的 `*_loop_count`，再统一加 1。
        可调参数：`stage_name`。
        默认参数及原因：默认按当前 loop count + 1 计算，原因是首轮执行时计数仍为 0，但目录希望从 `iter_01` 开始。
        """

        mapping = {
            "research": self.state.research_loop_count,
            "valuation": self.state.valuation_loop_count,
            "thesis": self.state.thesis_loop_count,
        }
        if stage_name not in mapping:
            raise ValueError(f"Unknown stage name: {stage_name!r}")
        return mapping[stage_name] + 1

    def _stage_iteration_dir(self, stage_name: str) -> Path:
        """
        目的：为阶段 crew 产物提供按 iteration 隔离的目录。
        功能：返回当前阶段本轮 iteration 的输出目录，例如 `research/iter_01/`。
        实现逻辑：先计算 iteration 编号，再在阶段根目录下创建 `iter_XX` 子目录。
        可调参数：`stage_name`。
        默认参数及原因：目录名固定为 `iter_XX` 两位格式，原因是人工查看和排序时更直观稳定。
        """

        iteration_number = self._stage_iteration_number(stage_name)
        return ensure_directory(Path(self.state.run_cache_dir) / stage_name / f"iter_{iteration_number:02d}")

    def _qa_iteration_dir(self, stage_name: str) -> Path:
        """
        目的：为各阶段 QA 结果提供按 iteration 隔离的目录。
        功能：返回当前阶段本轮 gate 对应的 QA 输出目录，例如 `qa/research/iter_01/`。
        实现逻辑：复用当前阶段的 iteration 编号，在 `qa/<stage_name>/` 下创建 `iter_XX` 子目录。
        可调参数：`stage_name`。
        默认参数及原因：QA 目录跟阶段 iteration 对齐，原因是后续比对“第几轮产物对应哪次 gate”时最直接。
        """

        iteration_number = self._stage_iteration_number(stage_name)
        return ensure_directory(
            Path(self.state.run_cache_dir) / "qa" / stage_name / f"iter_{iteration_number:02d}"
        )

    def _checkpoint_dir(self) -> Path:
        """
        目的：提供当前 run 的 checkpoint 根目录。
        功能：返回 `.cache/<run_slug>/checkpoints/`，不存在时自动创建。
        实现逻辑：固定基于 `run_cache_dir` 拼出路径并调用 `ensure_directory()`。
        可调参数：当前无显式参数。
        默认参数及原因：目录固定按 run 维度隔离，原因是便于单次执行排查。
        """

        return ensure_directory(Path(self.state.run_cache_dir) / "checkpoints")

    def _registry_snapshot_dir(self) -> Path:
        """
        目的：提供 registry 阶段快照目录。
        功能：返回 `.cache/<run_slug>/registry/snapshots/`，不存在时自动创建。
        实现逻辑：固定基于 `run_cache_dir` 拼出路径并调用 `ensure_directory()`。
        可调参数：当前无显式参数。
        默认参数及原因：路径固定，原因是后续 diff 需要稳定目录结构。
        """

        return ensure_directory(Path(self.state.run_cache_dir) / "registry" / "snapshots")

    def _write_checkpoint(self, checkpoint_code: str, payload: dict[str, object]) -> str:
        """
        目的：把关键阶段状态落盘成可回放的 checkpoint。
        功能：写入 checkpoint JSON，并同步保存当前 registry 快照。
        实现逻辑：先写 checkpoint，再把 registry JSON 复制到同名 snapshot 文件。
        可调参数：checkpoint 代号和要保存的 payload。
        默认参数及原因：每个 checkpoint 都带 `run_slug` 和时间戳，原因是排查时需要最小上下文。
        """

        checkpoint_path = self._checkpoint_dir() / f"{checkpoint_code}.json"
        checkpoint_payload = {
            "checkpoint": checkpoint_code,
            "run_slug": self.state.run_slug,
            "generated_at": self._now(),
            **payload,
        }
        checkpoint_path.write_text(
            json.dumps(checkpoint_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if self.state.evidence_registry_path:
            snapshot_path = self._registry_snapshot_dir() / f"{checkpoint_code}.json"
            save_registry_snapshot(self.state.evidence_registry_path, snapshot_path)
        return checkpoint_path.as_posix()

    def _now(self) -> str:
        """
        目的：给 checkpoint 和辅助文本提供统一时间戳。
        功能：返回当前 UTC ISO 时间字符串。
        实现逻辑：直接复用 `utc_timestamp()`。
        可调参数：无。
        默认参数及原因：统一走 UTC，原因是日志和 registry 也是同一时间口径。
        """

        from automated_research_report_generator.flow.common import utc_timestamp

        return utc_timestamp()

    def _qa_feedback_text(self, review: GateReviewOutput) -> str:
        """
        目的：把结构化 QA 结果压缩成适合下轮 crew 直接消费的文本反馈。
        功能：把 summary、key_gaps、priority_actions 和 affected_packs 拼成短文本。
        实现逻辑：按固定顺序拼接几段文本，避免下游 pack 只拿到零散字段。
        可调参数：`review`。
        默认参数及原因：默认输出精简多行文本，原因是 manager 在重跑时更容易直接吸收。
        """

        lines = [f"QA 总结：{review.summary}"]
        if review.key_gaps:
            lines.append("关键缺口：")
            lines.extend([f"- {item}" for item in review.key_gaps])
        if review.priority_actions:
            lines.append("优先动作：")
            lines.extend([f"- {item}" for item in review.priority_actions])
        if review.affected_packs:
            lines.append(f"定向重跑 pack：{', '.join(review.affected_packs)}")
        return "\n".join(lines)

    def _current_registry_diff_summary(self, stage_name: str) -> str:
        """
        目的：给 QA 阶段生成当前 registry 相对上一轮的变化摘要。
        功能：读取当前 registry，并与同阶段上一份 snapshot 做差异摘要。
        实现逻辑：按阶段名映射到固定 checkpoint 代号；如果上一份不存在，则返回首次审查说明。
        可调参数：`stage_name`。
        默认参数及原因：默认只对研究阶段有真实增量意义，原因是当前外部 QA 主要用于 research gate。
        """

        stage_to_checkpoint = {
            "research": "cp03_research_gate",
            "valuation": "cp04_valuation",
            "thesis": "cp05_thesis",
        }
        snapshot_code = stage_to_checkpoint.get(stage_name, "cp03_research_gate")
        previous_snapshot_path = self._registry_snapshot_dir() / f"{snapshot_code}.json"
        current_snapshot_path = self._registry_snapshot_dir() / "__current_tmp__.json"
        save_registry_snapshot(self.state.evidence_registry_path, current_snapshot_path)
        try:
            return build_registry_diff_summary(previous_snapshot_path, current_snapshot_path)
        finally:
            if current_snapshot_path.exists():
                current_snapshot_path.unlink()

    def _research_pack_paths(self) -> list[str]:
        """
        目的：统一返回当前 research 阶段全部 pack 的最新路径集合。
        功能：按设计顺序输出 7 个 research pack 的 state 路径。
        实现逻辑：直接从 state 读取，未生成的路径留空，由 `_compose_stage_bundle()` 自动忽略。
        可调参数：当前无显式参数。
        默认参数及原因：固定顺序返回，原因是 QA 审查需要稳定的上游材料顺序。
        """

        return [
            self.state.history_background_pack_path,
            self.state.industry_pack_path,
            self.state.business_pack_path,
            self.state.peer_info_pack_path,
            self.state.finance_pack_path,
            self.state.operating_metrics_pack_path,
            self.state.risk_pack_path,
        ]

    def _research_subcrew_inputs(
        self,
        *,
        crew_instance,
        pack_name: str,
        pack_title: str,
        output_path: str,
        loop_reason: str,
        qa_feedback: str,
    ) -> dict[str, str]:
        """
        目的：为单个 research sub-crew 生成最小且真实的 kickoff 输入。
        功能：在公共输入之外补充当前 pack 的配置占位符、输出路径、返工反馈和依赖 pack 文本。
        实现逻辑：先取 `_base_inputs()`，再补当前 crew 的 pack 元数据，最后按 pack 名补充必要的上游包文本。
        可调参数：crew 实例、pack 名、pack 标题、输出路径、loop_reason 和 qa_feedback。
        默认参数及原因：只补与当前 pack 真正相关的上游文本，原因是避免 prompt 无谓膨胀。
        """

        inputs = self._base_inputs() | {
            "pack_name": pack_name,
            "owner_crew": getattr(crew_instance, "crew_name", ""),
            "pack_title": getattr(crew_instance, "pack_title", pack_title),
            "pack_focus": getattr(crew_instance, "pack_focus", ""),
            "output_title": getattr(crew_instance, "output_title", pack_title),
            "search_guidance": getattr(crew_instance, "search_guidance", ""),
            "extract_guidance": getattr(crew_instance, "extract_guidance", ""),
            "qa_guidance": getattr(crew_instance, "qa_guidance", ""),
            "synthesize_guidance": getattr(crew_instance, "synthesize_guidance", ""),
            "output_skeleton": getattr(crew_instance, "output_skeleton", ""),
            "pack_output_path": output_path,
            "loop_reason": loop_reason,
            "qa_feedback": qa_feedback,
        }
        if pack_name == "peer_info_pack":
            inputs["industry_pack_text"] = self._read(self.state.industry_pack_path)
            inputs["business_pack_text"] = self._read(self.state.business_pack_path)
        if pack_name in {"finance_pack", "operating_metrics_pack"}:
            inputs["peer_info_pack_text"] = self._read(self.state.peer_info_pack_path)
        return inputs

    def _run_research_stage(
        self,
        loop_reason: str,
        *,
        targeted_packs: list[str] | None = None,
        qa_feedback: str = "",
    ):
        """
        目的：封装研究阶段的实际执行逻辑。
        功能：顺序运行 7 个 research sub-crew，或按 QA 反馈定向重跑部分 pack。
        实现逻辑：创建本轮 research 输出目录，循环调度 pack 对应的子 crew，并只更新本轮重跑的 state 路径。
        可调参数：`loop_reason`、可选的 `targeted_packs` 和 `qa_feedback`。
        默认参数及原因：产物默认写入 `research/iter_XX` 目录，原因是每轮返工结果都需要保留以便对比。
        """

        iteration_number = self._stage_iteration_number("research")
        research_dir = self._stage_iteration_dir("research")
        selected_pack_names = set(targeted_packs or [])
        self._log_flow(
            f"_run_research_stage started | iteration={iteration_number} | loop_reason={loop_reason} | "
            f"research_output_dir={research_dir.as_posix()} | "
            f"targeted_packs={sorted(selected_pack_names) if selected_pack_names else 'all'}"
        )
        self._prepare_tool_context()
        for spec in RESEARCH_SUB_CREW_SPECS:
            pack_name = spec["pack_name"]
            if selected_pack_names and pack_name not in selected_pack_names:
                continue
            output_path = (research_dir / spec["output_file_name"]).as_posix()
            crew_instance = self._configure_crew_log(spec["crew_cls"](), self._crew_log_path(spec["crew_name"]))
            crew_instance.crew().kickoff(
                inputs=self._research_subcrew_inputs(
                    crew_instance=crew_instance,
                    pack_name=pack_name,
                    pack_title=spec["title"],
                    output_path=output_path,
                    loop_reason=loop_reason,
                    qa_feedback=qa_feedback,
                )
            )
            setattr(self.state, spec["state_attr"], output_path)
            self._register_pack_output(output_path, pack_name, spec["title"])
            self._write_checkpoint(
                spec["checkpoint_code"],
                {
                    "pack_name": pack_name,
                    "output_path": output_path,
                    "loop_reason": loop_reason,
                    "qa_feedback": qa_feedback,
                },
            )
        self._log_flow(
            "_run_research_stage completed | "
            f"history_background_pack_path={self.state.history_background_pack_path} | "
            f"industry_pack_path={self.state.industry_pack_path} | "
            f"business_pack_path={self.state.business_pack_path} | "
            f"peer_info_pack_path={self.state.peer_info_pack_path} | "
            f"finance_pack_path={self.state.finance_pack_path} | "
            f"operating_metrics_pack_path={self.state.operating_metrics_pack_path} | "
            f"risk_pack_path={self.state.risk_pack_path}"
        )
        return self.state.risk_pack_path

    def _run_valuation_stage(self, loop_reason: str):
        """
        目的：封装估值阶段的实际执行逻辑。
        功能：运行 valuation crew，并登记三份估值 pack。
        实现逻辑：创建估值输出目录，注入 peer_info、财务、运营指标和风险文本，执行 crew 后回写产物路径。
        可调参数：`loop_reason`。
        默认参数及原因：估值产物默认写入 `valuation/iter_XX`，原因是即使当前不返工，也需要稳定的阶段目录。
        """

        iteration_number = self._stage_iteration_number("valuation")
        valuation_dir = self._stage_iteration_dir("valuation")
        self._log_flow(
            f"_run_valuation_stage started | iteration={iteration_number} | loop_reason={loop_reason} | "
            f"valuation_output_dir={valuation_dir.as_posix()}"
        )
        self._prepare_tool_context()
        valuation_crew = self._configure_crew_log(ValuationCrew(), self._crew_log_path("valuation_crew"))
        valuation_crew.crew().kickoff(
            inputs=self._base_inputs()
            | {
                "valuation_output_dir": valuation_dir.as_posix(),
                "loop_reason": loop_reason,
                "peer_info_pack_text": self._read(self.state.peer_info_pack_path),
                "finance_pack_text": self._read(self.state.finance_pack_path),
                "operating_metrics_pack_text": self._read(self.state.operating_metrics_pack_path),
                "risk_pack_text": self._read(self.state.risk_pack_path),
            }
        )
        self.state.peers_pack_path = (valuation_dir / "01_peers_pack.md").as_posix()
        self.state.intrinsic_value_pack_path = (valuation_dir / "02_intrinsic_value_pack.md").as_posix()
        self.state.valuation_pack_path = (valuation_dir / "03_valuation_pack.md").as_posix()
        for path, pack_name, title in [
            (self.state.peers_pack_path, "peers_pack", "可比公司分析包"),
            (self.state.intrinsic_value_pack_path, "intrinsic_value_pack", "内在价值分析包"),
        ]:
            self._register_pack_output(path, pack_name, title)
        self._log_flow(
            "_run_valuation_stage completed | "
            f"peers_pack_path={self.state.peers_pack_path} | "
            f"intrinsic_value_pack_path={self.state.intrinsic_value_pack_path} | "
            f"valuation_pack_path={self.state.valuation_pack_path}"
        )
        self._write_checkpoint(
            "cp04_valuation",
            {
                "peers_pack_path": self.state.peers_pack_path,
                "intrinsic_value_pack_path": self.state.intrinsic_value_pack_path,
                "valuation_pack_path": self.state.valuation_pack_path,
            },
        )
        return self.state.valuation_pack_path

    def _run_qa_stage(
        self,
        *,
        stage_name: str,
        stage_focus: str,
        stage_bundle: str,
        consistency_stage_bundle: str | None = None,
        run_consistency_review: bool = True,
    ) -> tuple[GateReviewOutput, GateReviewOutput]:
        """
        目的：统一质检阶段的调用方式。
        功能：运行 research 外部 QA gate，并返回覆盖度结果与兼容用的一致性占位结果。
        实现逻辑：创建 `qa/<stage_name>/iter_XX` 目录，执行覆盖度任务后把输出转换成 `GateReviewOutput`。
        可调参数：`stage_name`、`stage_focus`、`stage_bundle` 和兼容保留的 `run_consistency_review`。
        默认参数及原因：结果统一落到 `qa/<stage_name>/iter_XX`，原因是这样能直接对应到同轮 stage 产物。
        """

        iteration_number = self._stage_iteration_number(stage_name)
        qa_dir = self._qa_iteration_dir(stage_name)
        self._log_flow(
            "_run_qa_stage started | "
            f"stage_name={stage_name} | "
            f"iteration={iteration_number} | "
            f"qa_output_dir={qa_dir.as_posix()} | "
            f"run_consistency_review={run_consistency_review}"
        )
        self._prepare_tool_context()
        qa_crew = self._configure_crew_log(QACrew(), self._qa_log_path(stage_name))
        qa_crew.run_consistency_review = run_consistency_review
        result = qa_crew.crew().kickoff(
            inputs=self._base_inputs()
            | {
                "stage_name": stage_name,
                "stage_focus": stage_focus,
                "stage_pack_bundle_text": stage_bundle,
                "consistency_stage_pack_bundle_text": consistency_stage_bundle or stage_bundle,
                "qa_output_dir": qa_dir.as_posix(),
                "registry_markdown_text": render_registry_markdown(self.state.evidence_registry_path),
                "registry_diff_text": self._current_registry_diff_summary(stage_name),
                "registry_full_text": self._read(self.state.evidence_registry_path),
                "investment_thesis_text": self._read(self.state.investment_thesis_path),
                "diligence_questions_text": self._read(self.state.diligence_questions_path),
            }
        )

        coverage_task_output = result.tasks_output[0] if result.tasks_output else None
        coverage_payload = (
            coverage_task_output.json_dict
            if coverage_task_output and coverage_task_output.json_dict is not None
            else coverage_task_output.pydantic if coverage_task_output else None
        )
        coverage = self._coerce_gate_result(coverage_payload)
        consistency = self._build_deferred_consistency_result(stage_name)
        self._log_flow(
            "_run_qa_stage completed | "
            f"stage_name={stage_name} | "
            f"coverage_status={coverage.status} | "
            f"consistency_status={consistency.status}"
        )
        return coverage, consistency

    def _build_deferred_consistency_result(self, stage_name: str) -> GateReviewOutput:
        """
        目的：为延后执行的一致性检查提供稳定的占位结果。
        功能：返回一份明确说明“已延后到 thesis 阶段统一复核”的 `GateReviewOutput`。
        实现逻辑：根据阶段名拼接摘要，并固定返回 `pass` 状态和后续动作提示。
        可调参数：`stage_name`。
        默认参数及原因：默认延后到 thesis 阶段，原因是跨阶段一致性只有在上游产物齐全后才有真实审查对象。
        """

        stage_label = STAGE_LABELS.get(stage_name, stage_name)
        return GateReviewOutput(
            status="pass",
            summary=f"{stage_label} 不单独执行一致性审查，已延后到 thesis 阶段统一复核。",
            key_gaps=[],
            priority_actions=["在 thesis 阶段结合完整 registry 与全部上游产物执行跨阶段一致性审查。"],
        )

    def _configure_crew_log(self, crew_instance, log_path: str):
        """
        目的：给 crew 实例注入当前 run 的日志路径。
        功能：在 crew 创建后覆盖 `output_log_file_path` 并返回实例。
        实现逻辑：直接写入实例属性，不依赖 `__init__` 接收额外参数。
        可调参数：`crew_instance`、`log_path`。
        默认参数及原因：统一由 flow 层注入路径，原因是 run 级目录信息只在 flow 层最完整。
        """

        crew_instance.output_log_file_path = log_path
        return crew_instance

    def _crew_log_path(self, crew_name: str) -> str:
        """
        目的：按当前 run 生成 crew 日志路径。
        功能：根据 `run_slug` 返回指定 crew 的本次运行日志文件。
        实现逻辑：校验 `run_slug` 后调用 `run_crew_log_path()`。
        可调参数：`crew_name`。
        默认参数及原因：按 run 维度隔离日志，原因是便于排查单次执行。
        """

        if not self.state.run_slug:
            raise RuntimeError("run_slug is not initialized for crew logging.")
        return run_crew_log_path(self.state.run_slug, crew_name)

    def _flow_log_path(self) -> str:
        """
        目的：按当前 run 生成 Flow 日志路径。
        功能：根据 `run_slug` 返回本次 Flow 的文本日志文件。
        实现逻辑：当 `run_slug` 可用时调用 `run_flow_log_path()`，否则返回空串。
        可调参数：当前无显式参数。
        默认参数及原因：初始化前返回空串，原因是那时 run 目录还未建立。
        """

        if not self.state.run_slug:
            return ""
        return run_flow_log_path(self.state.run_slug)

    def _log_flow(self, message: str) -> str:
        """
        目的：统一写入 Flow 级日志。
        功能：把阶段推进、路由决策和关键状态写入 Flow 文本日志。
        实现逻辑：先解析当前日志路径，再复用通用追加函数落盘。
        可调参数：`message`。
        默认参数及原因：按一行一条记录，原因是方便 grep 和手动排查。
        """

        log_path = self._flow_log_path()
        if not log_path:
            return ""
        return append_text_log_line(log_path, message)

    def _qa_log_path(self, stage_name: str) -> str:
        """
        目的：为 QA 阶段按子阶段分配日志文件。
        功能：把 `research`、`valuation`、`thesis` 映射到各自 QA 日志路径。
        实现逻辑：优先走阶段专用路径，未知阶段再回退到通用 QA 日志。
        可调参数：`stage_name`。
        默认参数及原因：默认按阶段拆分，原因是同一 QA crew 会被多次调用。
        """

        mapping = {
            "research": "qa_research",
        }
        return self._crew_log_path(mapping.get(stage_name, "qa_research"))

    def _coerce_gate_result(self, payload) -> GateReviewOutput:
        """
        目的：把 QA 输出统一转换成 `GateReviewOutput`。
        功能：兼容模型对象、普通字典和空值三种输入。
        实现逻辑：优先直接返回或校验已有结果，空值时给出保守的 `revise` 结果。
        可调参数：`payload`。
        默认参数及原因：空值默认回写 `revise`，原因是不能在缺少 QA 结果时静默放行。
        """

        if isinstance(payload, GateReviewOutput):
            return payload
        if payload:
            return GateReviewOutput.model_validate(payload)
        return GateReviewOutput(
            status="revise",
            summary="缺少结构化 QA 输出。",
            key_gaps=["当前未拿到 QA 输出结果。"],
            priority_actions=["请重新执行 QA 阶段。"],
        )

    def _register_pack_output(self, path: str, pack_name: str, title: str) -> None:
        """
        目的：把关键中间产物登记到 evidence registry。
        功能：仅在 pack 已经关联到 judgment 类型 entry 时，把 pack 文本作为 context evidence 写入账本。
        实现逻辑：先读取文本，再查询 pack 对应的 judgment entry ID，只有命中时才调用 `register_evidence()`。
        可调参数：`path`、`pack_name`、`title`。
        默认参数及原因：摘要默认截取前 800 个字符，原因是兼顾信息密度和账本体积；没有关联 judgment entry 时直接跳过，原因是避免产生孤立 evidence。
        """

        text = self._read(path)
        if not text:
            return
        entry_ids = entry_ids_for_packs(
            self.state.evidence_registry_path,
            [pack_name],
            entry_types=["judgment"],
        )
        if not entry_ids:
            return
        register_evidence(
            self.state.evidence_registry_path,
            title=title,
            summary=text[:800],
            source_type="crew_output",
            source_ref=path,
            pack_name=pack_name,
            entry_ids=entry_ids,
            stance="context",
            note="Flow-level pack artifact. Pointed judgments should be linked by agent-added evidence rows.",
        )

    def _compose_stage_bundle(self, paths: list[str]) -> str:
        """
        目的：把同一阶段的多个产物合并成一份 QA 审查文本。
        功能：按文件名加标题后拼接各个非空文件内容。
        实现逻辑：逐个读取路径，过滤空文件，再拼成 Markdown 块。
        可调参数：`paths`。
        默认参数及原因：只拼接有内容的文件，原因是缺失文件不应制造空块噪音。
        """

        blocks = []
        for path in paths:
            text = self._read(path)
            if text:
                blocks.append(f"## {Path(path).name}\n\n{text}")
        return "\n\n".join(blocks)

    def _final_qa_summary(self) -> str:
        """
        目的：为成文阶段提供压缩版 QA 摘要。
        功能：把 research 外部 QA 结果整理成简短清单。
        实现逻辑：遍历已有 QA 结果并输出 `标签: 状态 | 摘要` 形式的多行文本。
        可调参数：当前无显式参数。
        默认参数及原因：只输出 research QA，原因是 valuation 与 thesis 已改为不走外部 gate。
        """

        reports = [
            ("研究覆盖度", self.state.coverage_report_research),
            ("研究一致性", self.state.qa_report_research),
        ]
        lines: list[str] = []
        for label, report in reports:
            if not report:
                continue
            lines.append(f"- {label}: {report.status} | {report.summary}")
        return "\n".join(lines)

    def _write_manifest_from_state(self, status: str) -> str:
        """
        目的：统一把当前运行状态写入 manifest。
        功能：把 run 路径、索引文件、账本路径和最终报告路径一次性落盘。
        实现逻辑：复用 `write_run_debug_manifest()`，从 state 提取关键路径后写回 `run_debug_manifest_path`。
        可调参数：`status`。
        默认参数及原因：路径缺失时沿用现有兜底逻辑，原因是异常阶段也要留下可排查信息。
        """

        self.state.run_debug_manifest_path = write_run_debug_manifest(
            run_slug=self.state.run_slug or "unknown-run",
            status=status,
            pdf_file_path=self.state.pdf_file_path or DEFAULT_PDF_PATH.as_posix(),
            run_cache_dir=self.state.run_cache_dir or DEFAULT_PDF_PATH.parent.as_posix(),
            evidence_registry_path=self.state.evidence_registry_path,
            page_index_file_path=self.state.page_index_file_path,
            document_metadata_file_path=self.state.document_metadata_file_path,
            final_report_markdown_path=self.state.final_report_markdown_path,
            final_report_pdf_path=self.state.final_report_pdf_path,
        )
        return self.state.run_debug_manifest_path

    def _route_gate(
        self,
        *,
        stage_name: str,
        coverage: GateReviewOutput | None,
        consistency: GateReviewOutput | None,
        current_loops: int,
        max_loops: int,
        rerun_label: str,
        pass_label: str,
        force_pass_label: str,
        loop_attr: str,
    ) -> str:
        """
        目的：集中处理通用 gate 路由。
        功能：根据 coverage 与 consistency 的结果决定通过、重跑或强制放行。
        实现逻辑：先检查是否全部通过，否则增加循环计数，并在超过上限后返回强制放行标签。
        可调参数：阶段名、QA 结果、循环计数和各类分支标签。
        默认参数及原因：缺失结果按 `revise` 处理，原因是不能静默放过问题。
        """

        statuses = [
            coverage.status if coverage else "revise",
            consistency.status if consistency else "revise",
        ]
        if all(status == "pass" for status in statuses):
            self._log_flow(
                f"{stage_name}_gate passed | "
                f"coverage_status={statuses[0]} | "
                f"consistency_status={statuses[1]}"
            )
            return pass_label
        next_loop_count = current_loops + 1
        if loop_attr == "research_loop_count":
            self.state.research_loop_count = next_loop_count
        elif loop_attr == "valuation_loop_count":
            self.state.valuation_loop_count = next_loop_count
        elif loop_attr == "thesis_loop_count":
            self.state.thesis_loop_count = next_loop_count
        else:
            raise ValueError(f"Unknown loop attribute: {loop_attr!r}")
        if next_loop_count <= max_loops:
            self._log_flow(
                f"{stage_name}_gate retry_requested | "
                f"loop_count={next_loop_count} | "
                f"max_loops={max_loops} | "
                f"coverage_status={statuses[0]} | "
                f"consistency_status={statuses[1]}"
            )
            return rerun_label
        self._log_flow(
            f"{stage_name}_gate force_passed | "
            f"loop_count={next_loop_count} | "
            f"max_loops={max_loops} | "
            f"coverage_status={statuses[0]} | "
            f"consistency_status={statuses[1]}"
        )
        return force_pass_label

    def _read(self, path: str) -> str:
        """
        目的：给 flow 内部提供统一的安全读文件入口。
        功能：复用 `read_text_if_exists()` 读取文本。
        实现逻辑：直接把路径委托给公共读取函数。
        可调参数：`path`。
        默认参数及原因：缺文件返回空串，原因是部分阶段的产物可能尚未生成。
        """

        return read_text_if_exists(path)
