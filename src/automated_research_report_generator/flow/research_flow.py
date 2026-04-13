from __future__ import annotations

# 设计目的：统一管理预处理、研究、估值、投资论点、质检和成文流程。
# 模块功能：预处理 PDF、顺序调度六类任务组、按 QA 结果路由，并维护全程状态。
# 实现逻辑：先准备证据底座，再依次执行模板初始化、research、valuation、thesis、QA 和 writeup。
# 可调参数：三类阶段的自动返工上限、各阶段输入拼接方式和 gate 路由标签。
# 默认参数及原因：三个阶段默认各允许 1 次自动返工，原因是总运行次数固定为 2 次。

import json
from pathlib import Path

from crewai.flow.flow import Flow, listen, router, start

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
from automated_research_report_generator.crews.risk_crew.risk_crew import RiskCrew
from automated_research_report_generator.crews.valuation_crew.valuation_crew import ValuationCrew
from automated_research_report_generator.crews.writeup_crew.writeup_crew import WriteupCrew
from automated_research_report_generator.flow.document_metadata import resolve_pdf_document_metadata_payload
from automated_research_report_generator.flow.models import ResearchFlowState
from automated_research_report_generator.flow.pdf_indexing import (
    ensure_pdf_page_index,
    reset_pdf_preprocessing_runtime_state,
)
from automated_research_report_generator.flow.registry import (
    entry_ids_for_packs,
    initialize_registry,
    initialize_registry_from_template,
    load_registry_template,
    register_evidence,
    save_registry_snapshot,
)
from automated_research_report_generator.tools import set_evidence_registry_context
from automated_research_report_generator.tools.document_metadata_tools import save_document_metadata
from automated_research_report_generator.tools.pdf_page_tools import activate_page_index_directory, set_pdf_context

# 设计目的：标记当前主流程里会被真正消费的阶段事件，给后续节点提供稳定的 router outcome。
# 模块功能：统一用字符串事件连接 research、估值和 thesis 阶段，避免直接监听方法引用。
# 实现逻辑：显式定义事件常量，让 Flow 在 research 完成后稳定进入 valuation，再继续推进后续监听。
# 可调参数：事件名称字符串；如需重命名，必须同步修改 return 语句和监听装饰器。
# 默认参数及原因：研究、估值、主线三个主节点各自一个事件常量，原因是便于追踪当前有效链路。
RESEARCH_STAGE_COMPLETED_EVENT = "research_stage_completed"
VALUATION_STAGE_COMPLETED_NO_GATE_EVENT = "valuation_stage_completed_no_gate"
THESIS_STAGE_COMPLETED_NO_GATE_EVENT = "thesis_stage_completed_no_gate"
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
    功能：管理阶段执行顺序、research 内部校验摘要汇总、运行状态落盘和最终产物输出。
    实现逻辑：先准备证据底座，再依次执行模板初始化、research、valuation、thesis、writeup。
    可调参数：阶段输入拼接方式和输出路径。
    默认参数及原因：research 阶段固定单轮执行，原因是外部 QA gate 已移除，内部校验改由 sub-crew 的 `check_registry` 统一产出。
    """

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
        实现逻辑：研究阶段完成后直接返回 `RESEARCH_STAGE_COMPLETED_EVENT`，供估值阶段继续接力。
        可调参数：当前无额外参数。
        默认参数及原因：固定使用 `initial`，原因是保留阶段语义而不再引入 research 外部返工。
        """

        self._run_research_stage("initial")
        return RESEARCH_STAGE_COMPLETED_EVENT

    @router(RESEARCH_STAGE_COMPLETED_EVENT)
    def run_valuation_crew(self):
        """
        目的：触发估值阶段的首轮执行。
        功能：调用 `_run_valuation_stage("initial")`，并直接进入 thesis 阶段。
        实现逻辑：research 阶段完成后直接进入估值阶段，整个链路不再等待 research 外部 QA gate。
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
        实现逻辑：把各阶段文本和 research 内部校验摘要注入 writeup 输入，执行后更新 manifest。
        可调参数：最终报告路径、阶段产物文本和内部校验摘要。
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
            "final_qa_summary": self._internal_research_review_summary_text(),
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
        实现逻辑：research 固定返回首轮编号；其他阶段继续按对应的 `*_loop_count` 统一加 1。
        可调参数：`stage_name`。
        默认参数及原因：research 固定单轮执行，原因是外部 QA gate 已移除；其他阶段仍按当前 loop count + 1 计算，保证目录从 `iter_01` 开始。
        """

        mapping = {
            "research": 0,
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

    def _research_subcrew_inputs(
        self,
        *,
        crew_instance,
        pack_name: str,
        pack_title: str,
        output_path: str,
        loop_reason: str,
        qa_feedback: str = "",
    ) -> dict[str, str]:
        """
        目的：为单个 research sub-crew 生成最小且真实的 kickoff 输入。
        功能：在公共输入之外补充当前 pack 的配置占位符、输出路径、内部校验占位输入和依赖 pack 文本。
        实现逻辑：先取 `_base_inputs()`，再补当前 crew 的 pack 元数据，最后按 pack 名补充必要的上游包文本。
        可调参数：crew 实例、pack 名、pack 标题、输出路径、loop_reason 和 qa_feedback。
        默认参数及原因：`qa_feedback` 默认空串，原因是 research 外部 gate 已移除，但 YAML 仍需要稳定占位符；其他输入只补与当前 pack 真正相关的上游文本，避免 prompt 无谓膨胀。
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
    ):
        """
        目的：封装研究阶段的实际执行逻辑。
        功能：顺序运行 7 个 research sub-crew，并把每个 crew 的 `check_registry` 输出汇总成内部校验摘要。
        实现逻辑：创建本轮 research 输出目录，循环调度 pack 对应的子 crew，记录 pack 产物，再额外生成 `08_research_internal_registry_checks.md`。
        可调参数：`loop_reason`。
        默认参数及原因：产物默认写入 `research/iter_01`，原因是 research 外部 QA gate 已移除，当前设计只保留固定单轮执行。
        """

        iteration_number = self._stage_iteration_number("research")
        research_dir = self._stage_iteration_dir("research")
        internal_review_memos: list[dict[str, str]] = []
        self._log_flow(
            f"_run_research_stage started | iteration={iteration_number} | loop_reason={loop_reason} | "
            f"research_output_dir={research_dir.as_posix()}"
        )
        self._prepare_tool_context()
        for spec in RESEARCH_SUB_CREW_SPECS:
            pack_name = spec["pack_name"]
            output_path = (research_dir / spec["output_file_name"]).as_posix()
            crew_instance = self._configure_crew_log(spec["crew_cls"](), self._crew_log_path(spec["crew_name"]))
            result = crew_instance.crew().kickoff(
                inputs=self._research_subcrew_inputs(
                    crew_instance=crew_instance,
                    pack_name=pack_name,
                    pack_title=spec["title"],
                    output_path=output_path,
                    loop_reason=loop_reason,
                )
            )
            setattr(self.state, spec["state_attr"], output_path)
            self._register_pack_output(output_path, pack_name, spec["title"])
            internal_review_memos.append(
                {
                    "pack_name": pack_name,
                    "title": spec["title"],
                    "memo": self._extract_check_registry_memo(result),
                }
            )
            self._write_checkpoint(
                spec["checkpoint_code"],
                {
                    "pack_name": pack_name,
                    "output_path": output_path,
                    "loop_reason": loop_reason,
                },
            )
        summary_path = self._write_research_internal_review_summary(research_dir, internal_review_memos)
        missing_pack_names = [item["pack_name"] for item in internal_review_memos if not item["memo"]]
        self._write_checkpoint(
            "cp03_research_internal_checks",
            {
                "summary_path": summary_path,
                "covered_packs": [item["pack_name"] for item in internal_review_memos],
                "missing_packs": missing_pack_names,
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
            f"risk_pack_path={self.state.risk_pack_path} | "
            f"research_internal_review_summary_path={summary_path}"
        )
        return summary_path

    def _extract_check_registry_memo(self, crew_result) -> str:
        """
        目的：从 research sub-crew 的执行结果里提取 `check_registry` 任务输出。
        功能：优先按任务名识别 `check_registry`，必要时回退到固定任务顺序中的第 5 个结果。
        实现逻辑：遍历 `tasks_output` 查找显式任务名；如果没有可用名字，再按当前 6-task 固定链路取索引 4。
        可调参数：`crew_result`。
        默认参数及原因：找不到输出时返回空串，原因是摘要文件需要明确保留缺口，而不是抛异常中断整轮 research。
        """

        tasks_output = getattr(crew_result, "tasks_output", None) or []
        for task_output in tasks_output:
            task_name_candidates = [
                getattr(task_output, "name", None),
                getattr(task_output, "task_name", None),
                getattr(getattr(task_output, "task", None), "name", None),
            ]
            if any(candidate == "check_registry" for candidate in task_name_candidates if isinstance(candidate, str)):
                return self._task_output_text(task_output)
        if len(tasks_output) >= 5:
            return self._task_output_text(tasks_output[4])
        return ""

    def _task_output_text(self, task_output) -> str:
        """
        目的：把 CrewAI 的任务输出对象稳定转成文本。
        功能：兼容 `raw`、`json_dict`、`pydantic` 和少量兜底属性，尽量拿到可直接写入 Markdown 的正文。
        实现逻辑：按最接近原始正文的优先级逐项尝试；结构化对象统一序列化成 UTF-8 JSON 字符串。
        可调参数：`task_output`。
        默认参数及原因：拿不到有效文本时返回空串，原因是内部校验摘要应显式暴露缺口而不是制造伪内容。
        """

        raw_text = getattr(task_output, "raw", None)
        if isinstance(raw_text, str) and raw_text.strip():
            return raw_text.strip()

        json_dict = getattr(task_output, "json_dict", None)
        if json_dict:
            return json.dumps(json_dict, ensure_ascii=False, indent=2)

        pydantic_payload = getattr(task_output, "pydantic", None)
        if pydantic_payload is not None:
            if hasattr(pydantic_payload, "model_dump_json"):
                return pydantic_payload.model_dump_json(indent=2)
            try:
                return json.dumps(pydantic_payload, ensure_ascii=False, indent=2)
            except TypeError:
                return str(pydantic_payload)

        for attr_name in ("result", "summary", "description"):
            attr_value = getattr(task_output, attr_name, None)
            if isinstance(attr_value, str) and attr_value.strip():
                return attr_value.strip()
        return ""

    def _write_research_internal_review_summary(
        self,
        research_dir: Path,
        review_memos: list[dict[str, str]],
    ) -> str:
        """
        目的：把 7 个 research packs 的内部校验结果落成独立 Markdown 产物。
        功能：生成 `08_research_internal_registry_checks.md`，供 writeup 最后一节直接引用。
        实现逻辑：按 pack 顺序拼出标题和正文；缺失 memo 时写明确占位文本，再把路径回填到 state。
        可调参数：`research_dir` 和 `review_memos`。
        默认参数及原因：没有可用 memo 时保留明确占位，原因是 writeup 阶段不能留空白或悄悄省略这一节。
        """

        summary_path = research_dir / "08_research_internal_registry_checks.md"
        lines = [
            "# Research 内部校验摘要",
            "",
            "本文件汇总 7 个 research packs 在 `check_registry` 任务中的内部校验输出，用于 writeup 的结论边界章节。",
        ]
        for item in review_memos:
            lines.extend(
                [
                    "",
                    f"## {item['title']}（{item['pack_name']}）",
                    "",
                    item["memo"].strip() if item["memo"].strip() else "本 pack 未返回 `check_registry` 输出。",
                ]
            )
        if len(lines) == 3:
            lines.extend(["", "本轮未生成内部校验摘要。"])
        summary_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        self.state.research_internal_review_summary_path = summary_path.as_posix()
        return self.state.research_internal_review_summary_path

    def _internal_research_review_summary_text(self) -> str:
        """
        目的：为 writeup 阶段提供稳定可用的内部校验摘要正文。
        功能：优先读取 research 阶段已生成的摘要文件；缺失时返回明确占位文本。
        实现逻辑：复用统一读文件入口，空文本时回退到固定 Markdown 模板。
        可调参数：当前无显式参数。
        默认参数及原因：缺失时明确声明“本轮未生成内部校验摘要”，原因是 writeup 不能把这一节静默吞掉。
        """

        summary_text = self._read(self.state.research_internal_review_summary_path)
        if summary_text.strip():
            return summary_text
        return "# Research 内部校验摘要\n\n本轮未生成内部校验摘要。\n"

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

    def _read(self, path: str) -> str:
        """
        目的：给 flow 内部提供统一的安全读文件入口。
        功能：复用 `read_text_if_exists()` 读取文本。
        实现逻辑：直接把路径委托给公共读取函数。
        可调参数：`path`。
        默认参数及原因：缺文件返回空串，原因是部分阶段的产物可能尚未生成。
        """

        return read_text_if_exists(path)
