# Crew 重构讨论底稿

这个文件用于讨论和修订新的 `agent / task / crew / flow` 运行框架。

原则：
- 先定运行骨架，再落代码。
- 按投资判断产物拆分流程，而不是按报告章节机械拆分任务。
- 尽量减少重复角色、重复命名和重复功能。
- 逻辑顺序要符合真实研究顺序与运行顺序。
- `ResearchPlanner` 只负责初始化问题树与证据地图，不要求一开始想全。
- 后续由 `research_crew` 与 `valuation_crew` 内的分析 agent 动态发现新问题，并持续回填证据地图。

## 1. Crew 主定义表

| Crew | 对应投资逻辑 | 核心问题 | 主要输入 | 主要输出 |
|---|---|---|---|---|
| `planning_crew` | 研究范围定义、问题树初始化、证据地图初始化 | 这次研究先回答哪些一级问题，先看哪些证据，哪些问题暂时未知 | `document_profile` `page_index` | `research_scope` `question_tree` `evidence_map_seed` |
| `research_crew` | 公司历史与治理、行业、业务、财务、风险 | 这家公司怎么发展、所在赛道如何、靠什么赢、财务质量怎样、主要风险是什么 | `document_profile` `page_index` `question_tree` `evidence_registry` 公开资料 | `history_governance_pack` `industry_pack` `business_pack` `finance_pack` `risk_pack` |
| `valuation_crew` | 同行业研究、估值与回报分析 | 当前合理价值多少、上行下行空间如何、回报如何测算 | `finance_pack` `business_pack` `risk_pack` `question_tree` `evidence_registry` 市场数据 | `peers_pack` `valuation_pack` |
| `investment_thesis_crew` | 投资逻辑、关键假设、尽调问题 | 为什么值得投、不值得投的关键点是什么、还需要问什么 | `history_governance_pack` `industry_pack` `business_pack` `finance_pack` `risk_pack` `peers_pack` `valuation_pack` | `investment_thesis` `diligence_questions` |
| `qa_crew` | 覆盖率检查、一致性检查、证据闭环检查 | 研究问题是否覆盖完整，证据是否闭环，最终报告是否一致 | 全部 pack `question_tree` `evidence_registry` `investment_thesis` `diligence_questions` | `coverage_report` `qa_report` |
| `writeup_crew` | 最终成文与导出 | 如何把全部研究产物稳定编排成最终报告 | 全部 pack `investment_thesis` `diligence_questions` `qa_report` | `final_report` |

## 2. Agent / Task 定义表

| Crew | Agent | 建议任务 | 任务输出重点 | 是否需要人工介入 |
|---|---|---|---|---|
| `planning_crew` | `ResearchPlanner` | `define_research_scope` `seed_question_tree` `seed_evidence_map` | 研究范围、一级问题树、初始证据地图；只做第一版骨架，不要求一次想全 | 是，人工复核/修改一级问题 |
| `research_crew` | `HistoryGovernanceAnalyst` | `analyze_history` `analyze_governance` | 发展历程、主要里程碑、关键转折、治理结构、核心管理层与股东关系 | 否 |
| `research_crew` | `IndustryAnalyst` | `analyze_industry_structure` `analyze_industry_economics` `analyze_industry_constraints` | 行业定义、行业价值、行业空间、发展趋势、上下游、竞争、周期、瓶颈、监管 | 否 |
| `research_crew` | `BusinessAnalyst` | `analyze_business_model` `analyze_competitive_position` `analyze_growth_logic` | 商业模式、发展阶段、产品、定位、竞争力、客户、供应商、战略、增长逻辑 | 否 |
| `research_crew` | `FinancialAnalyst` | `normalize_financials` `analyze_financial_quality` | 财务数据标准化、经营数据整理、比率计算、财务与经营质量分析 | 否 |
| `research_crew` | `RiskAnalyst` | `analyze_core_risks` `analyze_external_constraints` | 核心经营风险、客户/供应链/治理风险、地缘政治与出海风险、关键待验证风险点 | 否 |
| `valuation_crew` | `PeerAnalyst` | `build_peer_set` | 可比池、可比口径、剔除理由 | 是，人工复核/修改同行可比 |
| `valuation_crew` | `CashflowAnalyst` | `build_valuation_assumptions` `derive_intrinsic_valuation` `run_sensitivity_analysis` | 现金流估值假设、内在价值区间、敏感性分析 | 是，人工复核/修改估值关键假设 |
| `valuation_crew` | `ValuationAnalyst` | `derive_comparable_valuation` `summarize_valuation` `analyze_upside_downside` `derive_irr` | 可比估值、估值结论汇总、上下行空间、回报测算 | 是，人工复核/修改估值关键假设 |
| `investment_thesis_crew` | `InvestmentSynthesizer` | `synthesize_investment_case` `draft_diligence_questions` | 多空逻辑、关键假设、催化剂、估值展望、盈利空间、投前待验证问题 | 否 |
| `qa_crew` | `CoverageReviewer` | `review_question_coverage` `review_evidence_gaps` | 问题覆盖率、证据冲突、缺口、待补充项、可关闭项 | 否 |
| `qa_crew` | `QualityAssurance` | `review_report_consistency` | 一致性审核、逻辑冲突、数字冲突、引用缺失 | 否 |
| `writeup_crew` | `ReportEditor` | `compile_report` `export_pdf` | 最终报告、PDF 导出 | 否 |

## 3. Tool 定义表

| Tool | 类型 | 用途 | 谁调用 | 状态 |
|---|---|---|---|---|
| `document_metadata_resolver` | flow 内部工具 | 识别公司名、行业、文档标签 | `flow` | 现有能力可保留 |
| `page_index_builder` | flow 内部工具 | 生成逐页索引，作为文档内证据定位底稿 | `flow` | 现有能力可保留 |
| `read_page_index` | 证据工具 | 先定位相关页 | `planning_crew` `research_crew` `valuation_crew` `qa_crew` | 现有 |
| `read_relevant_pages` | 证据工具 | 读取原文证据 | `planning_crew` `research_crew` `valuation_crew` `qa_crew` | 现有 |
| `search_public_sources` | 外部研究工具 | 行业、业务、管理层、竞争、风险的公开资料补充 | `research_crew` | 现有 |
| `import_market_data` | 外部研究工具 | 行业、业务、估值相关外部市场数据导入 | `research_crew` `valuation_crew` | 建议新增 |
| `evidence_registry` | 证据管理工具 | 统一记录研究问题、证据对象、支持/反驳关系、日期、链接、口径说明、冲突和 gap | `planning_crew` `research_crew` `valuation_crew` `qa_crew` | 建议新增 |
| `financial_model_tool` | 计算工具 | 财务标准化、业务数据汇总、比率计算 | `FinancialAnalyst` | 建议新增 |
| `valuation_comparable_tool` | 计算工具 | 可比倍数、同行对比表、估值口径统一 | `PeerAnalyst` `ValuationAnalyst` | 建议新增 |
| `valuation_model_tool` | 计算工具 | 现金流估值计算、估值区间、敏感性分析 | `CashflowAnalyst` `ValuationAnalyst` | 建议新增 |
| `report_renderer` | 输出工具 | Markdown / PDF 导出 | `ReportEditor` | 现有 |

## 4. Flow 定义表

### 4.1 主流程节点表

| Step | Flow 节点 | 依赖输入 | 执行对象 | 成功产物 | 通过后去向 | 未通过去向 |
|---|---|---|---|---|---|---|
| 1 | `prepare_evidence` | PDF 文件 | 内部工具 | `document_profile` `page_index` | Step 2 | 停止执行 |
| 2 | `build_research_plan` | `document_profile` `page_index` | `planning_crew` | `research_scope` `question_tree` `evidence_map_seed` | Step 3 | Step 2 |
| 3 | `run_research_crew` | `research_scope` `question_tree` `evidence_registry` | `research_crew` | `history_governance_pack` `industry_pack` `business_pack` `finance_pack` `risk_pack` | Step 4 | Step 3 |
| 4 | `review_research_gate` | research 阶段全部输出 `question_tree` `evidence_registry` | `qa_crew` | `coverage_report_research` `qa_report_research` | Step 5 | Step 3 |
| 5 | `run_valuation_crew` | `finance_pack` `business_pack` `risk_pack` `question_tree` `evidence_registry` | `valuation_crew` | `peers_pack` `valuation_pack` | Step 6 | Step 5 |
| 6 | `review_valuation_gate` | valuation 阶段全部输出 `question_tree` `evidence_registry` | `qa_crew` | `coverage_report_valuation` `qa_report_valuation` | Step 7 | Step 5 |
| 7 | `run_investment_thesis_crew` | 全部研究/估值 pack | `investment_thesis_crew` | `investment_thesis` `diligence_questions` | Step 8 | Step 7 |
| 8 | `review_thesis_gate` | thesis 阶段全部输出 `question_tree` `evidence_registry` | `qa_crew` | `coverage_report_thesis` `qa_report_thesis` | Step 9 | Step 7 |
| 9 | `publish_if_passed` | 全部 pack `investment_thesis` `diligence_questions` `qa_report_thesis` | `writeup_crew` | `final_report.md` `final_report.pdf` | 结束 | Step 8 |

### 4.2 迭代规则表

| Loop 名称 | 生产节点 | 审核节点 | 通过条件 | 不通过后的动作 | 退出条件 |
|---|---|---|---|---|---|
| `research_loop` | Step 3 | Step 4 | `coverage_report_research.status = pass` 且 `qa_report_research.status = pass` | 回到 Step 3，按 `coverage_report_research` 和 `qa_report_research` 补研究缺口 | 通过 gate 或人工终止 |
| `valuation_loop` | Step 5 | Step 6 | `coverage_report_valuation.status = pass` 且 `qa_report_valuation.status = pass` | 回到 Step 5，按估值口径冲突、假设缺口、证据不足重新计算 | 通过 gate 或人工终止 |
| `thesis_loop` | Step 7 | Step 8 | `coverage_report_thesis.status = pass` 且 `qa_report_thesis.status = pass` | 回到 Step 7，补结论闭环、补尽调问题、修正逻辑冲突 | 通过 gate 或人工终止 |

## 5. 证据地图 / Evidence Registry 规则

### 6.1 角色分工

| 组件 | 负责什么 | 不负责什么 |
|---|---|---|
| `page_index_builder` | 生成 PDF 页级索引 | 不直接生成完整证据地图 |
| `ResearchPlanner` | 基于 `page_index` 初始化第一版问题树和初始证据地图 | 不要求一开始覆盖全部子问题 |
| `research_crew` 与 `valuation_crew` | 在研究过程中新增子问题、补充证据、标记冲突和 gap | 不应脱离证据地图随意写结论 |
| `evidence_registry` | 存储问题、证据、链接关系、冲突、缺口、状态 | 不负责做投资判断 |
| `CoverageReviewer` | 检查问题覆盖率和证据闭环情况 | 不替代前序 agent 做实质分析 |

### 6.2 标准动作

| 动作 | 谁可以做 | 结果 |
|---|---|---|
| `seed_question` | `planning_crew` `research_crew` `valuation_crew` | 创建一级问题 |
| `register_evidence` | `planning_crew` `research_crew` `valuation_crew` `qa_crew` | 新增一条证据对象 |
| `link_evidence_to_question` | `planning_crew` `research_crew` `valuation_crew` `qa_crew` | 把证据挂到某个问题下 |
| `add_discovered_question` | `research_crew` `valuation_crew` | 研究中发现新子问题并挂到问题树 |
| `mark_conflict_or_gap` | `research_crew` `valuation_crew` `qa_crew` | 标记证据冲突或缺口 |
| `close_or_defer_question` | `qa_crew` | 标记问题已闭环或暂缓处理 |

### 5.3 证据地图样式表

| Column | 是否必填 | 含义 | 备注 |
|---|---|---|---|
| `question_id` | 是 | 问题唯一标识 | 建议稳定、可引用 |
| `parent_question_id` | 否 | 父问题 ID | 一级问题可为空 |
| `question_level` | 是 | 问题层级 | 建议 `L1/L2/L3` |
| `question_title` | 是 | 问题短标题 | 用于快速扫描 |
| `question_text` | 是 | 问题完整表述 | 便于后续 agent 理解 |
| `question_origin` | 是 | 问题来源 | 建议值：`seeded` `discovered` |
| `owner_crew` | 是 | 当前主负责 crew | 建议值：`planning_crew` `research_crew` `valuation_crew` `qa_crew` |
| `target_pack` | 是 | 该问题主要服务的产物 | 例如 `industry_pack` |
| `priority` | 是 | 优先级 | 建议值：`high` `medium` `low` |
| `status` | 是 | 当前处理状态 | 见下方状态表 |
| `evidence_needed` | 是 | 需要什么类型证据 | 例如“PDF 页码、更多公开数据、管理层资料” |
| `supporting_evidence_ids` | 否 | 支持该问题的证据 ID 列表 | 可为空数组 |
| `conflicting_evidence_ids` | 否 | 与当前结论冲突的证据 ID 列表 | 可为空数组 |
| `gap_note` | 否 | 当前缺口描述 | 没有缺口可为空 |
| `next_action` | 是 | 下一步动作 | 指导下一轮 loop |
| `last_updated_at` | 是 | 最近更新时间 | 便于 QA 审核 |

### 5.4 问题状态建议

| 状态 | 含义 |
|---|---|
| `open` | 问题已创建，尚未正式推进 |
| `in_progress` | 正在补证据 |
| `supported` | 证据基本充分 |
| `conflicted` | 证据冲突待澄清 |
| `gap` | 明显缺证据 |
| `deferred` | 本轮暂不展开 |
| `closed` | 已完成闭环 |

## 修订备注

你可以直接在每个表格里改：
- 命名是否顺手
- 是否需要增删 Crew
- Agent 是否还要继续合并
- Tool 是否要继续抽象
- Flow 是否要改成更多或更少的阶段
- `evidence_registry` 要不要再拆成更细的工具
