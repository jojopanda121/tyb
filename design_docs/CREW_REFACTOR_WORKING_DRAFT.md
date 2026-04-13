# Crew 结构现状说明

这个文件不再是旧版重构底稿，而是当前已经落地的 crew / task / tool / flow 结构说明。
如果和更早的讨论稿冲突，以这里和当前代码为准。

## 1. 当前 crew 总览

| Crew | 作用 | 主要输入 | 主要输出 |
|---|---|---|---|
| `history_background_crew` | 公司历史、改制、治理基础研究 | 文档摘要、页索引、registry | `history_background_pack` |
| `industry_crew` | 行业定义、增长、竞争与监管 | 文档摘要、页索引、registry | `industry_pack` |
| `business_crew` | 商业模式、产品矩阵、业务结构 | 文档摘要、页索引、registry | `business_pack` |
| `peer_info_crew` | 可比池与同行信息整理 | 文档摘要、页索引、registry、行业包、业务包 | `peer_info_pack` |
| `financial_crew` | 财务报表与利润质量分析 | 文档摘要、页索引、registry、同行信息包 | `finance_pack` |
| `operating_metrics_crew` | 关键运营指标与趋势分析 | 文档摘要、页索引、registry、同行信息包 | `operating_metrics_pack` |
| `risk_crew` | 关键经营与外部风险识别 | 文档摘要、页索引、registry | `risk_pack` |
| `valuation_crew` | 可比估值、内在价值、综合估值 | 基础输入 + peer_info/finance/ops/risk packs | `peers_pack`、`intrinsic_value_pack`、`valuation_pack` |
| `investment_thesis_crew` | 投资逻辑与尽调问题提炼 | 全部 research packs + peers/valuation + registry | `investment_thesis`、`diligence_questions` |
| `writeup_crew` | 汇编最终 Markdown 与 PDF | 上游 packs + thesis + research 内部校验摘要 | 最终 `.md` 和 `.pdf` |

当前不再存在独立 `planning_crew`。planning 的职责已经收口到 Flow 的 `build_research_plan` 节点。

## 2. research sub-crew 统一模式

当前 7 个 research sub-crews 都遵守同一套骨架：

- 每个 crew 使用独立目录和独立 YAML 配置
- 每个 crew 当前都有 4 个 agent
  - `search_fact_agent`
  - `extract_file_fact_agent`
  - `qa_check_agent`
  - `synthesizing_agent`
- 每个 crew 当前都有 6 个 task
  - `extract_file_facts`
  - `record_extract_registry`
  - `search_facts`
  - `record_search_registry`
  - `check_registry`
  - `synthesize_and_output`

当前统一输入边界：

- `_base_inputs()` 提供基础文档上下文
- Flow 额外注入 `pack_name`、`pack_title`、`owner_crew`、`pack_output_path`、`loop_reason`
- Flow 按 crew 需要补充上游 pack 文本

当前统一行为边界：

- 先按 `owner_crew` 从 registry 读取自己负责的条目
- 优先补 `unchecked` 和 `need_revision` 条目
- 用 `update_entry` 回填已有模板条目
- 必要时追加 `add_entry` 与 `add_evidence`
- 每轮结束必须写 `registry_review`

## 3. 当前 registry 与 crew 的关系

当前 registry 是 crew 协作主接口，不再使用任何 planning 产物文件作为研究驱动。

当前模板入口：

- `src/automated_research_report_generator/flow/config/registry_template.yaml`

当前主字段：

- `entry_type`: `fact` / `data` / `judgment`
- `content_type`: `single` / `table`
- `status`: `unchecked` / `checked` / `need_revision`
- `owner_crew`: 直接对应真实 crew 名

当前稳定工具接口：

- `add_entry`
- `update_entry`
- `add_evidence`
- `status_update`
- `registry_review`
- `read_registry`

当前 `read_registry` 主要视图：

- `markdown`
- `entry_list`
- `full`
- `entry_detail`
- `evidence_detail`

## 4. 当前 Flow 与 crew 的接线方式

当前 Flow 主链路：

1. `prepare_evidence`
2. `build_research_plan`
3. `run_research_crew`
4. `run_valuation_crew`
5. `run_investment_thesis_crew`
6. `publish_if_passed`

当前迭代规则：

- research 不再有外部 gate，固定单轮执行
- valuation 不走外部 gate
- thesis 不走外部 gate

当前目录落盘方式：

- research 产物：`.cache/<run_slug>/md/research/iter_XX/`
- valuation 产物：`.cache/<run_slug>/md/valuation/iter_XX/`
- thesis 产物：`.cache/<run_slug>/md/thesis/iter_XX/`
- research 内部校验摘要：`.cache/<run_slug>/md/research/iter_XX/08_research_internal_registry_checks.md`
- crew 日志：`.cache/<run_slug>/logs/<crew_name>.txt`

## 5. 当前 writeup 边界

当前 writeup 的职责很窄：

- 汇编上游正文
- 保持一级标题结构固定
- 生成最终 Markdown
- 调用 `MarkdownToPdfTool` 导出 PDF

当前 writeup prompt 要求：

- 对上游正文做完整插入
- 不重新摘要
- 不改写事实、数字、结论和风险表述

## 6. 近期设计方向

已经确认但尚未完全落地的方向：

- thesis 阶段进一步提炼市场一致预期
- 明确预期差与催化剂
- 将投资逻辑收敛到更清楚的多空辩论式框架

这部分属于路线图，不应反向污染当前 crew 接口说明。

## 7. 不再展示的旧设计

下面这些旧设计已经退出当前结构说明：

- `planning_crew`
- `research_scope`
- `question_tree`
- `seed_evidence_map`
- 旧 registry 状态语义：`open`、`supported`、`gap`、`conflicted`、`deferred`、`closed`
- 共享 `research_subcrew_base.py`
