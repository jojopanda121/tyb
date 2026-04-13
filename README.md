# Automated Research Report Generator v0.2

一个基于 CrewAI Flow 的买方研究报告生成项目。当前仓库的真实主线已经切到 `v0.2` 的 registry-centric 结构：planning 不再依赖单独的 planning crew，而是用固定 YAML 模板初始化 research registry；7 个 research sub-crews 围绕统一 registry 工作；research 完成后直接进入 valuation，并把各 pack 的 `check_registry` 输出汇总成内部校验摘要。

## 当前实现边界

- 当前项目版本：`0.2.0`
- 当前 CrewAI 依赖：`crewai[file-processing,google-genai,litellm,tools]==1.14.1`
- 当前项目类型：`[tool.crewai].type = "flow"`
- 当前主入口：`src/automated_research_report_generator/main.py`
- 当前主 Flow：`src/automated_research_report_generator/flow/research_flow.py`
- 当前包路径：`src/automated_research_report_generator/`
- 当前默认模型供应商：OpenRouter
- 当前远端仓库名仍是 `automated_research_report_generator_v0.1`

## 当前设计原则

- planning 只做确定性初始化：`build_research_plan` 直接加载 `flow/config/registry_template.yaml`，不再生成独立 planning 产物。
- registry 是研究主接口：research、valuation、thesis 和 writeup 都围绕 `evidence_registry.json` 协作。
- research 内部自校验：research 不再经过外部 gate，而是汇总 7 个 sub-crew 的 `check_registry` 输出来形成内部校验摘要。
- research 子 crew 调度：7 个 research sub-crews 在 crew 内统一使用 `Process.sequential`，不再依赖 manager agent 做任务转派。
- `check_registry` 输出契约：优先输出结构化 JSON 并由 Flow 直接汇总 `ready/not_ready` 供内部校验摘要使用；若结构化结果缺失，则降级到原始 memo 解析并记录 warning，且不阻断下游阶段。
- 运行目录按 run 隔离：单次运行统一写入 `.cache/<run_slug>/`，方便排查单轮产物、日志和快照。

## 当前 Flow 链路

1. `prepare_evidence`
   - 识别 PDF 元数据
   - 在当前 run 下生成页索引
   - 初始化 evidence registry 与 run 目录
2. `build_research_plan`
   - 加载固定 `registry_template.yaml`
   - 替换 `{company_name}` 与 `{industry}` 占位符
   - 用模板重建当前 run 的 research registry
3. `run_research_crew`
   - 顺序执行 7 个 research sub-crews
   - 产出 `history_background`、`industry`、`business`、`peer_info`、`finance`、`operating_metrics`、`risk` 七个 pack
   - 汇总 7 个 `check_registry` 输出，生成 `08_research_internal_registry_checks.md`
4. `run_valuation_crew`
   - research 完成后直接进入 valuation
   - 产出 `peers_pack`、`intrinsic_value_pack`、`valuation_pack`
   - 不再经过外部 valuation gate
5. `run_investment_thesis_crew`
   - 产出 `investment_thesis` 和 `diligence_questions`
   - 可读取完整 registry 快照，但不经过外部 thesis gate
6. `publish_if_passed`
   - 先由 flow 确定性拼装最终 Markdown
   - 主文完整纳入 thesis、7 个 research packs、3 个 valuation packs 和内部校验摘要
   - 末尾以附录方式追加 `registry_snapshot.md`
   - 再调用 writeup crew 做非破坏性确认与 PDF 导出

## 当前 Crew 结构

当前 `crews/` 下只保留这几类目录：

- 7 个 research sub-crews
  - `history_background_crew`
  - `industry_crew`
  - `business_crew`
  - `peer_info_crew`
  - `financial_crew`
  - `operating_metrics_crew`
  - `risk_crew`
- 3 个后续阶段 crews
  - `valuation_crew`
  - `investment_thesis_crew`
  - `writeup_crew`

当前不再有 `planning_crew`，也不再保留共享 `research_subcrew_base.py` 这类中间抽象。

## 当前 Registry 契约

当前 registry 文件位于：

- `.cache/<run_slug>/md/registry/evidence_registry.json`
- `.cache/<run_slug>/md/registry/registry_snapshot.md`

当前 entry 模型只保留现行字段体系：

- `entry_type`: `fact` / `data` / `judgment`
- `content_type`: `single` / `table`
- `status`: `unchecked` / `checked` / `need_revision`
- `topic`: 按 `history`、`industry`、`business`、`peer_info`、`financial`、`operating_metrics`、`risk`、`peers`、`intrinsic_value`、`valuation`、`investment_thesis` 分组
- `owner_crew`: 指向当前真实 crew 名

当前稳定工具接口位于 `src/automated_research_report_generator/tools/`：

- `add_entry`
- `update_entry`
- `add_evidence`
- `status_update`
- `registry_review`
- `read_registry`

## 已移除的旧接口

下面这些旧接口已经不属于当前实现，不应再作为项目说明或 prompt 依赖展示：

- 独立 `planning_crew`
- `research_scope`
- `question_tree`
- `seed_evidence_map`
- `RegistrySeedPlan`
- `RegistrySeedTool`
- 旧 registry 状态语义：`open`、`in_progress`、`supported`、`gap`、`conflicted`、`deferred`、`closed`
- 项目级 `latest_run.json`

## 运行产物与目录结构

单次 run 的真实目录结构如下：

```text
.cache/<run_slug>/
├─ indexing/
│  ├─ <pdf_stem>_document_metadata.json
│  └─ <pdf_stem>_page_index.json
├─ logs/
│  ├─ preprocess.txt
│  ├─ flow.txt
│  ├─ history_background_crew.txt
│  ├─ industry_crew.txt
│  ├─ business_crew.txt
│  ├─ peer_info_crew.txt
│  ├─ financial_crew.txt
│  ├─ operating_metrics_crew.txt
│  ├─ risk_crew.txt
│  ├─ valuation_crew.txt
│  ├─ investment_thesis_crew.txt
│  └─ writeup_crew.txt
└─ md/
   ├─ research/
   │  └─ iter_01/
   ├─ valuation/
   │  └─ iter_01/
   ├─ thesis/
   │  └─ iter_01/
   ├─ registry/
   │  ├─ evidence_registry.json
   │  ├─ registry_snapshot.md
   │  └─ snapshots/
   ├─ checkpoints/
   ├─ run_manifest.json
   ├─ <pdf_stem>_v2_report.md
   └─ <pdf_stem>_v2_report.pdf
```

说明：

- 当前正式运行主路径是 `.cache/<run_slug>/`，不是仓库根目录的 `output/`
- `run_manifest.json` 写在 `md/` 目录
- research、valuation、thesis 都按 `iter_XX` 保留阶段版本
- research 内部校验摘要随 `research/iter_01/08_research_internal_registry_checks.md` 一起落盘
- `latest_run.json` 已经移除，不再维护项目级最新索引

## 仓库目录

```text
.
├─ AGENTS.md
├─ PROJECT_HANDOFF.md
├─ README.md
├─ design_docs/
├─ pdf/
├─ src/
│  └─ automated_research_report_generator/
│     ├─ main.py
│     ├─ llm_config.py
│     ├─ flow/
│     │  ├─ common.py
│     │  ├─ document_metadata.py
│     │  ├─ models.py
│     │  ├─ pdf_indexing.py
│     │  ├─ registry.py
│     │  ├─ research_flow.py
│     │  └─ config/
│     │     └─ registry_template.yaml
│     ├─ crews/
│     └─ tools/
└─ test_src/
```

## 环境要求

- Python `>=3.10,<3.14`
- 建议使用 `uv`
- 当前主要在 Windows 环境下开发和验证

## 环境变量

必需：

- `OPENROUTER_API_KEY`

通常需要：

- `SERPER_API_KEY`
  - 多个 research sub-crews 使用公开资料搜索工具

按场景需要：

- `TUSHARE_TOKEN`
  - A 股估值工具 `TushareValuationDataTool` 需要
- `PDF_INDEX_MAX_CONCURRENCY`
  - 覆盖 PDF 页面索引最大并发，默认 `100`
- `PDF_INDEX_RETRY_LIMIT`
  - 覆盖页面索引失败重试次数，默认 `2`

## 安装

优先使用 `uv`：

```bash
uv sync
```

如果只想使用 `pip`：

```bash
pip install -r requirements.txt
```

## 运行

运行主 Flow：

```bash
crewai run
```

直接指定 PDF：

```bash
uv run python -m automated_research_report_generator.main --pdf <PDF路径>
```

绘制 Flow 图：

```bash
uv run python -m automated_research_report_generator.main --plot
```

## 测试

- 测试文件统一放在 `test_src/`
- 当前已有 flow、registry、tools、Tushare、PDF indexing 和结构约束测试
- `pytest` 当前没有作为默认依赖写入 `pyproject.toml`

运行测试：

```bash
uv run pytest test_src
```

如果只想检查文本编码：

```bash
uv run pytest test_src/test_text_file_encoding.py -q
```

## 文档导航

- 当前项目描述：`README.md`
- 当前续接说明：`PROJECT_HANDOFF.md`
- 当前结构链路说明：`design_docs/项目信息传递链路全面分析.md`
- 当前 crew 结构说明：`design_docs/CREW_REFACTOR_WORKING_DRAFT.md`
- 近期下一步设计方向：`design_docs/next_step_20260410.md`

## 近期设计方向

近期已经明确但尚未完全落地的方向主要集中在 thesis 阶段：

- 提炼当前市场一致预期
- 识别基本面与估值相对市场预期的预期差
- 明确能够收敛预期差的催化剂
- 将 thesis 进一步收敛到更明确的多空辩论式框架

这些内容目前属于路线图，不应误认为已经是当前接口或当前输出结构。
