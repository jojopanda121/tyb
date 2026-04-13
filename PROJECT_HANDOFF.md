# Project Handoff

## 目的

这份文件给后续 agent 会话做续接说明。
先读 `AGENTS.md`，再读本文件，再决定下一步操作。

本版 handoff 更新时间：`2026-04-11`。
它反映当前工作区真实结构，而不是更早那套带 planning crew 的设计稿。

## 仓库快照

- 本地目录：`D:\pyproject\automated_research_report_generator_v0.2`
- 当前分支：`main-v0.2`
- 当前 `HEAD`：`35c3c8f5b9370bc0d8459fa113e85c373bca5491`
- 最近提交：`35c3c8f Document planner refactor and align writeup outline`
- 远端仓库：`https://github.com/tybzhw1230-boop/automated_research_report_generator_v0.1`
- 当前包名：`automated_research_report_generator`
- 当前版本：`0.2.0`
- 当前 CrewAI 依赖：`crewai[file-processing,google-genai,litellm,tools]==1.14.1`
- 当前项目类型：`[tool.crewai].type = "flow"`

## 当前真实工作状态

当前工作区不是干净状态，文档、research sub-crew 配置和 flow/registry 相关文件都还在持续收敛。
不要把最近一次提交当成“唯一真相”，也不要把旧设计稿当成当前接口。

当前已经明确落地的变化有：

- `planning_crew` 已经移除
- planning 已改成固定模板初始化，不再生成 `research_scope`、`question_tree`、`seed_evidence_map`
- registry 已切到新的 `entry` 字段体系
- research 阶段改为内部 `check_registry` 汇总，不再走外部 QA gate
- 运行产物主路径已经切到 `.cache/<run_slug>/`

继续工作前，必须先区分三件事：

- 当前 `HEAD` 代表什么
- 当前 working tree 多了什么
- 哪些 design doc 只是历史讨论，不是现行结构

## 当前代码的真实目录约定

以 working tree 当前代码为准，目录边界已经稳定为：

- 入口：`src/automated_research_report_generator/main.py`
- LLM 配置：`src/automated_research_report_generator/llm_config.py`
- Flow 层：`src/automated_research_report_generator/flow/`
- Crew 层：`src/automated_research_report_generator/crews/`
- Tool 层：`src/automated_research_report_generator/tools/`
- 测试层：`test_src/`

`flow/` 下当前核心模块：

- `common.py`
- `document_metadata.py`
- `models.py`
- `pdf_indexing.py`
- `registry.py`
- `research_flow.py`
- `config/registry_template.yaml`

`crews/` 下当前真实存在的目录：

- research sub-crews
  - `history_background_crew`
  - `industry_crew`
  - `business_crew`
  - `peer_info_crew`
  - `financial_crew`
  - `operating_metrics_crew`
  - `risk_crew`
- 后续阶段 crews
  - `valuation_crew`
  - `investment_thesis_crew`
  - `writeup_crew`

当前不再存在：

- `src/automated_research_report_generator/crews/planning_crew/`
- `src/automated_research_report_generator/crews/qa_crew/`
- `src/automated_research_report_generator/crews/research_subcrew_base.py`

## 当前接口边界

当前只应把下面这些当作真实接口：

- Flow 主类：`ResearchReportFlow`
- registry 模型：`fact/data/judgment` + `single/table` + `unchecked/checked/need_revision`
- registry 工具：
  - `add_entry`
  - `update_entry`
  - `add_evidence`
  - `status_update`
  - `registry_review`
  - `read_registry`
- 主入口参数：
  - `--pdf`
  - `--plot`

下面这些旧接口已经退出主路径，不要再在文档、prompt 或测试假设里当成当前实现：

- `planning_crew`
- `research_scope`
- `question_tree`
- `seed_evidence_map`
- `RegistrySeedPlan`
- `RegistrySeedTool`
- 旧 registry 状态：`open`、`supported`、`gap`、`conflicted`、`deferred`、`closed`
- 项目级 `latest_run.json`

## 当前 Flow 行为摘要

当前主流程是：

1. `prepare_evidence`
   - 识别 PDF 元数据
   - 创建 run 目录
   - 生成页索引
   - 初始化 registry
2. `build_research_plan`
   - 读取 `flow/config/registry_template.yaml`
   - 用 `{company_name}`、`{industry}` 做占位符替换
   - 把模板条目写回当前 run 的 registry
3. `run_research_crew`
   - 顺序执行 7 个 research sub-crews
   - 产出 7 个 research packs
   - 汇总 7 个 `check_registry` 输出，生成 `08_research_internal_registry_checks.md`
4. `run_valuation_crew`
   - research 完成后直接进入 valuation
   - 产出 `peers_pack`、`intrinsic_value_pack`、`valuation_pack`
5. `run_investment_thesis_crew`
   - 产出 `investment_thesis`、`diligence_questions`
   - 可读取完整 registry 快照
6. `publish_if_passed`
   - 先由 flow 确定性拼装最终 Markdown
   - 主文完整纳入 thesis、7 个 research packs、3 个 valuation packs 和内部校验摘要
   - 末尾以附录方式追加 `registry_snapshot.md`
   - 再调用 writeup crew 做非破坏性确认与 PDF 导出

当前校验规则：

- `research` 阶段不再保留外部 gate
- 7 个 research sub-crews 各自保留内部 `check_registry` 任务
- 7 个 research sub-crews 在 crew 内统一使用 `Process.sequential`，不再依赖 manager agent 转派任务
- Flow 会把 7 个 `check_registry` 输出汇总成 `08_research_internal_registry_checks.md`
- `check_registry` 的 `ready/not_ready` 只用于内部校验摘要和结论边界，不阻断 valuation / thesis / writeup
- `check_registry` 优先返回结构化 JSON；结构化结果缺失时，Flow 会降级到原始 memo 解析并记录 warning
- valuation 和 thesis 的校验都已经内收到各自 crew 或下游综合阶段

## 当前运行时真相

单次运行根目录：

- `.cache/<run_slug>/`

其中的关键目录：

- `indexing/`
  - 文档元数据和页索引都落在这里
- `logs/`
  - `preprocess.txt`
  - `flow.txt`
  - 各 crew 独立日志
- `md/`
  - `research/iter_XX/`
  - `valuation/iter_XX/`
  - `thesis/iter_XX/`
  - `registry/`
  - `checkpoints/`
  - `run_manifest.json`
  - 最终 `.md` 与 `.pdf`

关键文件：

- 证据注册表：`.cache/<run_slug>/md/registry/evidence_registry.json`
- registry 快照：`.cache/<run_slug>/md/registry/registry_snapshot.md`
- registry 历史快照：`.cache/<run_slug>/md/registry/snapshots/`
- checkpoints：`.cache/<run_slug>/md/checkpoints/cpXX_*.json`
- 运行索引：`.cache/<run_slug>/md/run_manifest.json`
- 最终报告：`.cache/<run_slug>/md/<pdf_stem>_v2_report.md`
- 最终 PDF：`.cache/<run_slug>/md/<pdf_stem>_v2_report.pdf`

补充说明：

- 当前正式运行主路径不是仓库根目录 `output/`
- 当前代码里没有项目级 `logs/latest_run.json` 或类似 latest 索引
- `run_manifest.json` 已经承担单轮运行索引职责

## 文档状态

当前优先信任这些文档：

- `AGENTS.md`
- `PROJECT_HANDOFF.md`
- `README.md`
- `design_docs/项目信息传递链路全面分析.md`
- `design_docs/CREW_REFACTOR_WORKING_DRAFT.md`

这些文档如果发生以下变更，必须同步更新：

- Flow 主链路变化
- registry 字段体系变化
- 运行目录或产物目录变化
- crew 目录结构变化
- 旧接口被彻底删除或重新引入

`design_docs/next_step_20260410.md` 当前属于路线图，不代表已经落地。

## 建议的恢复顺序

后续会话继续工作时，建议按这个顺序恢复：

1. 读 `AGENTS.md`
2. 读本文件
3. 看 `git status --short --branch`
4. 分开看 `git diff --cached` 和 `git diff`
5. 看 `pyproject.toml`
6. 看 `README.md`
7. 看 `design_docs/项目信息传递链路全面分析.md`
8. 如果要处理后续设计方向，再看 `design_docs/next_step_20260410.md`
9. 如涉及结构调整，先做一次导入级 smoke test，确认 `automated_research_report_generator.flow` 路径没有断

## 本次 handoff 的边界

这次 handoff 之后，当前已确认的边界是：

- deterministic planner 已经落地，模板文件是 `flow/config/registry_template.yaml`
- 7 个 research sub-crews、统一 registry entry 模型、research 内部校验摘要和 checkpoint 机制已经落地
- registry/tool 链路上的旧 question-style 兼容接口已移除，当前以 `entry` 命名为准
- `planning_crew` 已删除，不应再作为当前结构展示
