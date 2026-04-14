# Project-Specific Continuation Note

Before starting work on this repository, read `PROJECT_HANDOFF.md`.
This repository no longer keeps `PROJECT_CONVERSATIONS/` in-tree, so do not assume the archive directory exists.

# AGENTS.md - automated_research_report_generator v0.2

## 第一性原则

1. 先确认问题的最小真实边界，再动代码，不要靠猜。
2. 优先看公开官方文档，尤其是 CrewAI 文档和 changelog。
3. 处理文件时默认按 UTF-8 读写，避免中英文内容损坏。
4. 新增或修改 Python 代码时，继续保持中文注释可读、直接、贴近代码。

## 当前仓库状态

- 当前 Python 包前缀固定为 `automated_research_report_generator`。
- 当前主包目录固定为 `src/automated_research_report_generator/`。
- 当前项目版本来自 `pyproject.toml`：`0.2.0`。
- 当前 CrewAI 依赖固定为 `crewai[file-processing,google-genai,litellm,tools]==1.14.1`。
- 当前项目类型是 Flow 项目，`[tool.crewai].type = "flow"`。
- 当前 GitHub 远端仓库名仍是 `automated_research_report_generator_v0.1`，不要把远端名字和本地包路径混淆。
- `pdf/` 和 `output/` 目录是正式产物目录，当前仓库允许跟踪它们。
- `.env`、`.venv/`、`.cache/`、`logs/` 和本地临时目录仍然应保持不入库。

## 当前目录约定

### 1. Flow 相关模块

凡是直接服务于 Flow 编排、Flow 状态、Flow 预处理、Flow 中间产物管理的 Python 模块，都放在：

`src/automated_research_report_generator/flow/`

当前核心模块包括：

- `common.py`
- `document_metadata.py`
- `models.py`
- `pdf_indexing.py`
- `registry.py`
- `research_flow.py`

### 2. 入口层

- 命令行和脚本入口保留在 `src/automated_research_report_generator/main.py`
- 模型配置入口保留在 `src/automated_research_report_generator/llm_config.py`
- `main.py` 只负责参数解析、默认输入和调用 Flow，不承载具体业务编排细节

### 3. Crew 层

Crew 定义统一放在：

`src/automated_research_report_generator/crews/`

当前包含：

- `history_background_crew`
- `industry_crew`
- `business_crew`
- `peer_info_crew`
- `financial_crew`
- `operating_metrics_crew`
- `risk_crew`
- `valuation_crew`
- `investment_thesis_crew`
- `writeup_crew`

补充说明：

- 当前主 Flow 使用的是 7 个 research sub-crews。

### 4. Tool 层

工具统一放在：

`src/automated_research_report_generator/tools/`

Flow 与 Crew 都通过 `tools/` 访问工具，不要把工具逻辑重新散落回 `flow/` 或 `crews/`。

### 5. 测试层

测试统一放在：

`test_src/`

当前已有：

- Flow gate 测试
- PDF indexing 测试
- registry / tools 测试
- Tushare 工具测试
- 运行时缓存清理测试

## 当前导入规则

1. 不要再从包根目录导入这些旧路径：
   - `automated_research_report_generator.common`
   - `automated_research_report_generator.document_metadata`
   - `automated_research_report_generator.models`
   - `automated_research_report_generator.pdf_indexing`
   - `automated_research_report_generator.registry`
   - `automated_research_report_generator.research_flow`
2. 现在统一改为从 `automated_research_report_generator.flow` 下导入。
3. 新增 Flow 相关模块时，先判断它是不是“被 Flow 直接调用或共享的流程级模块”。
   - 如果是，放进 `flow/`
   - 如果是 Crew 定义，放进 `crews/`
   - 如果是工具，放进 `tools/`

## CrewAI 新鲜度检查

在修改任何 CrewAI 相关代码前，先做下面几件事：

1. 检查本地安装版本：
   - `uv run python -c "import crewai; print(crewai.__version__)"`
2. 检查 PyPI 最新版本：
   - `https://pypi.org/pypi/crewai/json`
3. 看官方 changelog：
   - `https://docs.crewai.com/en/changelog`
4. 按修改内容查看对应概念文档：
   - `https://docs.crewai.com/en/concepts/agents`
   - `https://docs.crewai.com/en/concepts/tasks`
   - `https://docs.crewai.com/en/concepts/flows`
   - `https://docs.crewai.com/en/concepts/tools`
5. 如果本文件和官方文档冲突，以官方文档为准，再反向更新本文件。

截至本次整理时，本地 `crewai` 版本是 `1.14.1`。

## 当前 Flow 真实行为

1. `prepare_evidence`
   - 识别 PDF 元数据
   - 生成页面主题索引
   - 初始化 evidence registry
2. `build_research_plan`
3. `run_research_crew`
   - 顺序执行 7 个 research sub-crews
   - 产出 `history_background`、`industry`、`business`、`peer_info`、`finance`、`operating_metrics`、`risk` 七个 pack
4. `run_valuation_crew`
   - research 完成后直接进入 valuation
   - 产出 `peers_pack`、`intrinsic_value_pack`、`valuation_pack`
   - 不再走外部 valuation QA gate
5. `run_investment_thesis_crew`
   - 不再走外部 thesis QA gate
6. `publish_if_passed`
   - writeup 最后一节使用 research 内部校验摘要，而不是外部 QA gate 结果

当前校验规则：

- `research` 阶段不再保留外部 QA gate
- 7 个 research sub-crews 各自保留内部 `check_registry` 任务
- Flow 会把 7 个 `check_registry` 输出汇总成 `08_research_internal_registry_checks.md`
- `check_registry` 的 `ready/not_ready` 只用于内部校验摘要和结论边界，不阻断 valuation / thesis / writeup
- `valuation_crew` 的 QA 继续留在 crew 内部自校验
- thesis 阶段不再设置外部 QA gate

## 当前运行产物和日志

单次 run 的关键产物：

- 运行缓存：`.cache/<run_slug>/`
- 中间产物目录：`.cache/<run_slug>/md/`
- 证据注册表：`.cache/<run_slug>/md/registry/evidence_registry.json`
- Registry Markdown 快照：`.cache/<run_slug>/md/registry/registry_snapshot.md`
- Registry 历史快照：`.cache/<run_slug>/md/registry/snapshots/`
- Checkpoints：`.cache/<run_slug>/md/checkpoints/cpXX_*.json`
- 运行索引：`.cache/<run_slug>/md/run_manifest.json`
- 最终 Markdown：`.cache/<run_slug>/md/<pdf_stem>_v2_report.md`
- 最终 PDF：`.cache/<run_slug>/md/<pdf_stem>_v2_report.pdf`

单次 run 日志：

- `.cache/<run_slug>/logs/preprocess.txt`
- `.cache/<run_slug>/logs/flow.txt`
- `.cache/<run_slug>/logs/history_background_crew.txt`
- `.cache/<run_slug>/logs/industry_crew.txt`
- `.cache/<run_slug>/logs/business_crew.txt`
- `.cache/<run_slug>/logs/peer_info_crew.txt`
- `.cache/<run_slug>/logs/financial_crew.txt`
- `.cache/<run_slug>/logs/operating_metrics_crew.txt`
- `.cache/<run_slug>/logs/risk_crew.txt`
- `.cache/<run_slug>/logs/valuation_crew.txt`
- `.cache/<run_slug>/logs/investment_thesis_crew.txt`
- `.cache/<run_slug>/logs/writeup_crew.txt`

说明：

- 当前代码里没有项目级 `latest_run.json` 这套 latest 索引
- `run_manifest.json` 会写当前 run 的路径索引
- `flow`、各个 sub-crew、valuation、thesis、writeup 的日志都可以分开看

## 当前环境与工具约定

- LLM 配置统一从 `src/automated_research_report_generator/llm_config.py` 取
- 当前模型供应商是 OpenRouter，核心环境变量是 `OPENROUTER_API_KEY`
- 多个 research sub-crews 的公开资料搜索通常需要 `SERPER_API_KEY`
- A 股估值工具 `TushareValuationDataTool` 需要 `TUSHARE_TOKEN`
- PDF 页面索引并发默认值当前是 `100`
  - 可通过 `PDF_INDEX_MAX_CONCURRENCY` 覆盖
- 页面索引重试次数默认值当前是 `2`
  - 可通过 `PDF_INDEX_RETRY_LIMIT` 覆盖

## 注释与编码要求

### 编码

- 默认按 UTF-8 处理文本文件。
- 修改含中文的 Markdown、Python、YAML 文件时，先确认不会把编码写坏。

### 注释

- 所有 `class` 和 `def` 都要有中文注释。
- 注释尽量包含这五个模块：
  - 目的
  - 功能
  - 实现逻辑
  - 可调参数
  - 默认参数及原因
- 注释要简单直白，不要故作复杂。

## 运行方式

安装依赖：

```bash
uv sync
```

运行主 Flow：

```bash
crewai run
```

或直接指定 PDF：

```bash
uv run python -m automated_research_report_generator.main --pdf <PDF路径>
```

生成 Flow 图：

```bash
uv run python -m automated_research_report_generator.main --plot
```

## 修改后的最低检查

每次做完结构调整，至少检查这几件事：

1. 所有受影响的 `from import` 是否已经同步更新。
2. `main.py` 是否还能正确导入 `ResearchReportFlow`。
3. `crews/`、`tools/`、`test_src/` 中是否还残留旧导入路径。
4. 至少做一次导入级 smoke test，确认包路径没有断。
5. 如果改动了日志、人审或运行目录，要同步检查 `README.md` 和 `PROJECT_HANDOFF.md` 是否还准确。

## 恢复工作时的优先顺序

1. 读 `AGENTS.md`
2. 读 `PROJECT_HANDOFF.md`
3. 看 `git status --short --branch`
4. 看 `pyproject.toml`
5. 看 `README.md`
6. 再决定是否需要看 `design_docs/` 下的设计讨论文档
