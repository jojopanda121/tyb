# 重构计划：Research Crew 拆分 + Registry 重构 + QA 反馈机制

## Context

### 当前架构核心问题
- QA 反馈无法定向传导，gate 失败后重跑全部 5 个 agent
- Registry JSON 格式对 LLM 不友好，agent 依从性差
- Registry 记录仅有 judgment，需要同时包含事实、数据、判断
- QA 输出趋同，三个 gate 传入相同完整 registry 快照
- Valuation crew 中 peer_info（选同行+拉数据）应前移到 research flow
- 缺少 operating_metrics 独立维度

### 用户确认的设计决策
1. **Research 拆为 7 个 sub-crew**（含 peer_info_crew + operating_metrics_crew）
2. **每个 sub-crew 使用 Hierarchical Process**（manager 协调 4 agent：search_fact、extract_file_fact、qa_check、synthesizing）
3. **Registry: JSON 存储 + MD 视图**（保留 Pydantic 校验，ReadRegistryTool 输出 MD）
4. **Registry 字段丰富化**（不拆分成三种模型，但扩展字段以包含事实/数据/判断）
5. **跨 crew 问题路由: Registry 异步路由**（risk_crew 写入 registry → QA gate 检测 → 定向重跑 business_crew）
6. **QA 精简**：
   - Research: 7 个 sub-crew 完成后 1 个 QA gate（1 agent 1 task，只做跨 pack 一致性+覆盖度）
   - Valuation: Hierarchical manager 内部 QA，无外部 gate
   - Thesis: 取消 QA gate
7. **Valuation 拆分**：peer_info_crew 融入 research flow（business 之后，financial 之前），valuation_crew 专注估值计算
8. **接受 2-3x token 成本增幅**

---

## 交付物

`design_docs/CREW_REFACTOR_WORKING_v2.md` — 完整中文重构设计文档

---

## 一、新 Flow 主管线架构

```
prepare_evidence
    ↓
build_research_plan (PlanningCrew, Sequential)
    ↓
┌─── Research Sub-Crews (7个, 每个 Hierarchical Process) ────┐
│  1. history_background_crew                                │
│  2. industry_crew                                          │
│  3. business_crew                                          │
│  4. peer_info_crew  ← NEW (选同行+拉peer数据)               │
│  5. financial_crew                                         │
│  6. operating_metrics_crew  ← NEW                          │
│  7. risk_crew                                              │
└────────────────────────────────────────────────────────────┘
    ↓
research_qa_gate (1 agent, 1 task: 跨 pack 一致性+覆盖度)
    |── revise → 定向重跑 affected sub-crew(s)
    |── pass ↓
run_valuation_crew (Hierarchical, manager 含内部 QA)
    ↓  (无外部 QA gate)
run_investment_thesis_crew (保持 Sequential, 2 agent)
    ↓  (无 QA gate)
publish_if_passed (WriteupCrew, 保持 Sequential)
```

### Research Sub-Crew 执行顺序与信息依赖

| 序号 | Sub-Crew | 前置依赖 | 产出 |
|---|---|---|---|
| 1 | history_background_crew | 无 | history_background_pack.md |
| 2 | industry_crew | 无 | industry_pack.md |
| 3 | business_crew | 无 | business_pack.md |
| 4 | peer_info_crew | industry + business | peer_info_pack.md（同行列表+peer财务数据+peer估值倍数）|
| 5 | financial_crew | peer_info | finance_pack.md |
| 6 | operating_metrics_crew | peer_info | operating_metrics_pack.md |
| 7 | risk_crew | 无 | risk_pack.md |

---

## 二、每个 Research Sub-Crew 内部设计 (Hierarchical Process)

### 2.1 通用 4-Agent 结构（Hierarchical Process）

每个 sub-crew 采用 `Process.hierarchical`，包含 1 个 manager + 4 个工作 agent：

> **CrewAI 限制**：manager agent 不能使用任何工具（CrewAI 源码强制清空并抛异常），只有 `DelegateWorkTool` 和 `AskQuestionTool` 两个内置委托工具。因此通过增加 crew 内部的 `qa_check_agent` 来代替 manager 执行 registry 检查。

| Agent | 角色 | 工具 | 说明 |
|---|---|---|---|
| **manager** | 协调分配任务，决定 agent 执行顺序 | _(无，CrewAI 自动注入 DelegateWorkTool + AskQuestionTool)_ | 通过委派和提问协调 4 个 agent；重试时将 QA 反馈分配给 search/extract agent |
| **search_fact_agent** | 网络搜索、回答 registry 问题、发现新问题 | SerperDevTool, ReadRegistryTool, AddJudgmentTool, AddEvidenceTool, StatusUpdateTool, RegistryReviewTool | 由 manager 委派；重试时也处理 QA 反馈中的搜索类缺口 |
| **extract_file_fact_agent** | PDF 分析、回答 registry 问题、发现新问题 | ReadPdfPageIndexTool, ReadPdfPagesTool, ReadRegistryTool, AddJudgmentTool, AddEvidenceTool, StatusUpdateTool, RegistryReviewTool | 由 manager 委派；重试时也处理 QA 反馈中的文件提取类缺口 |
| **qa_check_agent** | 检查 registry 完成度，验证 search/extract 是否更新了 registry，标记遗漏 | ReadRegistryTool, StatusUpdateTool, RegistryReviewTool | 在 search 和 extract 完成后由 manager 委派；代替 manager 执行 registry 状态检查 |
| **synthesizing_agent** | 综合分析、标记冲突（含冲突程度：minor/major）和未解决项、写出分析包 | ReadRegistryTool, RegistryReviewTool, StatusUpdateTool | 由 manager 委派（最后执行）|

> **与之前的 answering_qa_agent 区别**：qa_check_agent 的职责是 **crew 内部 registry 完成度检查**（确保 search/extract 更新了 registry），不是回答外部 QA gate 的问题。外部 QA 反馈在重试时由 search/extract agent 直接处理。

### 2.2 各 Sub-Crew 差异化配置

| Sub-Crew | 特殊工具 | 特殊说明 |
|---|---|---|
| history_background_crew | — | PDF提取信息高于搜索权重 |
| industry_crew | — | 需要外部行业数据搜索 |
| business_crew | — | 信息密度最高 |
| **peer_info_crew** | **TushareValuationDataTool, ComparableValuationTool** | 选同行+拉取peer财务和估值数据 |
| financial_crew | FinancialModelTool | 专注 PDF 提取和计算，对于输入文件没有的财务数据，可以用搜索填上 |
| operating_metrics_crew | SerperDevTool（搜索运营指标） | 只用搜索工具查找运营指标，搜不到也可以。|
| risk_crew | — | 需要外部验证搜索 |

### 2.3 Hierarchical Manager 的职责边界

Manager 负责（仅通过 DelegateWorkTool / AskQuestionTool）：
- 将 search_facts task 委派给 search_fact_agent
- 将 extract_file_facts task 委派给 extract_file_fact_agent
- 将 check_registry task 委派给 qa_check_agent（检查 registry 完成度）
- 将 synthesize_and_output task 委派给 synthesizing_agent
- 重试时将 QA 反馈内容传递给 search/extract agent
- 可以通过 AskQuestionTool 向任何 agent 提问确认进度

Manager 不能做：
- 直接使用任何工具（CrewAI 强制限制）
- 直接读取 registry（由 qa_check_agent 代替）
- 跨 pack 一致性检查（由外部 QA gate 负责）
- 跨 crew 问题路由（由 registry 异步路由机制负责）

### 2.4 Task 定义

每个 sub-crew 定义 4 个 task（Hierarchical 由 manager 委派给合适 agent）：

1. **search_facts**: 网络搜索相关信息，回答 registry 中 target_pack 对应的问题，发现新问题写入 registry（重试时同时处理 QA 反馈中的搜索类缺口）
2. **extract_file_facts**: PDF 文件分析提取，回答 registry 问题，发现新问题写入 registry（重试时同时处理 QA 反馈中的文件提取类缺口）
3. **check_registry**: 读取 registry 当前状态，检查 search 和 extract 是否正确更新了 registry，标记遗漏项，补充 gap_note 和 next_action（代替 manager 不能做的 registry 检查）
4. **synthesize_and_output**: 综合 search 和 extract 的发现，进行定性分析，标记冲突及冲突程度（minor/major），标记未解决项，输出分析包 MD 文件

---

## 三、Registry 重构设计

### 3.1 存储方案: JSON 底层 + MD 视图

- **底层存储**: 保留 `evidence_registry.json`，保留 Pydantic 校验和线程锁
- **Agent 读取**: `ReadRegistryTool` 输出渲染为 Markdown 表格
- **Agent 写入**: 通过现有工具（AddJudgmentTool, AddEvidenceTool 等）操作 JSON
- **人类查看**: 自动生成 `registry_snapshot.md` 快照

### 3.2 Registry 字段扩展

在现有 `QuestionRecord` 基础上扩展，不拆分为三种独立模型：

```python
class RegistryEntry(BaseModel):
    # === 通用字段 ===
    entry_id: str               # 唯一标识
    entry_type: Literal["fact", "data", "judgment"]  # 类型标签
    title: str                  # 短标题
    content: str                # 事实描述 / 数值含义 / 判断陈述
    target_pack: str            # 归属分析包
    owner_crew: CrewOwner       # 负责 crew
    priority: QuestionPriority  # high/medium/low
    status: str                 # open/in_progress/supported/conflicted/gap/confirmed/closed
    conflict_severity: Literal["none", "minor", "major"] = "none"
                                # none=无冲突, minor=略微冲突(可接受), major=严重冲突(需提示)
    source_ref: str = ""        # 来源引用（如"招股书P245"）
    gap_note: str = ""          # 缺口说明
    next_action: str = ""       # 下一步动作
    last_updated_at: str

    # === 数据专用字段（entry_type="data" 时使用）===
    value: str = ""             # 数值（字符串避免精度问题）
    unit: str = ""              # 元、%、万吨
    period: str = ""            # 2024、2024H1
    calibration_note: str = ""  # 口径说明

    # === 判断专用字段（entry_type="judgment" 时使用）===
    parent_entry_id: str | None = None
    entry_level: QuestionLevel = "L1"  # L1/L2/L3
    evidence_needed: str = ""
    supporting_evidence_ids: list[str] = []
    conflicting_evidence_ids: list[str] = []
    context_evidence_ids: list[str] = []
```

### 3.3 Markdown 渲染格式

`ReadRegistryTool` 输出示例：

```markdown
# 证据注册表：{company_name} | {industry}
更新时间：{updated_at}

## 事实 (共 N 条)
| ID | 标题 | 内容 | 来源 | Pack | 状态 |
|---|---|---|---|---|---|
| F-001 | 公司成立时间 | 2010年3月 | fileP3 | history_pack | confirmed |
| F-001 | 创始人名称 | XXX | fileP3 | history_pack | confirmed |

## 数据 (共 N 条)
| ID | 指标 | 值 | 单位 | 期间 | 口径 | Pack | 状态 |
|---|---|---|---|---|---|---|---|
| D-001 | 营业收入 | 12.5 | 亿元 | 2024 | 合并报表 | finance_pack | confirmed |
| F-001 | 行业增速 | 10 | % | 2024-2026 | CAGR | finance_pack | confirmed |

## 判断 (按优先级，共 N 条)
### ⚠ 待补证据 (gap/open/conflicted)
| ID | 标题 | 判断 | Pack | 状态 | 冲突程度 | 缺口 | 下一步 |
|---|---|---|---|---|---|---|---|
| J-003 | 利润质量 | 利润未充分转成现金 | finance_pack | gap | — | 缺CFO数据 | 提取现金流表 |
| J-007 | 客户集中度 | 前五大客户占比>60% | business_pack | conflicted | **major** | file与搜索不一致 | 核实两份来源 |

### ✓ 已支持 (supported/closed)
| ID | 标题 | 判断 | Pack | 证据数 |
|---|---|---|---|---|
| J-001 | 上市前是否经过重大重组 | 是 | history_pack | 3 |
| J-001 | 行业是否高速增长 | 是 | history_pack | 2 |
```

### 3.4 工具变更

| 工具 | 变化 |
|---|---|
| ReadRegistryTool | 输出改为 MD 格式，增加 `filter_entry_type` 参数 |
| AddJudgmentTool | 扩展为 AddEntryTool，支持 entry_type 参数 |
| AddEvidenceTool | 保留不变 |
| StatusUpdateTool | 保留不变 |
| RegistryReviewTool | 保留不变 |
| RegistrySeedTool | 扩展支持三类 entry 的初始化 |

---

## 四、QA 反馈传导机制

### 4.1 新 QA 架构

| 阶段 | QA 方式 | 说明 |
|---|---|---|
| Research (7 sub-crews) | **外部 QA gate**: 1 agent, 1 task | 跨 pack 一致性 + 覆盖度检查 |
| Valuation | **Hierarchical manager 内部 QA** | manager 自行验证估值一致性，无外部 gate |
| Thesis | **无 QA** | 取消 gate |

### 4.2 Research QA Gate 设计

**输入裁剪**：QA 不再接收完整 registry JSON dump，改为接收：
- 按 entry_type 分组的 MD 渲染视图
- 增量变化摘要（与上一轮 snapshot 的 diff）

**输出增强**：`GateReviewOutput` 新增字段：

```python
class GateReviewOutput(BaseModel):
    status: GateStatus            # pass/revise/stop
    summary: str
    key_gaps: list[str]
    priority_actions: list[str]
    affected_packs: list[str]     # NEW: 标记需要重做的 pack
```

### 4.3 跨 Crew 问题路由（Registry 异步路由）

流程：
1. risk_crew agent 发现问题需要 business_crew 回答
2. risk_crew 写入 registry：`entry_id=J-xxx, target_pack="business_pack", status="open", next_action="需要补充..."`
3. 本轮 7 个 sub-crew 全部完成
4. QA gate 读取 registry → 发现 business_pack 有未关闭的 open 问题
5. QA 输出 `affected_packs: ["business_pack"]`
6. Flow 层只重跑 business_crew，注入 QA 反馈
7. business_crew 的 manager 将 QA 反馈分配给 search_fact_agent 和 extract_file_fact_agent 处理

### 4.4 定向重试实现

```python
def _rerun_affected_research_sub_crews(self, affected_packs: list[str], qa_feedback: str):
    pack_to_runner = {
        "history_background_pack": self._run_history_background_crew,
        "industry_pack": self._run_industry_crew,
        "business_pack": self._run_business_crew,
        "peer_info_pack": self._run_peer_info_crew,
        "finance_pack": self._run_financial_crew,
        "operating_metrics_pack": self._run_operating_metrics_crew,
        "risk_pack": self._run_risk_crew,
    }
    for pack in affected_packs:
        if pack in pack_to_runner:
            pack_to_runner[pack](qa_feedback=qa_feedback)
```

---

## 五、Valuation Crew 重构

### 5.1 拆分方案

原 ValuationCrew (3 agents) 拆为：

| Crew | Process | 位置 | Agents | 产出 |
|---|---|---|---|---|
| **peer_info_crew** | Hierarchical | Research flow 第4位 | search_fact_agent, extract_file_fact_agent, qa_check_agent, synthesizing_agent | peer_info_pack.md |
| **valuation_crew** | Hierarchical | Research flow 之后 | peer_valuation_agent, intrinsic_valuation_agent, valuation_synthesizer, (manager 兼 QA) | peers_pack.md, intrinsic_value_pack.md, valuation_pack.md |

### 5.2 peer_info_crew 详细设计

- 融入 research flow 第 4 位（business 之后，financial 之前）
- 核心职责：确定可比公司列表 + 拉取 peer 财务和估值数据
- 特殊工具：TushareValuationDataTool, ComparableValuationTool
- 产出供 financial_crew 和 operating_metrics_crew 进行同行对比

### 5.3 valuation_crew 详细设计

- 使用 Hierarchical Process，manager 兼任内部 QA
- 无外部 QA gate
- Agents: peer_valuation_agent（可比估值）, intrinsic_valuation_agent（DCF）, valuation_synthesizer（汇总）
- 工具：ComparableValuationTool, IntrinsicValuationTool, FootballFieldTool, TushareValuationDataTool

---

## 六、Planning Crew 与 Thesis/Writeup

### 6.1 Planning Crew 调整

- Registry seed 扩展：支持 entry_type（fact/data/judgment）三类 entry 播种
- Planning 负责"骨架全面"：30-50 条 judgment + 事实和数据 seeds
- 下游 crew 负责细节补充

### 6.2 Investment Thesis Crew

- **保持 Sequential，2 agents，不变**
- 仅增加 peer_info_pack_text 和 operating_metrics_pack_text 输入
- 无 QA gate

### 6.3 Writeup Crew

- **保持 Sequential，1 agent，不变**
- 增加 peer_info_pack_text 和 operating_metrics_pack_text 输入

---

## 七、Debug 检查点

### 7.1 自动落盘检查点

所有检查点存放在 `.cache/{run_id_company}/checkpoints/` 下。

| 检查点 | 时机 | 路径模式 |
|---|---|---|
| CP-00 | prepare_evidence 完成 | `.cache/{run_id_company}/checkpoints/cp00_prepared.json` |
| CP-01 | planning 完成 | `.cache/{run_id_company}/checkpoints/cp01_planned.json` |
| CP-02a~g | 每个 sub-crew 完成 | `.cache/{run_id_company}/checkpoints/cp02{a-g}_{pack_name}.json` |
| CP-03 | research QA gate | `.cache/{run_id_company}/checkpoints/cp03_research_gate.json` |
| CP-04 | valuation 完成 | `.cache/{run_id_company}/checkpoints/cp04_valuation.json` |
| CP-05 | thesis 完成 | `.cache/{run_id_company}/checkpoints/cp05_thesis.json` |
| CP-06 | writeup 完成 | `.cache/{run_id_company}/checkpoints/cp06_writeup.json` |

### 7.2 Registry 快照版本化

每个 checkpoint 保存 registry 快照到 `.cache/{run_id_company}/registry/snapshots/cp{XX}_{name}.json`，支持任意两阶段 diff。

### 7.3 Sub-Crew 级别日志隔离

每个 sub-crew 独立日志文件：`.cache/logs/runs/<slug>__<crew_name>.txt`

---

## 八、实施阶段

### Phase 1: Registry 模型层重构
- `flow/models.py` — 新增 RegistryEntry 统一模型（扩展字段）
- `flow/registry.py` — 新增 render_registry_markdown(), 修改 add 函数
- `tools/registry_tools.py` — 扩展 AddJudgmentTool 为 AddEntryTool, 修改 ReadRegistryTool 输出为 MD
- `tools/__init__.py` — 更新导出

### Phase 2: Research Sub-Crew 拆分
- 新建 7 个 sub-crew 目录（含 config/agents.yaml, config/tasks.yaml）
- `flow/research_flow.py` — 替换 _run_research_stage 为 7 sub-crew 循环调度
- 实现 Hierarchical Process 配置

### Phase 3: Valuation 拆分 + QA 精简
- 新建 peer_info_crew（融入 research flow）
- 重构 valuation_crew 为 Hierarchical
- 精简 QA gate（research: 1 agent 1 task，valuation/thesis: 无外部 gate）
- 更新 GateReviewOutput 增加 affected_packs
- 实现定向重试逻辑

### Phase 4: Planning + Thesis + Writeup 适配
- Planning seed 支持三类 entry
- Thesis/Writeup 增加新 pack 输入

### Phase 5: Debug 检查点
- Checkpoint 自动落盘
- Registry 快照版本化
- Sub-crew 日志隔离

---

## 九、关键文件变更清单

### 新增文件
```
crews/history_background_crew/{__init__.py, crew.py, config/agents.yaml, config/tasks.yaml}
crews/industry_crew/{...}
crews/business_crew/{...}
crews/peer_info_crew/{...}
crews/financial_crew/{...}
crews/operating_metrics_crew/{...}
crews/risk_crew/{...}
```

### 重大修改
```
flow/models.py                    — RegistryEntry 统一模型, GateReviewOutput.affected_packs
flow/registry.py                  — render_registry_markdown(), registry snapshot
flow/research_flow.py             — 7 sub-crew 调度, 定向重试, QA gate 精简
tools/registry_tools.py           — AddEntryTool, ReadRegistryTool MD 输出
tools/__init__.py                 — 导出更新
crews/valuation_crew/             — Hierarchical Process 重构
crews/qa_crew/config/tasks.yaml   — 精简为 1 task
crews/planning_crew/config/tasks.yaml — seed 三类 entry
crews/investment_thesis_crew/config/tasks.yaml — 增加 pack 输入
crews/writeup_crew/config/tasks.yaml — 增加 pack 输入
```

### 可删除
```
crews/research_crew/ — 替换为 7 个独立 sub-crew
```

---

## 十、验证方案

1. **Phase 1 验证**: 运行现有 flow，确认 JSON 兼容性和 MD 渲染正确
2. **Phase 2 验证**: 对单个公司运行 7 sub-crew pipeline，对比产出质量
3. **Phase 3 验证**: 模拟 business_pack 缺失场景，验证 QA → 定向重试 → business_crew 重跑
4. **端到端验证**: 完整运行一次，对比 v0.2 与 v0.3 的最终报告质量
