# 臥龍電驅 Run 诊断 + 三个改进方向

## Context

基于 `20260410_130646_臥龍電氣驅動集團股份有限公司` 这次完整 run 的日志、产出物和代码进行诊断。用户有三个问题。

---

## Q1: cryptic-petting-treasure.md 计划 vs 当前代码的分歧点

**结论：计划已基本完整落地，仅存以下细微分歧。**

| 计划要求 | 当前代码状态 | 分歧程度 |
|---|---|---|
| 7 个 research sub-crew (Hierarchical) | 全部实现 | 无 |
| Registry: JSON + MD 双层，统一 RegistryEntry (fact/data/judgment) | 已实现 | 无 |
| Research-only QA gate + affected_packs 定向重跑 | 已实现，max 2 轮 | 无 |
| Valuation Hierarchical + 内部 QA，无外部 gate | 已实现 | 无 |
| Thesis 无 QA gate | 已实现 | 无 |
| Checkpoint cp00-cp06 + registry snapshot 版本化 | cp00-cp05 正常落盘，cp06 缺失（见 Q3） | **小分歧**：cp06 未产出是 bug，非设计分歧 |
| ReadRegistryTool 增加 `filter_entry_type` 参数 | **未实现** | **小分歧**：计划提到但代码里 ReadRegistryTool 没有 filter_entry_type 入参 |
| Planning seed 30-50 条 entry | 本次 run 仅 seed 了 **8 条**（全为 judgment） | **中等分歧**：见 Q2 详细分析 |
| Writeup 增加 peer_info/operating_metrics pack 输入 | 已实现（publish_if_passed 中包含） | 无 |

**总结**：架构层面完全一致。遗留的两个 gap：(a) ReadRegistryTool 缺 filter_entry_type 过滤；(b) planning seed 数量远低于计划目标。

---

## Q2: Registry 信息零散不系统——改进方案

### 2.1 现状诊断

本次 run 的 registry 最终有 **60 条 entries**（16 fact + 28 data + 16 judgment），但存在以下系统性问题：

**问题 A：Planner seed 太少且类型单一**
- 计划要求 30-50 条（含三类），实际只 seed 了 8 条 judgment
- 没有 seed 任何 fact 或 data 条目
- 下游 crew 缺少"填表框架"，只能各自发挥

**问题 B：ID 命名混乱**
- Planning seed: `WL-BUS-01`, `WL-RISK-01`
- History crew: `hist_001_governance`, `hist_003_timeline_detail`
- Business crew: `WL_BIZ_001`, `bus_002_evtol_contracts`
- Financial crew: `finance_rd_capitalization_001` 和 `fin_003_rd_capitalization_rate`（同一指标两个 entry）
- Risk crew: `RISK_001_EVTOL_PROGRESS`（全大写）
- 没有统一的命名规范，导致同类信息无法通过 ID 快速分组

**问题 C：重复/冗余 entry**
- R&D 资本化率有 2 个 entry（`finance_rd_capitalization_001` + `fin_003_rd_capitalization_rate`）
- 分业务盈利有 2 个 entry（`finance_segment_profit_001` + `fin_001_segment_profitability`）
- eVTOL 进展横跨 3 个 pack 各有 1-2 条，内容大量重叠（`ind_002`, `WL_BIZ_002`, `bus_002`, `RISK_001`）

**问题 D：缺少系统性覆盖模板**
- 没有预定义"每个 pack 必须收集哪些数据点"
- 例如 finance_pack 应该系统覆盖：收入分拆、毛利率、EBITDA、净利润、FCF、负债率、利息覆盖、应收周转等
- 但当前全靠 agent 自由发挥，coverage 质量取决于 agent 的 initiative

### 2.2 改进方案：给 Planning Crew 注入 Pack Registry 模板

**核心思路**：在 `seed_registry_judgments` task 的 description 中注入一个 **per-pack 的必填数据清单**，让 planner 产出更完整的 skeleton entries，下游 crew 只需"填空"而非"创造"。

**具体改动**：

#### 修改 1：扩展 `seed_registry_judgments` task 的 prompt

在 `crews/planning_crew/config/tasks.yaml` 的 `seed_registry_judgments.description` 中增加：

```
每个 target_pack 必须包含以下最低 entry 数量：
- history_background_pack: 3-5 fact + 2-3 judgment
- industry_pack: 3-5 data + 2-3 judgment  
- business_pack: 5-8 data + 3-5 judgment
- peer_info_pack: 3-5 data + 2-3 judgment
- finance_pack: 8-12 data + 3-5 judgment（必须覆盖：收入/毛利率/EBITDA/净利润/FCF/负债率/利息覆盖/应收周转/存货周转）
- operating_metrics_pack: 3-5 data + 1-2 judgment
- risk_pack: 2-3 fact + 3-5 judgment

entry_id 命名规范：
- fact: F_{pack简写}_{三位序号}  (如 F_HIS_001)
- data: D_{pack简写}_{三位序号}  (如 D_FIN_001)
- judgment: J_{pack简写}_{三位序号}  (如 J_BUS_001)
- pack 简写映射：HIS=history_background, IND=industry, BUS=business, PEER=peer_info, FIN=finance, OPS=operating_metrics, RISK=risk

必须为 data 类型填写 value="待填" / unit / period / calibration_note。
```

#### 修改 2：给每个 sub-crew 的 task prompt 注入 registry 引导

在每个 sub-crew 的 `synthesize_and_output` task 中增加一段：

```
在完成分析包之前，检查 registry 中 target_pack={pack_name} 的所有 entry：
- data entry 如果 value 仍为"待填"，必须尝试补上实际数值
- 新增 entry 必须使用 {ID命名规范} 
- 不得新建与已有 entry 内容重复的条目，而是更新已有条目
```

#### 修改 3：ReadRegistryTool 增加 filter_entry_type

按 cryptic-petting-treasure.md 计划实现。让 QA agent 和 synthesizing agent 可以 `filter_entry_type="data"` 只看数据条目的完整性。

### 2.3 预期效果

- Planner seed 从 8 条提升到 40-50 条，覆盖全部 7 个 pack
- 下游 crew 有明确的"填空任务"，减少 ad-hoc 发挥
- ID 命名统一，避免同一指标出现多个 entry
- QA gate 能通过 data entry 的"待填"状态快速定位 coverage gap

### 2.4 关键文件

- `src/.../crews/planning_crew/config/tasks.yaml` — seed_registry_judgments 任务 prompt
- `src/.../tools/registry_tools.py` — ReadRegistryTool 增加 filter_entry_type
- 各 sub-crew 的 `config/tasks.yaml` — synthesize_and_output 任务增加 registry 引导

---

## Q3: 为什么输出 PDF 功能"没了"

### 3.1 诊断

**PDF 生成代码完整存在**，WriteupCrew 和 MarkdownToPdfTool 均已实现。问题是 **`publish_if_passed` 这个 flow step 根本没有被触发**。

证据：
- flow.txt 最后一行是 `13:29:11 run_investment_thesis_crew completed` — 没有 `publish_if_passed started`
- `writeup_crew.txt` 日志文件不存在
- `cp06_writeup.json` checkpoint 不存在
- run_manifest.json 的 status 仍为 `"prepared"`（非 `"completed"`）
- 预期的 `*_v2_report.md` 和 `*_v2_report.pdf` 文件均不存在

### 3.2 根因分析

**Bug 在 `@listen` vs `@router` 装饰器的使用上。**

CrewAI Flow 中，`@router` 方法的返回值会作为事件发射给下游 listener；但 `@listen` 方法的返回值**不会**被发射为事件。

当前代码链：
```
run_valuation_crew        → @router(...)  返回 VALUATION_STAGE_COMPLETED_NO_GATE_EVENT  ✅ 发射
run_investment_thesis_crew → @listen(...)  返回 THESIS_STAGE_COMPLETED_NO_GATE_EVENT     ❌ 未发射
publish_if_passed          → @listen(THESIS_STAGE_COMPLETED_NO_GATE_EVENT)               ❌ 永远等不到
```

对比所有能正常工作的事件路由，`run_valuation_crew` 是 `@router` 所以它返回的字符串事件能被下游 `@listen` 接收。但 `run_investment_thesis_crew` 使用了 `@listen`，它的返回值被 Flow 框架忽略了。

### 3.3 修复方案

**方案 A（推荐）**：将 `run_investment_thesis_crew` 改为 `@router`

```python
# research_flow.py line 455
@router(VALUATION_STAGE_COMPLETED_NO_GATE_EVENT)  # 原来是 @listen
def run_investment_thesis_crew(self):
    self._run_thesis_stage()
    return THESIS_STAGE_COMPLETED_NO_GATE_EVENT
```

这是最小改动，与 `run_valuation_crew` 使用 `@router` 的模式一致。

**方案 B（备选）**：将 `publish_if_passed` 改为监听方法引用

```python
@listen(run_investment_thesis_crew)  # 原来是 @listen(THESIS_STAGE_COMPLETED_NO_GATE_EVENT)
def publish_if_passed(self):
```

方案 B 也能工作，但不够优雅——如果未来 thesis 也需要 QA gate 分支，就无法路由了。

### 3.4 关键文件

- `src/.../flow/research_flow.py:455` — `run_investment_thesis_crew` 的装饰器

---

## 实施优先级

| 优先级 | 改动 | 原因 |
|---|---|---|
| **P0** | Q3 修复：`@listen` → `@router` on `run_investment_thesis_crew` | 一行改动恢复 PDF 输出，当前整个 writeup stage 不工作 |
| **P1** | Q2 改进：扩展 `seed_registry_judgments` prompt，增加 per-pack 必填清单和 ID 命名规范 | 直接提升 registry 系统性，改善下游 crew 的信息收集质量 |
| **P2** | Q2 补充：各 sub-crew synthesize task 增加 registry 引导 | 配合 P1，让下游 crew 知道如何与 seed 对齐 |
| **P3** | Q1 补齐：ReadRegistryTool 增加 filter_entry_type | 计划中已有，实现简单 |

## 验证方案

1. **P0 验证**：修复后重跑一次 flow（可从 cp05_thesis checkpoint 恢复跑 writeup only），确认：
   - flow.txt 出现 `publish_if_passed started/completed`
   - `cp06_writeup.json` 生成
   - `*_v2_report.md` 和 `*_v2_report.pdf` 文件生成
   - run_manifest.json status 变为 `"completed"`

2. **P1/P2 验证**：修改 planning prompt 后对同一公司重跑，对比：
   - cp01_planned.json 中 entry 数量（目标 40-50 vs 当前 8）
   - 最终 registry 中重复 entry 数量（目标 0）
   - ID 命名是否统一

3. **P3 验证**：单元测试 ReadRegistryTool 带 filter_entry_type 参数
