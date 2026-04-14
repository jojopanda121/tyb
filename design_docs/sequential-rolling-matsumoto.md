# Registry-Centric Deterministic Refactoring Plan

## Context

当前 planning phase 使用 LLM 生成 research_scope、question_tree、seed_evidence_map 三个产物，但这些产物对后续 research crew 没有实质帮助。本次重构将：
1. 用固定 YAML 模板替代 LLM planning，实现确定性初始化
2. 重构 RegistryEntry 模型，使其匹配新的字段体系
3. 扩展 ReadRegistryTool 的筛选能力
4. 更新所有 crew 的 YAML prompt，移除 planning 产物引用，转向 registry-centric 工作流
5. 清理所有废弃代码

---

## Phase 1: YAML 模板 + 模板加载器（纯新增，无破坏）

### 1a. 新建全局 registry 模板
- **新文件**: `src/automated_research_report_generator/flow/config/registry_template.yaml`
- 覆盖 7 个 research crew 方向，使用 fact/data/judgment 混合结构
- title/description 中可用 `{company_name}` 和 `{industry}` 占位符

**entry 分为两种 content_type**：

#### 单值型 entry（content_type: "single"，默认）
适用于 fact / judgment 以及少量独立数据点。字段：
- `entry_id`, `topic`, `owner_crew`, `priority`, `title`, `description`
- `content`: str — agent 填写答案
- `unit`, `period`, `source`, `confidence`, `status`, `revision_detail`, `creator`

#### 表格型 entry（content_type: "table"）
适用于财务报表、运营指标、同行比较等多指标×多期间的结构化数据。字段：
- `entry_id`, `topic`, `owner_crew`, `priority`, `title`, `description`
- `content_type`: `"table"`
- `columns`: `list[str]` — 列头定义，如 `["指标", "2022", "2023", "2024", "单位", "来源"]`
- `content`: `list[dict]` — agent 填充的行数据，每行是一个 dict，key 对应 columns
- `source`, `confidence`, `status`, `revision_detail`, `creator`
- 不使用 `unit` / `period`（已内化到 columns 中）

**模板示例**：
```yaml
# 单值型
- entry_id: F_HIS_001
  topic: history
  owner_crew: history_background_crew
  priority: high
  title: "{company_name} 设立背景"
  description: "公司成立时间、地点、创始人、初始业务方向"
  content_type: single

# 表格型
- entry_id: D_FIN_001
  topic: financial
  owner_crew: financial_crew
  priority: high
  title: "利润表核心指标"
  description: "收入、毛利、营业利润、净利润，最近3个完整财年+最新中报"
  content_type: table
  columns: ["指标", "2022", "2023", "2024", "2025H1", "单位", "来源"]
  content: []

- entry_id: D_OPS_001
  topic: business
  owner_crew: operating_metrics_crew
  priority: high
  title: "关键运营指标趋势"
  description: "产能、产量、出货量、利用率、ASP 等核心运营数据"
  content_type: table
  columns: ["指标", "2022", "2023", "2024", "单位", "来源"]
  content: []
```

**稳定 ID 规范**：`F_HIS_001` / `D_FIN_001` / `J_BUS_001`，pack 缩写固定 HIS/IND/BUS/PEER/FIN/OPS/RISK

### 1b. 新增模板加载函数
- **修改文件**: [registry.py](src/automated_research_report_generator/flow/registry.py)
- 新增 `load_registry_template(company_name, industry, template_path=None) -> list[RegistryEntry]`
- 读取 YAML → 校验每条 entry → 对 title/description 做 `{company_name}` / `{industry}` 插值 → 返回 entry 列表
- 校验规则：entry_id 唯一、owner_crew 合法、缺字段报错、UTF-8 中文不损坏

---

## Phase 2: RegistryEntry 模型迁移

### 2a. 重构 RegistryEntry
- **修改文件**: [models.py](src/automated_research_report_generator/flow/models.py)

**新增字段**:
- `topic`: `Literal["history", "industry", "business", "peer_info", "financial", "operating_metrics", "risk", "peers", "intrinsic_value", "valuation", "investment_thesis"]`
- `description`: str（对输出预期的指引）
- `content_type`: `Literal["single", "table"]`，默认 `"single"`
- `columns`: `list[str]`，默认 `[]` — 仅 table 型使用，定义列头
- `source`: str（替代 source_ref，URL/inputfile_page_X/tushare/N/A）
- `confidence`: str（agent 自评分）
- `revision_detail`: str（QA 修订说明）
- `creator`: str（默认 "system"）

**修改字段**:
- `content` 类型从 `str` 改为 `str | list[dict]` — single 型为 str，table 型为 list of row dicts
- `status` 类型改为 `Literal["unchecked", "checked", "need_revision"]`
- 不再保留旧 status 映射，旧 question registry 的 `open/supported/confirmed/closed/gap/conflicted/in_progress` 语义统一退出当前实现

**content 序列化约定**:
- single 型：content 为普通字符串
- table 型：content 为 `list[dict]`，每个 dict 的 key 对应 columns 列表中的列名
- JSON 序列化时 content 直接存为对应类型（str 或 list），无需额外编码
- Markdown 渲染时 table 型渲染为表格

**最终删除字段**:
- `target_pack`, `entry_origin`, `conflict_severity`, `gap_note`, `next_action`, `calibration_note`, `parent_entry_id`, `entry_level`, `evidence_needed`, `supporting/conflicting/context_evidence_ids`, `source_ref`

### 2b. 更新 CrewOwner
- 移除 `"planning_crew"`, `"research_crew"`（validator 兼容旧值映射）

### 2c. 更新序列化
- **修改文件**: [registry_tools.py](src/automated_research_report_generator/tools/registry_tools.py)
- `_serialize_entry()` 输出新字段（content_type, columns, 以及 content 的多态类型）
- table 型 entry 序列化时保留 content 为 list[dict] 结构
- **修改文件**: [registry.py](src/automated_research_report_generator/flow/registry.py)
- `_render_markdown_from_snapshot()` 更新表头和列
- table 型 entry 的 Markdown 渲染：按 columns 生成 Markdown 表格，嵌入到 entry 展示中

---

## Phase 3: 替换 Planning Crew 为确定性加载器

### 3a. 重写 build_research_plan()
- **修改文件**: [research_flow.py:242-287](src/automated_research_report_generator/flow/research_flow.py#L242-L287)
- 新逻辑：加载 YAML 模板 → 校验 → 写入 registry → checkpoint → 日志
- 不再调用 PlanningCrew，不再生成 research_scope/question_tree/evidence_map_seed

### 3b. 新增 initialize_registry_from_template()
- **修改文件**: [registry.py](src/automated_research_report_generator/flow/registry.py)
- 接受 entry 列表，写入 registry JSON + Markdown snapshot

### 3c. 删除废弃方法和字段
- **research_flow.py**: 删除 `_planning_inputs()`（L539-555）、`_coerce_registry_seed()`（L1037-1050）、PlanningCrew import、replace_registry_entries import、RegistrySeedPlan import
- **models.py**: 删除 `RegistrySeedPlan`（L156-168）、`RegistrySeedEntryPlan`（L127-154）
- **models.py**: 从 ResearchFlowState 删除 `research_scope_path`, `question_tree_path`, `evidence_map_seed_path`

---

## Phase 4: 更新 _base_inputs() 和所有 Crew YAML Prompt

> **关键**：YAML 占位符和 Python 输入字典必须同步修改，否则 CrewAI 会报错。本 phase 内的改动必须作为一个 commit 提交。

### 4a. 修改 _base_inputs()
- **修改文件**: [research_flow.py:520-537](src/automated_research_report_generator/flow/research_flow.py#L520-L537)
- 移除 `"question_tree_text"` 和 `"research_scope_text"` 两行
- 新增 `"owner_crew"` 以便 YAML 中引用

### 4b. 更新 _research_subcrew_inputs()
- **修改文件**: [research_flow.py:736-772](src/automated_research_report_generator/flow/research_flow.py#L736-L772)
- 确保 `owner_crew` 正确传入（基于 pack_name 推断对应 crew 名）

### 4c. 更新 7 个 research crew 的 tasks.yaml
- **修改文件** (共 7 个):
  - [history_background_crew/config/tasks.yaml](src/automated_research_report_generator/crews/history_background_crew/config/tasks.yaml)
  - [industry_crew/config/tasks.yaml](src/automated_research_report_generator/crews/industry_crew/config/tasks.yaml)
  - [business_crew/config/tasks.yaml](src/automated_research_report_generator/crews/business_crew/config/tasks.yaml)
  - [peer_info_crew/config/tasks.yaml](src/automated_research_report_generator/crews/peer_info_crew/config/tasks.yaml)
  - [financial_crew/config/tasks.yaml](src/automated_research_report_generator/crews/financial_crew/config/tasks.yaml)
  - [operating_metrics_crew/config/tasks.yaml](src/automated_research_report_generator/crews/operating_metrics_crew/config/tasks.yaml)
  - [risk_crew/config/tasks.yaml](src/automated_research_report_generator/crews/risk_crew/config/tasks.yaml)

**search_facts 任务**：
- 删除 `- 研究范围：{research_scope_text}` 和 `- 问题树：{question_tree_text}`
- 新增：`- 先使用 read_registry 工具，按 owner_crew="{owner_crew}" 和 status="unchecked" 或 status="need_revision" 筛选本 crew 负责的条目`
- 新增：`- 被 QA 标记为 need_revision 的条目，参照其 revision_detail 进行定向补证`
- 新增：`- 对于 content_type="single" 的条目，填写 content/unit/period/source/confidence，将 status 改为 "checked"`
- 新增：`- 对于 content_type="table" 的条目，按 columns 定义逐行补充 content（list of row dicts），每行填齐所有列`

**extract_file_facts 任务**：
- 新增 registry 读取指引（按 owner_crew 或 entry_id 筛选）
- 被 QA 返工时按 entry_id 读取需修订的条目

**check_registry 任务**（原 qa_check_agent）：
- 改为按 status 筛选，提示 manager 将需要补证的 entry_id 返回给 search/extract agent

**synthesize_and_output 任务**：
- 按 owner_crew 读取所有 entry 进行综合

### 4d. 更新 QA crew tasks.yaml
- **修改文件**: [qa_crew/config/tasks.yaml](src/automated_research_report_generator/crews/qa_crew/config/tasks.yaml)
- 删除 `- 问题树：{question_tree_text}`
- 新增覆盖度基线描述：以 registry 中 status="unchecked" 或 status="need_revision" 的 high/medium priority 条目为阻塞基准
- 新增规则：low priority 仅记录缺陷，不阻塞

### 4e. 更新 QA gate 逻辑
- **修改文件**: [research_flow.py](src/automated_research_report_generator/flow/research_flow.py) `review_research_gate()` / `_run_qa_stage()`
- 移除 question_tree_text 输入
- QA 输入改为基于 registry status + priority 的筛选结果

### 4f. Valuation / Thesis / Writeup crew
- Valuation crew：按 topic 读取 registry（需在 tasks.yaml 中增加说明，但不改变输入字典结构）
- Thesis crew：确认不读取 registry（无修改）
- Writeup crew：无 planning 产物引用（无修改）

---

## Phase 5: 扩展 ReadRegistryTool

### 5a. 新增过滤参数
- **修改文件**: [registry_tools.py](src/automated_research_report_generator/tools/registry_tools.py)
- `ReadRegistryInput` 新增：
  - `owner_crew: str = ""` — 按 owner_crew 精确筛选
  - `topic: str = ""` — 按 topic 精确筛选
  - `title_contains: str = ""` — 按 title 包含匹配
  - `has_supporting_evidence: bool | None = None`
  - `has_conflicting_evidence: bool | None = None`
  - `has_context_evidence: bool | None = None`

### 5b. 新增 entry_list 视图
- `ReadRegistryView` 新增 `"entry_list"`
- 实现 `_read_entry_list()`：返回轻量 JSON（entry_id, title, status, priority, owner_crew, topic, confidence）
- 更新 `_filtered_entries()` 以支持 owner_crew、topic、title_contains 和 evidence 关联过滤
- 更新 `ReadRegistryTool._run()` 路由新视图

---

## Phase 6: 清理废弃代码

### 6a. 删除 planning_crew 目录
- **删除**: `src/automated_research_report_generator/crews/planning_crew/` 整个目录

### 6b. 删除 registry 废弃代码
- **registry.py**: 删除 `default_seed_entries()`、`replace_registry_entries()`
- **registry_tools.py**: 删除 `RegistrySeedTool`、`RegistrySeedInput`
- **tools/__init__.py**: 移除 RegistrySeedTool 导出

### 6c. 清理 RegistryEntry 过渡字段
- **models.py**: 删除 `target_pack`, `entry_origin`, `conflict_severity`, `gap_note`, `next_action`, `calibration_note`, `parent_entry_id`, `entry_level`, `evidence_needed`, `supporting/conflicting/context_evidence_ids`, `source_ref`
- 删除 `QuestionLevel`, `QuestionOrigin`, `ConflictSeverity` 类型别名（如无其他引用）
- 删除旧 status 映射 validator

### 6d. 更新 status 排序
- **registry_tools.py**: `QUESTION_STATUS_PRIORITY` 和 `_sort_entries()` 改用新 status 值排序

---

## 依赖关系

```
Phase 1 (模板+加载器) ─── 纯新增
    │
Phase 2 (模型迁移) ────── 依赖 Phase 1 的新字段验证
    │
Phase 3 (替换 Planning) ─ 依赖 Phase 1 (加载器) + Phase 2 (新模型)
    │
Phase 4 (YAML Prompt) ── 依赖 Phase 3 (state 字段已移除)
    │
Phase 5 (ReadRegistry) ─ 依赖 Phase 2 (新字段可供筛选)
    │
Phase 6 (清理) ────────── 依赖以上全部稳定
```

---

## 风险和应对

| 风险 | 应对 |
|------|------|
| YAML 占位符与 Python 输入不同步导致 CrewAI 报错 | Phase 4 内 YAML 和 _base_inputs() 修改必须同 commit |
| 历史 registry JSON 或测试仍引用旧字段 | 统一迁移到 topic/source/revision_detail 等当前字段，必要时在测试中显式断言旧字段已不存在 |
| target_pack 被广泛使用（QA gate、pack routing、过滤） | 当前实现不再保留 target_pack，pack routing 与过滤统一改用 topic 或 owner_crew |
| 模板 entry 数量庞大可能导致 prompt 过长 | entry_list 轻量视图 + 按 owner_crew 筛选，避免一次吃下整包 |
| content 多态类型（str vs list[dict]）增加序列化复杂度 | Pydantic discriminator 基于 content_type 字段区分；JSON 原生支持两种类型 |
| Agent 填写 table 型 content 格式不规范 | prompt 中明确 columns 约束 + add_entry/status_update 工具校验 row dict keys 必须匹配 columns |

---

## 验证计划

1. **模板加载**: 加载 YAML 成功、重复 ID 报错、非法 owner 报错、UTF-8 中文正常
2. **表格型 entry**: table 型 entry 的 columns 非空校验、content 为 list[dict] 序列化/反序列化、Markdown 渲染为表格
3. **Flow 测试**: build_research_plan 不调用 LLM、registry 条目数和 owner 分布正确
4. **Registry 工具**: read_registry 的 entry_list 视图、owner_crew/topic/status 筛选可用；table 型 entry 在各视图中正确展示
5. **Prompt 验证**: 7 个 research crew YAML 不含 research_scope_text / question_tree_text；QA crew 不以 question_tree 为基准
6. **QA Gate**: high+medium unchecked/need_revision 触发 revise；仅 low 未覆盖不阻塞
7. **回归**: 现有 registry 读写、evidence 登记、线程安全测试通过
