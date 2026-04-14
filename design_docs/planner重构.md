# 将 Planning Phase 重构为“固定 Registry 模板初始化”

## 用户意见
- 我想重构一下planning crew。我希望planning phase只是把registry建立起来，不要再设置什么reserach scope，question_tree，seed_evidence_map这些功能了（而且我认为这些功能对后续research crew的工作没有任何帮助。）

我希望初始化一个固定的registry模板，可能长达数千个问题，并且要易于人类增减删改这个模板，帮助引导后续research crew来关注应该收集什么信息，分析什么问题。

## Summary
- 把 planning phase 改成纯确定性初始化步骤，不再调用 LLM，不再生成 `research_scope`、`question_tree`、`seed_evidence_map`。
- 7 个 research crew + QA crew + valuation crew + thesis crew中的`question_tree_text` `research_scope_text` 作为占位的部分都由文本来描述其所在过程和关注重点。
- planning 的唯一职责变成：加载一套固定的全局 YAML registry 模板，实例化后写入当前 run 的 `evidence_registry.json`，供后续 7 个 research sub-crews 直接围绕 registry 工作。
- 模板覆盖 7 个 research crew的研究方向，采用 `fact/data/judgment` 混合结构，显式稳定 ID，接近完整 registry 字段，便于人工长期维护。
- QA crew 不再以 `question_tree` 为覆盖基准，以 registry 中 `priority`=high/medium为阻塞基准，若相关entry `confidence`<50 or `status`=unchecked or `status`=need_revision。research QA将会block。对于`priority`=low的entry，research QA 仅做缺陷记录，不阻塞流程。

## Implementation Changes
- Flow 编排：
  - `prepare_evidence` 删除，改为用新的`registry_initiation`
  - 保留 planning 这个 flow 节点，但内部改成“模板加载器 + 校验器 + registry 写入器”，不再调用 `PlanningCrew`。
  - 删除 `ResearchFlowState` 里的 `research_scope_path`、`question_tree_path`、`evidence_map_seed_path`，同步移除 `_base_inputs()`、checkpoint、日志、测试中的相关依赖。
  - 不再生成独立 planning Markdown 产物；人工检查统一看 `evidence_registry.json` 和 `registry_snapshot.md`。

- 模板体系：
  - 新增一个全局模板 UTF-8 YAML 文件；。
  - YAML 文件使用，显式写出稳定 `entry_id`，包含这些字段：
    - `entry_id` : `唯一的`
    - `topic`: `history`/`governance`/`business`/`industry`/`risk`/`financial`/`peers`/
    - `owner_crew`: 负责回答该问题的crew，`business_crew`/`financial_crew`/`history_background_crew`/`industry_crew`/`operating_metrics_crew`/ `peer_info_crew`/`risk_crew`
    - `priority` : high/medium/low
    - `title`: 关注问题
    - `description`: 关注问题详细描述及对输出预期的指引
    - `content`: 必填，agent填写答案，可以是数字、文字或数字及文字的结合，可以是查找不到信息
    - `unit` : 必填，agent填写答案，具体单位/不适用，可以是查找不到信息
    - `period` : 必填，agent填写答案，具体年份/年月日/也可以不适用，可以是查找不到信息
    - `source` : 必填，website url / inputfile_page_1 / inputfile_page_50-52 / tushare / ....，可以是不适用
    - `confidence` : 必填，agent对相关conent是否准确、完整回答了`title`和`description`的要求进行自我评分
    - `status` ： unchecked（待解答）/ checked（已解答）/ need_revision (需修订)
    - `revision_detail` : research QA 在标记 need_revision 时写入的具体修订说明，指导 research crew 知道要补什么
    - `creator` ： 默认是system，如果是crew create的，谁create的就是谁
  - 现有 `default_seed_entries()` 的小硬编码骨架删除，LLM 专用的 `RegistrySeedPlan` 命名退出主路径并删除；也删除“用确定性 entries 替换 registry”的 helper。

- Crew对Registry的使用方法：
  - 扩展 `read_registry`，让 crew 可按要求筛选 registry 字段，而不是一次吃下整包 Markdown。
  - 新增轻量读取视图`view="entry_list"`：返回过滤后的轻量 JSON 条目列表
  - 在输入模型中加入筛选与缩放参数：
    - 对所有运行时有意义的 entry 字段提供可选筛选
    - 文本字段支持精确匹配或 contains
    - 证据关联支持 `has_supporting_evidence` / `has_conflicting_evidence` / `has_context_evidence`
   - 7 个 research crews 的 prompt 改成先按 `owner_crew` + `status` 读取自己的 entry，再向`content` `unit` `period` `source` `status` 补证、补状态。
   - research里的每个crew里`extract_file_fact_agent`和`search_fact_agent` 可以以 `entry_id` or `owner_crew` or `status` 读取需要的 entry，再补证、补状态。用`entry_id`的时候仅为被QA要求修订时。
   - research里的每个crew里`synthesizing_agent` 按 `owner_crew` 读取自己的 entry，再综合输出。
   - research里的每个crew里`qa_agent` 按 `status` 读取自己的 entry，再提示mananger将相关续补证`entry_id`返回`extract_file_fact_agent`和`search_fact_agent` 。
   - 后续的valuation_crew在读取registry的时候，按`topic`读取需要的entry
   - 确定investment thesis_crew不读取registry
  - QA crew 改成基于`status`里 `unchecked` 状态 做覆盖审查，不再读取 `question_tree_text`。

- Planning 清理：
  - 旧 `planning_crew` 目录、其 YAML prompt、`RegistrySeedTool` 在 planning 阶段的用途，以及相关 LLM 依赖一起退场。
  - flow 里只保留一个确定性 planning 节点，不再保留“停用但还在仓库里”的旧实现。

## Public Interfaces / Types
- `ResearchFlowState` 删除 3 个 planning 产物路径字段。
- `ReadRegistryInput` 扩展为可表达字段级筛选与分页，并新增 `entry_list` / `summary` 视图。
- registry 模板成为新的人工维护接口：YAML 文件就是 planning phase 的唯一“研究引导源”。
- research / QA / writeup 输入中移除 `research_scope_text` 与 `question_tree_text`。

## Test Plan
- 模板加载：
  - 成功加载 pack YAML，并生成 registry 快照
  - 重复 ID、pack 不匹配、非法 owner、缺字段时明确失败
  - UTF-8 中文不损坏
- Flow：
  - `build_research_plan` 不再调用 crew，但会把模板写入 registry
  - `prepare_evidence` + planning 后的 registry 条目数、pack 分布、snapshot 正确
  - `ResearchFlowState` 与 checkpoint 不再依赖旧 3 个 planning 文件
- Tool / prompt：
  - `read_registry` 的字段筛选、分页、轻量视图可用
  - 7 个 research crew prompt 不再包含 `research_scope_text` / `question_tree_text`
  - QA prompt 不再以 question tree 为基准，而以 registry unresolved high/medium entries 为基准
- Gate：
  - `high + medium` 未覆盖会触发 `revise`
  - 仅 `low` 未覆盖不会阻塞通过
- 回归：
  - 现有 registry 读写、evidence 登记、线程安全测试继续通过
  - research sub-crew 输入和综合输出测试更新为“围绕 registry 工作”的新契约

## Assumptions
- 先只做单一全局模板，不做行业 overlay。
- 模板覆盖范围先限定在 7 个 research packs，不扩到 valuation / thesis。
- QA 阻塞规则固定为 `priority in {"high", "medium"}`；`low` 只记录缺口。
- CrewAI 官方 Flows 文档支持用普通 `@start/@listen/@router` Python 方法组织阶段，所以 planning 改成确定性节点是框架允许的：<https://docs.crewai.com/en/concepts/flows>
- 2026-04-10 观察到官方文档页头显示 `v1.12.1`，而本地锁定版本是 `crewai 1.14.0`；实现时以仓库锁版本和本地测试为准，并把官方 changelog 只作为背景参考：<https://docs.crewai.com/en/changelog>
