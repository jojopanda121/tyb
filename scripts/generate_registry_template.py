"""
Generate a comprehensive registry_template.yaml for buy-side equity research.
Target: 1000+ entries across 7 topics (no valuation topic).

Topics:
  history       -> history_background_crew
  industry      -> industry_crew
  business      -> business_crew
  peer_info     -> peer_info_crew
  financial     -> financial_crew
  operating_metrics -> operating_metrics_crew
  risk          -> risk_crew
"""

import yaml, sys
from collections import defaultdict, Counter

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def F(topic, crew, priority, title, desc, **kw):
    e = dict(entry_id="", entry_type="fact", topic=topic,
             owner_crew=crew, priority=priority, title=title, description=desc)
    e.update(kw)
    return e

def D(topic, crew, priority, title, desc, columns, **kw):
    e = dict(entry_id="", entry_type="data", topic=topic,
             owner_crew=crew, priority=priority, title=title, description=desc,
             content_type="table", columns=columns, content=[])
    e.update(kw)
    return e

def J(topic, crew, priority, title, desc, **kw):
    e = dict(entry_id="", entry_type="judgment", topic=topic,
             owner_crew=crew, priority=priority, title=title, description=desc)
    e.update(kw)
    return e

PERIODS = ["{期间1}", "{期间2}", "{期间3}", "{期间4}", "{期间5}"]
TS   = ["指标"] + PERIODS + ["单位", "来源"]
LIST = ["项目", "详情", "来源"]
COMP = ["公司", "指标值", "来源"]

entries = []

# ══════════════════════════════════════════════
# 1. HISTORY & BACKGROUND  (~100 entries)
# ══════════════════════════════════════════════
H = "history"; HC = "history_background_crew"

# 1.1 公司概况
for t, d in [
    ("{company_name} 公司全称与简称", "全称、简称、证券代码、英文名称。"),
    ("{company_name} 注册地与总部地址", "注册地址、办公地址、是否存在不一致。"),
    ("{company_name} 成立日期", "成立日期、股改日期（如适用）。"),
    ("{company_name} 法定代表人", "现任法定代表人及任职起始时间。"),
    ("{company_name} 注册资本", "注册资本金额、实缴情况。"),
    ("{company_name} 统一社会信用代码", "统一社会信用代码/营业执照编号。"),
    ("{company_name} 经营范围概述", "营业执照载明的经营范围要点。"),
    ("{company_name} 上市/挂牌状态", "上市交易所、板块、上市日期。"),
    ("{company_name} 所属行业分类", "证监会行业分类、GICS分类、申万行业分类。"),
    ("{company_name} 公司定位一句话描述", "用一句话概括公司是做什么的、服务谁、核心价值是什么。"),
]: entries.append(F(H, HC, "high" if "全称" in t or "上市" in t or "定位" in t else "medium", t, d))

# 1.2 历史沿革
for t, d in [
    ("{company_name} 设立背景与方式", "前身、设立方式（发起/改制）、原始出资方及出资方式。"),
    ("{company_name} 历史里程碑时间线", "上市、并购、扩产、融资等关键事件按时间排列。"),
    ("{company_name} 历次重大重组/并购", "历史重大资产重组、业务重组事件及影响。"),
    ("{company_name} 历史战略转型", "主营业务转型、商业模式变革及驱动因素。"),
    ("{company_name} 历史上的重大融资事件", "IPO、增发、配股、可转债等历史融资事项。"),
    ("{company_name} 历史上的重大诉讼/处罚", "历史重大诉讼、行政处罚及其解决情况。"),
    ("{company_name} 品牌历史与传承", "品牌创建历史、品牌演变及品牌价值积累。"),
]:  entries.append(F(H, HC, "high" if "里程碑" in t or "设立" in t else "medium", t, d))

# 1.3 股权结构（大幅扩展）
for t, d in [
    ("{company_name} 控股股东与实际控制人", "控股股东名称、持股比例、实际控制人认定及控制链条。"),
    ("{company_name} 股权结构图", "主要股东及持股比例，穿透至实际控制人。"),
    ("{company_name} 实际控制人背景", "实际控制人的个人履历、从业背景、其他控制企业。"),
    ("{company_name} 实际控制人一致行动人", "实际控制人的一致行动人及关联股东。"),
    ("{company_name} 控股股东主营业务", "控股股东的主要业务、资产规模及财务状况。"),
    ("{company_name} 控股股东资金状况", "控股股东资产负债率、是否存在资金紧张、对公司的资金依赖。"),
]: entries.append(F(H, HC, "high", t, d))

entries.append(D(H, HC, "high", "{company_name} 前十大股东持股明细",
    "前十大股东名称、持股数量、比例、股份性质。",
    ["股东名称", "持股数量（万股）", "持股比例", "股份性质", "来源"]))
entries.append(D(H, HC, "medium", "{company_name} 历次股本变动",
    "历次增资、股权转让、定增、配股等股本变动。",
    ["日期", "事项", "变动股数（万股）", "变动后总股本", "来源"]))

for t, d in [
    ("{company_name} 股东间一致行动关系", "主要股东间是否存在一致行动协议。"),
    ("{company_name} 股权质押情况", "控股股东及主要股东持股质押情况。"),
    ("{company_name} 股权冻结情况", "股东持股是否存在冻结或其他权利受限。"),
    ("{company_name} 战略投资者", "战略投资者名称、入股时间、持股比例及战略价值。"),
    ("{company_name} 机构投资者持股", "主要机构投资者（基金、保险、外资等）持股情况。"),
    ("{company_name} 对赌协议情况", "是否存在对赌协议、业绩承诺及解除情况。"),
    ("{company_name} 股权激励计划", "员工持股计划、限制性股票、股票期权安排。"),
    ("{company_name} 股权激励覆盖范围", "股权激励覆盖的人数、层级及解锁条件。"),
    ("{company_name} 限售股解禁安排", "限售股数量、解禁时间及对流通的影响。"),
    ("{company_name} 股东减持历史", "主要股东历史减持记录及减持原因。"),
    ("{company_name} 股份回购历史", "历史回购金额、回购价格区间及用途。"),
]: entries.append(F(H, HC, "medium" if "质押" in t or "冻结" in t or "激励" in t else "low", t, d))

# 1.4 管理层（扩展）
for t, d, p in [
    ("{company_name} 董事会构成", "董事会成员名单、独立董事占比、各董事背景。", "high"),
    ("{company_name} 董事会专业委员会", "审计委员会、薪酬委员会、战略委员会等设置及运作。", "medium"),
    ("{company_name} 独立董事背景", "独立董事的专业背景、行业经验及独立性。", "medium"),
    ("{company_name} CEO/总经理履历", "CEO的教育背景、从业经历、行业资历。", "high"),
    ("{company_name} CFO/财务总监履历", "CFO的专业资质、从业经历。", "medium"),
    ("{company_name} 其他核心高管", "COO、CTO、营销总监等其他核心高管背景。", "medium"),
    ("{company_name} 核心技术人员", "核心技术人员名单、技术贡献及约束激励。", "medium"),
    ("{company_name} 管理层任期与变动", "报告期内董事、高管变动及原因。", "medium"),
    ("{company_name} 管理层持股情况", "董监高及核心人员直接和间接持股。", "medium"),
    ("{company_name} 管理层兼职情况", "董监高是否在关联方或同业兼职。", "medium"),
    ("{company_name} 管理层竞业禁止", "核心人员竞业禁止协议及保密协议安排。", "low"),
    ("{company_name} 管理层过往成功案例", "管理层在此前公司或本公司的关键成功案例。", "medium"),
]: entries.append(F(H, HC, p, t, d))

entries.append(D(H, HC, "medium", "{company_name} 董监高薪酬明细",
    "主要高管年度薪酬总额。",
    ["姓名", "职务", "薪酬（万元）", "持股（万股）", "来源"]))

# 1.5 公司治理
for t, d, p in [
    ("{company_name} 公司治理架构", "三会一层治理架构及运作机制。", "medium"),
    ("{company_name} 内部控制制度", "主要内部控制制度及运行有效性评价。", "medium"),
    ("{company_name} 内部审计制度", "内部审计部门设置及运行情况。", "low"),
    ("{company_name} 信息披露制度", "信息披露制度及执行质量评价。", "medium"),
    ("{company_name} 关联交易管理制度", "关联交易的审批流程及管控制度。", "medium"),
    ("{company_name} 对外投资管理制度", "对外投资、担保的决策程序。", "low"),
    ("{company_name} 资金管理制度", "资金授权审批流程、资金安全管控。", "low"),
]: entries.append(F(H, HC, p, t, d))

# 1.6 组织与子公司
entries.append(F(H, HC, "medium", "{company_name} 组织架构", "部门设置及职能划分。"))
entries.append(D(H, HC, "medium", "{company_name} 主要控股子公司",
    "主要控股子公司名称、持股比例、主营、财务贡献。",
    ["子公司", "持股比例", "主营", "营收贡献", "净利贡献", "来源"]))
entries.append(D(H, HC, "low", "{company_name} 参股公司",
    "主要参股公司及投资收益。",
    ["参股公司", "持股比例", "主营", "投资收益", "来源"]))
entries.append(F(H, HC, "low", "{company_name} 境外架构（如有）", "境外控制架构、VIE等特殊安排。"))

# 1.7 员工
entries.append(D(H, HC, "medium", "{company_name} 员工总数与结构",
    "员工总数、专业结构、学历分布趋势。", TS))
for t, d in [
    ("{company_name} 研发人员占比", "研发人员数量及占员工总数比例。"),
    ("{company_name} 销售人员占比", "销售人员数量及占比。"),
    ("{company_name} 生产人员占比", "生产人员数量及占比。"),
    ("{company_name} 人均薪酬水平", "人均薪酬及与同地区/同行对比。"),
    ("{company_name} 员工流失率", "年度员工流失率及核心人员稳定性。"),
    ("{company_name} 劳务派遣情况", "劳务派遣用工人数、占比及合规性。"),
]: entries.append(F(H, HC, "low", t, d))

# 1.8 关联方
for t, d, p in [
    ("{company_name} 关联方清单", "主要关联方名称、关系类型、主营业务。", "high"),
    ("{company_name} 关联方关系图", "关联方之间的股权和业务关系。", "medium"),
    ("{company_name} 同业竞争情况", "控股股东控制的企业是否存在同业竞争。", "medium"),
    ("{company_name} 关联交易定价公允性", "关联交易的定价依据及与市场价格的对比。", "medium"),
    ("{company_name} 关联方资金占用", "是否存在关联方占用公司资金的情况。", "high"),
]: entries.append(F(H, HC, p, t, d))

entries.append(D(H, HC, "high", "{company_name} 关联交易汇总",
    "关联采购、关联销售金额及占比。",
    ["关联方", "交易类型", *PERIODS, "占比", "来源"]))

# 1.9 治理判断
for t, d, p in [
    ("{company_name} 治理结构是否稳健", "判断控股结构、管理层稳定性、治理安排是否支持长期经营。", "high"),
    ("{company_name} 管理层激励是否与股东利益一致", "判断薪酬、持股、考核是否与股东绑定。", "high"),
    ("{company_name} 实际控制人风险是否可控", "判断实控人的资金状况、质押比例、减持意向是否构成风险。", "high"),
    ("{company_name} 关联交易是否影响独立性", "判断关联交易规模、公允性及对利润的影响。", "high"),
    ("{company_name} 信息披露质量", "判断公司信息披露的及时性、完整性和可信度。", "medium"),
]: entries.append(J(H, HC, p, t, d))


# ══════════════════════════════════════════════
# 2. INDUSTRY  (~130 entries)
# ══════════════════════════════════════════════
I = "industry"; IC = "industry_crew"

# 2.1 行业定义
for t, d, p in [
    ("{industry} 行业定义与边界", "标准分类、核心业务范畴及边界。", "high"),
    ("{industry} 行业细分领域", "行业的主要细分市场及各细分特征。", "high"),
    ("{industry} 产业链全景图", "上游/中游/下游全链条关系。", "high"),
    ("{company_name} 在产业链中的位置", "公司处于哪个环节，与上下游的关系。", "high"),
    ("{industry} 行业价值链分析", "各环节的附加值分布及利润率差异。", "medium"),
]: entries.append(F(I, IC, p, t, d))

# 2.2 行业规模与增长（细化）
for t, d, p in [
    ("{industry} 全球市场规模", "全球市场规模（金额），近5年数据及来源。", "high"),
    ("{industry} 中国市场规模", "中国市场规模（金额），近5年数据。", "high"),
    ("{industry} 全球增速（历史）", "过去5年全球行业CAGR。", "high"),
    ("{industry} 中国增速（历史）", "过去5年中国行业CAGR。", "high"),
    ("{industry} 全球增速（预测）", "未来3-5年全球行业CAGR预测。", "high"),
    ("{industry} 中国增速（预测）", "未来3-5年中国行业CAGR预测。", "high"),
]: entries.append(F(I, IC, p, t, d))

entries.append(D(I, IC, "high", "{industry} 市场规模时序数据",
    "全球和中国市场规模历史及预测。",
    ["年份", "全球规模", "中国规模", "增速", "单位", "来源"]))

for t, d in [
    ("{industry} 增长驱动因素", "核心增长驱动因素（需求/技术/政策/人口等）。"),
    ("{industry} 行业生命周期阶段", "导入/成长/成熟/衰退，判断依据。"),
    ("{industry} 行业天花板测算", "行业潜在市场空间（TAM/SAM/SOM）的测算逻辑。"),
    ("{industry} 渗透率与提升空间", "当前渗透率及未来提升空间。"),
]: entries.append(F(I, IC, "high" if "驱动" in t else "medium", t, d))

# 2.3 各细分市场（生成多条）
for seg in ["细分市场A", "细分市场B", "细分市场C", "细分市场D", "细分市场E"]:
    entries.append(D(I, IC, "medium",
        f"{{industry}} {seg}市场规模",
        f"{seg}的市场规模及增速。",
        ["年份", "市场规模", "增速", "来源"]))
    entries.append(F(I, IC, "medium",
        f"{{industry}} {seg}竞争格局",
        f"{seg}的主要玩家及份额分布。"))
    entries.append(F(I, IC, "medium",
        f"{{industry}} {seg}增长驱动",
        f"{seg}的核心增长驱动因素。"))

# 2.4 行业特征（扩展）
for t, d, p in [
    ("{industry} 周期性特征", "是否有周期性，驱动因素，当前周期位置。", "medium"),
    ("{industry} 季节性特征", "收入/利润季节性波动，旺季淡季分布。", "medium"),
    ("{industry} 区域性特征", "是否有区域集中度，主要产能/市场区域。", "low"),
    ("{industry} 资本密集度", "行业的固定资产投资强度及资本壁垒。", "medium"),
    ("{industry} 技术密集度", "行业的技术含量、R&D投入水平。", "medium"),
    ("{industry} 劳动密集度", "行业的人工成本占比及自动化程度。", "low"),
    ("{industry} 规模效应", "规模效应的强弱及最低经济规模。", "medium"),
    ("{industry} 学习曲线效应", "产量累积对成本下降的影响。", "low"),
    ("{industry} 网络效应", "是否存在网络效应及其强弱。", "medium"),
    ("{industry} 客户转换成本", "行业客户的转换成本高低及锁定效应。", "medium"),
]: entries.append(F(I, IC, p, t, d))

# 2.5 竞争格局（大幅扩展）
entries.append(D(I, IC, "high", "{industry} 全球市场份额分布",
    "全球主要玩家的市场份额（CR3/CR5/CR10）。",
    ["排名", "公司", "市场份额", "来源"]))
entries.append(D(I, IC, "high", "{industry} 中国市场份额分布",
    "中国市场主要玩家份额。",
    ["排名", "公司", "市场份额", "来源"]))

for t, d, p in [
    ("{industry} 竞争格局类型", "分散/集中/寡头/垄断，变化趋势。", "high"),
    ("{industry} 主要竞争维度", "竞争主要依靠价格/技术/品牌/渠道/服务。", "high"),
    ("{company_name} 市场地位与份额", "公司排名、市场份额及变化趋势。", "high"),
    ("{company_name} 竞争优势来源", "公司相对竞品的核心差异化优势。", "high"),
    ("{company_name} 竞争劣势", "公司相对竞品的主要短板。", "high"),
    ("{industry} 进入壁垒", "新进入者面临的壁垒（资金/技术/牌照/规模等）。", "medium"),
    ("{industry} 退出壁垒", "行业退出壁垒的高低及对竞争的影响。", "low"),
    ("{industry} 潜在进入者", "可能的跨界竞争者及进入动机。", "medium"),
    ("{industry} 替代品威胁", "替代技术或替代产品对行业的威胁。", "medium"),
    ("{industry} 买方议价能力", "下游客户的议价能力及趋势。", "medium"),
    ("{industry} 供方议价能力", "上游供应商的议价能力及趋势。", "medium"),
    ("{industry} 竞争格局演变趋势", "未来3-5年竞争格局可能的演变方向。", "medium"),
    ("{industry} 行业整合度变化", "行业CR3/CR5近年变化趋势及驱动因素。", "medium"),
    ("{industry} 价格竞争烈度", "行业价格战风险、价格下降趋势。", "medium"),
]: entries.append(F(I, IC, p, t, d))

# 2.6 主要竞争对手画像（多条）
for i in range(1, 6):
    entries.append(F(I, IC, "medium",
        f"{{industry}} 竞争对手{i}深度画像",
        f"第{i}号核心竞争对手的业务模式、竞争策略、优劣势及最新动态。"))

# 2.7 政策与监管
for t, d, p in [
    ("{industry} 行业主管部门", "行业主管部门及监管框架。", "high"),
    ("{industry} 主要法律法规", "行业相关的主要法律法规清单。", "medium"),
    ("{industry} 准入制度/牌照要求", "从事行业所需的资质、牌照或审批。", "medium"),
    ("{industry} 产业政策方向", "国家/地方对行业的支持或限制政策。", "high"),
    ("{industry} 补贴政策", "行业相关的政府补贴、税收优惠政策。", "medium"),
    ("{industry} 环保监管趋势", "环保标准、排放要求及趋严趋势。", "medium"),
    ("{industry} 安全生产监管", "安全生产标准及监管执行力度。", "low"),
    ("{industry} 国际贸易政策", "出口管制、关税、反倾销等影响。", "medium"),
    ("{industry} 外资准入政策", "行业对外资的准入限制。", "low"),
    ("{industry} 知识产权保护", "行业知识产权保护力度及专利格局。", "medium"),
    ("{industry} 数据安全/隐私监管", "数据安全相关的监管要求（如适用）。", "low"),
    ("{industry} 标准制定参与情况", "行业标准制定情况，{company_name} 是否参与。", "low"),
]: entries.append(F(I, IC, p, t, d))

# 2.8 上下游
for t, d, p in [
    ("{industry} 上游原材料供给格局", "主要原材料的供给集中度及价格走势。", "medium"),
    ("{industry} 上游关键零部件供给", "关键零部件的供给来源及国产化率。", "medium"),
    ("{industry} 上游价格传导机制", "原材料涨价向中游/下游的传导能力。", "medium"),
    ("{industry} 下游需求结构", "下游主要应用领域及需求占比。", "medium"),
    ("{industry} 下游各应用领域增速", "各下游应用领域的增长率及前景。", "medium"),
    ("{industry} 下游客户集中度", "下游客户的集中度及大客户特征。", "medium"),
    ("{industry} 终端用户画像", "最终用户的特征、需求偏好及支付意愿。", "medium"),
]: entries.append(F(I, IC, p, t, d))

for seg in ["下游应用A", "下游应用B", "下游应用C"]:
    entries.append(D(I, IC, "medium",
        f"{{industry}} {seg}市场规模",
        f"{seg}的市场规模及增速。",
        ["年份", "市场规模", "增速", "来源"]))

# 2.9 技术趋势
for t, d, p in [
    ("{industry} 主流技术路径", "当前行业主流的技术路径及优劣势比较。", "medium"),
    ("{industry} 技术迭代方向", "下一代技术的方向及时间表预期。", "medium"),
    ("{industry} 颠覆性技术风险", "可能颠覆现有格局的新技术及成熟度。", "medium"),
    ("{industry} 全球技术差距", "中国与国际领先水平的技术差距。", "medium"),
    ("{industry} 专利格局", "行业专利分布及主要专利持有者。", "low"),
    ("{industry} 产学研合作", "行业内的产学研合作模式及主要成果。", "low"),
]: entries.append(F(I, IC, p, t, d))

# 2.10 行业趋势
for t, d, p in [
    ("{industry} 未来3-5年核心趋势", "行业最重要的3-5个发展趋势。", "high"),
    ("{industry} 国产替代进程", "国产化率、替代进程、技术差距。", "medium"),
    ("{industry} 海外市场机会", "中国企业出海的机会与挑战。", "medium"),
    ("{industry} 并购整合趋势", "并购活跃度、方向及对格局的影响。", "low"),
    ("{industry} 商业模式创新", "行业内新兴的商业模式及发展前景。", "medium"),
    ("{industry} 智能化/数字化转型", "行业数字化、智能化的进展及影响。", "medium"),
    ("{industry} 绿色/低碳转型", "行业的绿色低碳转型要求及机遇。", "medium"),
]: entries.append(F(I, IC, p, t, d))

# 2.11 行业判断
for t, d, p in [
    ("{industry} 增长驱动是否可持续", "判断需求、政策、技术和替代关系是否支持持续增长。", "high"),
    ("{industry} 竞争格局是否有利于 {company_name}", "判断竞争集中度趋势和公司竞争地位。", "high"),
    ("{industry} 政策环境对行业影响方向", "判断政策对行业增长是正向还是收紧。", "high"),
    ("{industry} 上下游变动对利润的影响", "判断原材料/下游需求波动对利润池的影响。", "medium"),
    ("{industry} 技术变化对 {company_name} 的影响", "判断技术迭代对公司竞争地位的影响方向。", "high"),
    ("{industry} 行业最佳商业模式", "判断行业中哪种商业模式最具长期优势。", "medium"),
    ("{industry} 最大的结构性变化", "判断正在发生的最大结构性变化及其受益者。", "high"),
]: entries.append(J(I, IC, p, t, d))


# ══════════════════════════════════════════════
# 3. BUSINESS  (~300 entries)
# ══════════════════════════════════════════════
B = "business"; BC = "business_crew"

# 3.1 商业模式
for t, d, p in [
    ("{company_name} 商业模式概述", "如何创造、传递、获取价值的核心逻辑。", "high"),
    ("{company_name} 商业模式画布", "价值主张、客户关系、渠道、收入流、成本结构等。", "medium"),
    ("{company_name} 收入模式", "收入来源（产品/服务/订阅/授权等）、计价方式。", "high"),
    ("{company_name} 盈利模式", "公司如何赚钱、利润的核心来源及利润质量。", "high"),
    ("{company_name} 商业模式迭代方向", "商业模式正在或计划的演变方向。", "medium"),
]: entries.append(F(B, BC, p, t, d))

# 3.2 产品/服务矩阵（大幅扩展）
entries.append(D(B, BC, "high", "{company_name} 产品/服务矩阵",
    "全部产品/服务的分类体系。",
    ["产品线", "产品名称", "功能", "目标客户", "营收占比", "来源"]))

# 为每个产品维度生成条目
for dim in ["产品A", "产品B", "产品C", "产品D", "产品E"]:
    for t, d, p in [
        (f"{{company_name}} {dim}产品介绍", f"{dim}的功能、规格、应用场景。", "medium"),
        (f"{{company_name}} {dim}收入规模", f"{dim}的收入金额及增速。", "high"),
        (f"{{company_name}} {dim}毛利率", f"{dim}的毛利率水平及变化趋势。", "high"),
        (f"{{company_name}} {dim}ASP趋势", f"{dim}的平均售价及变化。", "medium"),
        (f"{{company_name}} {dim}销量趋势", f"{dim}的销售数量及变化。", "medium"),
        (f"{{company_name}} {dim}竞争对标", f"{dim}与竞品的功能/价格/质量对比。", "medium"),
        (f"{{company_name}} {dim}市场份额", f"{dim}在其细分市场的份额。", "medium"),
        (f"{{company_name}} {dim}客户反馈", f"{dim}的客户评价、返修率、投诉情况。", "low"),
    ]: entries.append(F(B, BC, p, t, d))

for t, d, p in [
    ("{company_name} 核心产品竞争优势", "核心产品相对竞品的差异化优势。", "high"),
    ("{company_name} 产品生命周期阶段", "各产品所处生命周期阶段。", "medium"),
    ("{company_name} 新品管线", "在研/在推新产品、预期上市时间及空间。", "medium"),
    ("{company_name} 产品定价策略", "定价策略（成本加成/市场定价/竞争定价）。", "medium"),
    ("{company_name} 产品性价比分析", "核心产品与竞品的性价比对比。", "medium"),
    ("{company_name} 产品品牌溢价", "品牌带来的溢价空间及品牌认知度。", "medium"),
    ("{company_name} 产品组合协同效应", "产品间的交叉销售及协同效应。", "low"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "high", "{company_name} 各产品收入与增速",
    "各主要产品收入规模及同比增速。", TS))
entries.append(D(B, BC, "high", "{company_name} 各产品毛利率趋势",
    "各主要产品毛利率及变化。", TS))
entries.append(D(B, BC, "medium", "{company_name} 各产品ASP与销量",
    "各产品平均售价及销售数量。", TS))

# 3.3 销售与客户（大幅扩展）
for t, d, p in [
    ("{company_name} 销售模式概述", "直销/经销/线上/代理等占比及逻辑。", "high"),
    ("{company_name} 直销模式详情", "直销的客户获取方式、销售团队结构。", "medium"),
    ("{company_name} 经销模式详情", "经销体系的层级、管理、准入淘汰。", "medium"),
    ("{company_name} 线上销售模式", "电商平台布局、线上收入占比及增长。", "medium"),
    ("{company_name} 销售组织体系", "销售团队规模、区域布局。", "medium"),
    ("{company_name} 销售人员人均产出", "销售人员人均创收及变化趋势。", "low"),
    ("{company_name} 销售激励机制", "销售团队的薪酬结构和激励方式。", "low"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "high", "{company_name} 收入区域分布",
    "按国内外及主要区域的收入分布。",
    ["区域", *PERIODS, "占比", "来源"]))
entries.append(D(B, BC, "high", "{company_name} 收入按销售模式",
    "按直销/经销/线上等模式的收入分布。",
    ["模式", *PERIODS, "占比", "毛利率", "来源"]))

# 客户分析（细化）
entries.append(D(B, BC, "high", "{company_name} 前五大客户",
    "前五大客户名称、销售额及占比。",
    ["客户", *PERIODS, "占比", "类型", "来源"]))
entries.append(D(B, BC, "medium", "{company_name} 前十大客户",
    "前十大客户名称、销售额及占比。",
    ["客户", "2024销售额", "占比", "合作年限", "来源"]))
entries.append(D(B, BC, "medium", "{company_name} 前二十大客户",
    "前二十大客户覆盖的收入占比。",
    ["客户", "2024销售额", "占比", "来源"]))

for t, d, p in [
    ("{company_name} 客户集中度（CR1/CR5/CR10）", "前1/5/10大客户收入占比及变化。", "high"),
    ("{company_name} 客户行业分布", "客户按行业分布的收入结构。", "medium"),
    ("{company_name} 客户规模分布", "大客户/中小客户的数量及收入分布。", "medium"),
    ("{company_name} 新增客户分析", "报告期新增主要客户及获客原因。", "medium"),
    ("{company_name} 流失客户分析", "报告期流失的主要客户及流失原因。", "medium"),
    ("{company_name} 客户留存率/复购率", "客户留存率、复购率或续约率。", "medium"),
    ("{company_name} 客户转换成本", "客户转向竞品的难度及成本。", "medium"),
    ("{company_name} 客户LTV分析", "主要客户的生命周期价值（如适用）。", "low"),
    ("{company_name} 终端客户/最终用户", "经销模式下终端客户的画像。", "medium"),
    ("{company_name} 客户满意度", "客户满意度调查及反馈。", "low"),
]: entries.append(F(B, BC, p, t, d))

# 信用与回款
for t, d, p in [
    ("{company_name} 信用政策", "对主要客户的信用期限、授信额度。", "medium"),
    ("{company_name} 回款周期", "主要客户的平均回款周期及变化。", "medium"),
    ("{company_name} 逾期应收分析", "超信用期的应收账款金额及占比。", "medium"),
    ("{company_name} 坏账准备计提", "坏账准备计提政策及与同行对比。", "medium"),
    ("{company_name} 应收账款期后回收", "期末应收的期后回收情况。", "medium"),
    ("{company_name} 第三方回款情况", "是否存在第三方回款，原因及规范性。", "low"),
    ("{company_name} 票据结算情况", "应收票据的占比及信用风险。", "low"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "medium", "{company_name} 应收账款账龄分布",
    "应收账款账龄分布及期后回款。",
    ["账龄", "金额", "占比", "来源"]))

# 经销商详情
for t, d, p in [
    ("{company_name} 经销商数量与分布", "经销商总数、区域分布及变化。", "medium"),
    ("{company_name} 经销商准入/淘汰", "经销商准入标准、淘汰机制。", "low"),
    ("{company_name} 经销商库存水平", "经销商渠道库存水平及去化速度。", "medium"),
    ("{company_name} 经销商盈利能力", "经销商的利润空间及盈利状况。", "low"),
    ("{company_name} 返利/折让政策", "经销商返利、折让政策及执行。", "low"),
    ("{company_name} 经销商与直销价差", "经销价与直销价/终端价的差异。", "low"),
]: entries.append(F(B, BC, p, t, d))

# 出口
for t, d, p in [
    ("{company_name} 出口收入占比", "出口收入金额及占总收入比例。", "medium"),
    ("{company_name} 主要出口目的地", "前五大出口市场及收入分布。", "medium"),
    ("{company_name} 出口竞争格局", "出口市场的竞争对手及份额。", "medium"),
    ("{company_name} 出口贸易政策风险", "关税、反倾销、制裁等风险。", "medium"),
    ("{company_name} 汇率风险敞口", "主要结算币种及汇率波动影响。", "medium"),
    ("{company_name} 出口退税情况", "出口退税率及对利润的贡献。", "low"),
    ("{company_name} 海外仓/本地化布局", "海外仓储、本地化生产或服务布局。", "low"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "medium", "{company_name} 出口收入按区域",
    "各出口市场的收入及占比。",
    ["区域/国家", *PERIODS, "占比", "来源"]))

# 3.4 采购与供应链（大幅扩展）
for t, d, p in [
    ("{company_name} 采购模式概述", "采购方式、供应商管理模式。", "high"),
    ("{company_name} 采购决策流程", "采购审批权限、比价机制。", "low"),
    ("{company_name} 供应商管理制度", "供应商准入、评估、淘汰制度。", "low"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "high", "{company_name} 主要原材料采购",
    "主要原材料名称、采购额、占比及价格趋势。",
    ["原材料", *PERIODS, "占采购比", "价格趋势", "来源"]))
entries.append(D(B, BC, "high", "{company_name} 前五大供应商",
    "前五大供应商名称、采购额及占比。",
    ["供应商", *PERIODS, "占比", "来源"]))
entries.append(D(B, BC, "medium", "{company_name} 前十大供应商",
    "前十大供应商覆盖的采购占比。",
    ["供应商", "2024采购额", "占比", "来源"]))

for t, d, p in [
    ("{company_name} 供应商集中度", "前五/前十大供应商占采购比，依赖度。", "high"),
    ("{company_name} 核心原材料替代性", "核心原材料是否有替代来源。", "medium"),
    ("{company_name} 供应协议期限", "与主要供应商的协议期限及续约机制。", "medium"),
    ("{company_name} 安全库存策略", "关键原材料的安全库存策略。", "medium"),
    ("{company_name} 原材料价格波动", "主要原材料价格波动对成本的影响。", "high"),
    ("{company_name} 采购付款条件", "对主要供应商的付款周期及信用安排。", "medium"),
    ("{company_name} 进口原材料依赖", "进口原材料的占比及供应链风险。", "medium"),
    ("{company_name} 外协/外包情况", "外协外包的范围、占比及管理。", "low"),
    ("{company_name} 外协商集中度", "主要外协厂商的集中度。", "low"),
    ("{company_name} 采购与销售的客供重叠", "是否存在同时是客户和供应商的情况。", "low"),
]: entries.append(F(B, BC, p, t, d))

# 各主要原材料详情
for mat in ["原材料A", "原材料B", "原材料C"]:
    entries.append(D(B, BC, "medium",
        f"{{company_name}} {mat}采购价格趋势",
        f"{mat}的采购单价历史及预测。", TS))
    entries.append(F(B, BC, "medium",
        f"{{company_name}} {mat}供给格局",
        f"{mat}的主要供应商及市场供给情况。"))

# 3.5 生产与产能
for t, d, p in [
    ("{company_name} 生产模式概述", "自产/外协/代工模式、工艺流程概述。", "high"),
    ("{company_name} 生产工艺流程", "主要产品的生产工艺步骤及关键环节。", "medium"),
    ("{company_name} 生产工艺先进性", "工艺在行业中的先进程度及改进。", "medium"),
    ("{company_name} 主要生产基地", "生产基地位置、面积、功能分工。", "medium"),
    ("{company_name} 主要生产设备", "关键设备名称、用途、新旧程度。", "medium"),
    ("{company_name} 设备自动化水平", "自动化程度及智能制造推进情况。", "low"),
    ("{company_name} 扩产计划详情", "在建/拟建产能项目及新增产能。", "high"),
    ("{company_name} 扩产资金来源", "扩产项目的资金来源及融资安排。", "medium"),
    ("{company_name} 扩产项目风险", "扩产项目的建设风险及产能消化风险。", "medium"),
    ("{company_name} 良品率/成品率", "主要产品的良品率及改进趋势。", "medium"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "high", "{company_name} 产能与产量",
    "设计产能、实际产量及利用率。",
    ["产品", "设计产能", "2022产量", "2023产量", "2024产量", "利用率", "来源"]))
entries.append(D(B, BC, "high", "{company_name} 产能利用率趋势",
    "产能利用率变化趋势。", TS))
entries.append(D(B, BC, "medium", "{company_name} 产销率趋势",
    "产量、销量、产销率变化。", TS))
entries.append(D(B, BC, "medium", "{company_name} 在建产能进度",
    "在建产能项目投资进度。",
    ["项目", "总投资", "已投入", "进度", "预计投产", "新增产能", "来源"]))

# 3.6 研发
for t, d, p in [
    ("{company_name} 研发体系架构", "研发组织结构、研发机构设置。", "medium"),
    ("{company_name} 研发人员数量与结构", "研发人员数量、占比、学历构成。", "medium"),
    ("{company_name} 核心技术人员认定", "核心技术人员的认定标准及名单。", "medium"),
    ("{company_name} 核心技术清单", "核心技术名称、用途、取得方式。", "high"),
    ("{company_name} 核心技术与主营关系", "各核心技术与收入的关联度。", "high"),
    ("{company_name} 技术水平行业对比", "与国内外竞争对手的技术差距。", "high"),
    ("{company_name} 技术迭代路线图", "公司技术演进路线及下一代技术方向。", "medium"),
    ("{company_name} 在研项目清单", "主要在研项目名称、方向、进展。", "medium"),
    ("{company_name} 在研项目预期成果", "在研项目的商业化前景及时间表。", "medium"),
    ("{company_name} 技术许可/合作", "外部技术许可协议及合作研发安排。", "low"),
    ("{company_name} 产学研合作", "与高校/科研院所的合作项目。", "low"),
    ("{company_name} 知识产权保护措施", "专利、商业秘密保护制度。", "low"),
    ("{company_name} 核心技术人员流失风险", "核心技术人员的约束激励及竞业禁止。", "medium"),
    ("{company_name} 研发费用资本化比例", "研发支出资本化的比例及合理性。", "medium"),
    ("{company_name} 高新技术企业认定", "高新技术企业认定状态及有效期。", "low"),
]: entries.append(F(B, BC, p, t, d))

entries.append(D(B, BC, "high", "{company_name} 研发投入趋势",
    "研发费用金额、占收入比例。", TS))
entries.append(D(B, BC, "medium", "{company_name} 专利与知识产权",
    "专利数量（发明/实用新型/外观）。",
    ["类别", "数量", "核心专利", "来源"]))
entries.append(D(B, BC, "medium", "{company_name} 研发人员变化",
    "研发人员数量及占比变化。", TS))

# 3.7 质量与安全
for t, d, p in [
    ("{company_name} 质量管理体系", "质量认证（ISO等）、质控组织。", "medium"),
    ("{company_name} 质量认证证书", "已获得的质量认证清单。", "low"),
    ("{company_name} 返修率/不良率", "产品返修率、不良率及变化趋势。", "medium"),
    ("{company_name} 质量纠纷/投诉", "报告期质量投诉及纠纷情况。", "medium"),
    ("{company_name} 产品召回历史", "历史产品召回事件（如有）。", "low"),
    ("{company_name} 安全生产管理", "安全生产制度及报告期安全事故。", "low"),
]: entries.append(F(B, BC, p, t, d))

# 3.8 环保
for t, d in [
    ("{company_name} 环保合规情况", "主要污染物排放、环保设施运行。"),
    ("{company_name} 环保投入金额", "报告期环保投入金额及占收入比。"),
    ("{company_name} 环保处罚历史", "历史环保处罚（如有）。"),
    ("{company_name} 碳排放情况", "碳排放总量、单位产品排放。"),
]: entries.append(F(B, BC, "low", t, d))

# 3.9 资产状况
for t, d, p in [
    ("{company_name} 主要固定资产概况", "房产、土地、设备规模及权属。", "medium"),
    ("{company_name} 固定资产成新率", "主要设备的使用年限及成新率。", "low"),
    ("{company_name} 资产抵质押情况", "资产抵押、质押或权利限制。", "medium"),
    ("{company_name} 租赁资产情况", "租赁房产/设备的期限及金额。", "low"),
    ("{company_name} 特许经营权/资质", "必要的经营资质及有效期。", "medium"),
    ("{company_name} 商标品牌资产", "主要商标、品牌影响力。", "medium"),
    ("{company_name} 土地使用权情况", "土地面积、用途、使用年限。", "low"),
    ("{company_name} 在建工程进度", "在建工程项目、投资进度。", "medium"),
]: entries.append(F(B, BC, p, t, d))

# 3.10 重大合同
for t, d, p in [
    ("{company_name} 重大销售合同", "金额重大的销售合同及履约状态。", "medium"),
    ("{company_name} 重大采购合同", "金额重大的采购合同及履约状态。", "medium"),
    ("{company_name} 重大合作协议", "重要战略合作协议及合作内容。", "medium"),
    ("{company_name} 重大投资合同", "重大项目投资合同及执行进度。", "medium"),
    ("{company_name} 框架协议覆盖率", "框架协议覆盖的收入占比。", "low"),
]: entries.append(F(B, BC, p, t, d))

# 3.11 发展战略
for t, d, p in [
    ("{company_name} 中长期战略规划", "未来3-5年战略方向及目标。", "high"),
    ("{company_name} 战略实施路径", "战略落地的具体实施步骤和时间表。", "medium"),
    ("{company_name} 战略资源配置", "战略执行需要的资金、人才、技术资源。", "medium"),
    ("{company_name} 海外扩张战略", "国际化的目标市场及推进计划。", "medium"),
    ("{company_name} 并购战略", "并购方向、标的筛选标准及整合能力。", "medium"),
    ("{company_name} 新业务/新市场拓展", "新进入的业务领域或市场。", "medium"),
    ("{company_name} 融资/募资用途", "最近一次融资的募资用途及进展。", "medium"),
]: entries.append(F(B, BC, p, t, d))

# 3.12 ESG
for t, d in [
    ("{company_name} 环境（E）评估", "碳排放、能耗、环保投入等。"),
    ("{company_name} 社会（S）评估", "员工福利、社会责任、产品安全等。"),
    ("{company_name} 治理（G）评估", "董事会独立性、信息披露质量等。"),
    ("{company_name} ESG评级与报告", "外部ESG评级、ESG报告发布情况。"),
]: entries.append(F(B, BC, "low", t, d))

# 3.13 商业模式判断
for t, d, p in [
    ("{company_name} 商业模式是否具备扩张性", "判断产品竞争力、客户黏性、扩产路径。", "high"),
    ("{company_name} 竞争优势是否具备护城河", "判断品牌/技术/成本/网络效应/转换成本的护城河。", "high"),
    ("{company_name} 客户与供应商依赖度是否健康", "判断集中度是否在可接受范围。", "high"),
    ("{company_name} 产能扩张是否匹配需求", "判断产能与需求的匹配度。", "high"),
    ("{company_name} 技术竞争力是否可持续", "判断研发投入、技术储备是否支撑领先。", "high"),
    ("{company_name} 销售渠道是否健康", "判断渠道库存、经销商盈利、终端需求是否健康。", "medium"),
    ("{company_name} 采购成本是否可控", "判断原材料价格上涨能否有效转嫁或对冲。", "medium"),
    ("{company_name} 核心投资逻辑（Bull Case）", "支持看多的3-5个核心理由。", "high"),
    ("{company_name} 核心看空逻辑（Bear Case）", "支持看空的3-5个核心理由。", "high"),
    ("{company_name} 催化剂与时间节点", "可能推动股价上行的短期催化剂。", "medium"),
    ("{company_name} 关键假设验证指标", "需要跟踪的关键假设验证指标。", "medium"),
    ("{company_name} 战略执行可信度", "判断管理层战略规划的合理性和执行能力。", "medium"),
]: entries.append(J(B, BC, p, t, d))


# ══════════════════════════════════════════════
# 4. PEER INFO  (~60 entries)
# ══════════════════════════════════════════════
P = "peer_info"; PC = "peer_info_crew"

entries.append(F(P, PC, "high", "{company_name} 可比公司选择逻辑",
    "可比公司选取标准（产品/规模/区域/成长阶段）及排除原因。"))
entries.append(D(P, PC, "high", "{company_name} 可比公司基本信息",
    "可比公司名称、上市地、市值、主营、收入。",
    ["公司", "上市地", "市值", "主营", "2024收入", "来源"]))

# 财务指标对比（多维度）
for metric, cols in [
    ("盈利指标", ["公司", "毛利率", "净利率", "ROE", "ROIC", "来源"]),
    ("估值倍数", ["公司", "PE(TTM)", "PE(Fwd)", "PB", "EV/EBITDA", "来源"]),
    ("成长性", ["公司", "收入增速", "净利增速", "3年CAGR", "来源"]),
    ("运营效率", ["公司", "资产周转率", "存货周转天数", "应收周转天数", "来源"]),
    ("研发投入", ["公司", "研发费用率", "研发人员占比", "专利数", "来源"]),
    ("资本结构", ["公司", "资产负债率", "有息负债率", "经营现金流/净利", "来源"]),
    ("分红回购", ["公司", "分红率", "回购金额", "股息率", "来源"]),
    ("产能规模", ["公司", "产能", "利用率", "产量", "来源"]),
    ("费用结构", ["公司", "销售费率", "管理费率", "研发费率", "来源"]),
    ("现金流质量", ["公司", "经营现金流", "自由现金流", "CAPEX/收入", "来源"]),
]:
    entries.append(D(P, PC, "high" if metric in ("盈利指标", "估值倍数") else "medium",
        f"{{company_name}} 可比公司{metric}对比",
        f"可比公司{metric}对比表。", cols))

# 竞争对手深度画像
for i in range(1, 6):
    entries.append(F(P, PC, "medium",
        f"{{company_name}} 核心竞争对手{i}画像",
        f"第{i}号竞争对手的业务模式、竞争策略、优劣势。"))
    entries.append(D(P, PC, "medium",
        f"{{company_name}} 竞争对手{i}财务摘要",
        f"第{i}号竞争对手的核心财务指标。", TS))

# 可比交易（并购估值参考）
entries.append(D(P, PC, "medium", "{company_name} 可比交易分析",
    "近期行业并购交易的估值倍数。",
    ["交易", "买方", "标的", "交易金额", "估值倍数", "时间", "来源"]))

# 一致预期
entries.append(F(P, PC, "medium", "{company_name} 市场一致预期",
    "卖方一致预期的收入、利润及目标价中位数。"))

# 同行判断
for t, d, p in [
    ("{company_name} 同行可比性是否可靠", "判断同行在产品、客户、区域上的可比性。", "high"),
    ("{company_name} 相对可比公司优劣势", "与核心可比公司逐项对比。", "high"),
    ("{company_name} 估值溢折价是否合理", "相对可比公司的估值溢折价及驱动因素。", "high"),
    ("{company_name} 与一致预期的分歧点", "我们与市场一致预期的关键分歧。", "medium"),
]: entries.append(J(P, PC, p, t, d))


# ══════════════════════════════════════════════
# 5. FINANCIAL  (~250 entries)
# ══════════════════════════════════════════════
FN = "financial"; FC = "financial_crew"

# 5.1 利润表（按科目细化）
entries.append(D(FN, FC, "high", "{company_name} 利润表摘要", "核心利润表指标。", TS))

for item, p in [
    ("总收入", "high"), ("营业成本", "high"), ("毛利润", "high"),
    ("销售费用", "high"), ("管理费用", "high"), ("研发费用", "high"),
    ("财务费用", "medium"), ("营业利润", "high"), ("利润总额", "high"),
    ("所得税", "medium"), ("净利润", "high"), ("归母净利润", "high"),
    ("扣非归母净利润", "high"), ("EBITDA", "high"), ("EBIT", "medium"),
]:
    entries.append(D(FN, FC, p,
        f"{{company_name}} {item}趋势", f"{item}金额及变化趋势。", TS))

# 增速
for item in ["收入", "毛利", "营业利润", "净利润", "扣非净利"]:
    entries.append(D(FN, FC, "high",
        f"{{company_name}} {item}增速", f"{item}的同比增速。", TS))

# 利润率
for item, p in [
    ("毛利率", "high"), ("净利率", "high"), ("扣非净利率", "medium"),
    ("EBITDA利润率", "medium"), ("营业利润率", "medium"),
    ("销售费用率", "medium"), ("管理费用率", "medium"),
    ("研发费用率", "high"), ("财务费用率", "medium"),
    ("期间费用率合计", "medium"),
]:
    entries.append(D(FN, FC, p,
        f"{{company_name}} {item}趋势", f"{item}及变化趋势。", TS))

# 收入分析
for t, d, p in [
    ("{company_name} 收入变动量价拆解", "收入变动的量/价因素拆解。", "high"),
    ("{company_name} 收入季度分布", "各季度收入占比及波动规律。", "medium"),
    ("{company_name} 收入月度分布", "各月度收入分布（如有季节性）。", "low"),
    ("{company_name} 收入按产品构成", "各产品线的收入占比及变化。", "high"),
    ("{company_name} 收入按区域构成", "各区域的收入占比及变化。", "medium"),
    ("{company_name} 收入按客户类型构成", "各客户类型的收入占比。", "medium"),
    ("{company_name} 收入确认时点", "各业务的收入确认时点及政策。", "medium"),
    ("{company_name} 收入确认与同行差异", "收入确认政策与可比公司的异同。", "medium"),
    ("{company_name} 期末收入异常检查", "第四季度/12月收入占比是否异常偏高。", "medium"),
]: entries.append(F(FN, FC, p, t, d))

entries.append(D(FN, FC, "medium", "{company_name} 收入季度分布",
    "各季度收入金额及占比。",
    ["季度", *PERIODS, "占比", "来源"]))

# 成本分析
for t, d, p in [
    ("{company_name} 成本构成分析", "成本结构（原材料/人工/制造费用/折旧）。", "high"),
    ("{company_name} 单位成本变化", "单位产品成本构成及变化。", "medium"),
    ("{company_name} 成本核算方法", "成本核算方法及与同行一致性。", "medium"),
    ("{company_name} 毛利率变动归因", "毛利率变动的驱动因素拆解。", "high"),
    ("{company_name} 毛利率按产品", "各产品线毛利率及差异原因。", "high"),
    ("{company_name} 毛利率按区域", "各区域毛利率及差异原因。", "medium"),
    ("{company_name} 毛利率按销售模式", "直销/经销模式毛利率差异。", "medium"),
    ("{company_name} 毛利率与同行对比", "毛利率与可比公司的比较及差异原因。", "high"),
    ("{company_name} 毛利率季度波动", "各季度毛利率及波动原因。", "medium"),
]: entries.append(F(FN, FC, p, t, d))

entries.append(D(FN, FC, "high", "{company_name} 成本构成明细", "成本结构。",
    ["成本项", *PERIODS, "占比", "来源"]))

# 费用分析（按类别细化）
for fee_type in ["销售费用", "管理费用", "研发费用", "财务费用"]:
    entries.append(D(FN, FC, "medium",
        f"{{company_name}} {fee_type}构成",
        f"{fee_type}主要构成项及变化。", TS))
    entries.append(F(FN, FC, "medium",
        f"{{company_name}} {fee_type}变动原因",
        f"{fee_type}变动的驱动因素。"))
    entries.append(F(FN, FC, "medium",
        f"{{company_name}} {fee_type}与同行对比",
        f"{fee_type}率与可比公司的比较。"))

# 特殊费用项
for t, d in [
    ("{company_name} 广告宣传费分析", "广告费用金额、占比及效果。"),
    ("{company_name} 股份支付费用", "股份支付金额及对利润的影响。"),
    ("{company_name} 利息资本化金额", "利息资本化金额及对利润的影响。"),
]: entries.append(F(FN, FC, "low", t, d))

# 5.2 资产负债表（按科目细化）
entries.append(D(FN, FC, "high", "{company_name} 资产负债表摘要", "核心资产负债指标。", TS))

for item, p in [
    ("货币资金", "high"), ("交易性金融资产", "low"),
    ("应收票据", "medium"), ("应收账款", "high"),
    ("应收账款融资", "low"), ("预付账款", "medium"),
    ("其他应收款", "medium"), ("存货", "high"),
    ("合同资产", "medium"), ("其他流动资产", "low"),
    ("长期股权投资", "medium"), ("其他权益工具投资", "low"),
    ("投资性房地产", "low"), ("固定资产", "high"),
    ("在建工程", "medium"), ("使用权资产", "low"),
    ("无形资产", "medium"), ("开发支出", "medium"),
    ("商誉", "medium"), ("长期待摊费用", "low"),
    ("递延所得税资产", "low"),
    ("短期借款", "medium"), ("应付票据", "medium"),
    ("应付账款", "high"), ("合同负债", "medium"),
    ("应付职工薪酬", "low"), ("应交税费", "low"),
    ("其他应付款", "medium"), ("一年内到期非流动负债", "medium"),
    ("长期借款", "medium"), ("应付债券", "low"),
    ("租赁负债", "low"), ("长期应付款", "low"),
    ("预计负债", "medium"), ("递延所得税负债", "low"),
]:
    entries.append(D(FN, FC, p,
        f"{{company_name}} {item}分析", f"{item}余额、构成及变化分析。", TS))

# 资产质量深度分析
for t, d, p in [
    ("{company_name} 资产负债率趋势", "资产负债率变化趋势。", "high"),
    ("{company_name} 有息负债率", "有息负债/总资产的变化。", "high"),
    ("{company_name} 净负债率", "（有息负债-现金）/净资产。", "medium"),
    ("{company_name} 应收账款周转分析", "应收账款周转天数及变化。", "high"),
    ("{company_name} 应收账款坏账率", "坏账准备占应收账款的比例。", "medium"),
    ("{company_name} 应收账款前五大客户", "应收账款前五大欠款客户。", "medium"),
    ("{company_name} 存货周转分析", "存货周转天数及变化。", "high"),
    ("{company_name} 存货库龄分析", "存货的库龄分布。", "medium"),
    ("{company_name} 存货跌价准备", "存货跌价准备计提金额及比例。", "medium"),
    ("{company_name} 固定资产折旧政策", "折旧方法、年限、残值率。", "medium"),
    ("{company_name} 固定资产减值情况", "固定资产减值准备金额。", "low"),
    ("{company_name} 商誉减值风险", "商誉形成、减值测试及风险。", "medium"),
    ("{company_name} 商誉减值测试假设", "减值测试的关键假设（折现率、增长率等）。", "medium"),
    ("{company_name} 无形资产摊销", "无形资产摊销年限及方法。", "low"),
    ("{company_name} 开发支出资本化率", "开发支出资本化金额及比例。", "medium"),
    ("{company_name} 受限资产情况", "受限资产金额及原因。", "medium"),
    ("{company_name} 或有负债", "未在表内确认的或有负债。", "medium"),
    ("{company_name} 债务到期分布", "有息负债到期时间分布。", "medium"),
]: entries.append(F(FN, FC, p, t, d))

entries.append(D(FN, FC, "medium", "{company_name} 应收账款账龄",
    "应收账款账龄分布。",
    ["账龄", "金额", "占比", "坏账准备", "来源"]))
entries.append(D(FN, FC, "medium", "{company_name} 存货构成明细",
    "存货按原材料/在产品/产成品构成。",
    ["类别", *PERIODS, "占比", "来源"]))

# 5.3 现金流量
entries.append(D(FN, FC, "high", "{company_name} 现金流量表摘要",
    "经营/投资/筹资现金流。", TS))

for item, p in [
    ("经营活动现金流入", "medium"), ("经营活动现金流出", "medium"),
    ("经营活动净现金流", "high"),
    ("投资活动现金流入", "low"), ("投资活动现金流出", "medium"),
    ("投资活动净现金流", "medium"),
    ("筹资活动现金流入", "low"), ("筹资活动现金流出", "medium"),
    ("筹资活动净现金流", "medium"),
    ("资本开支", "high"), ("自由现金流（FCFF）", "high"),
    ("自由现金流（FCFE）", "medium"),
]:
    entries.append(D(FN, FC, p,
        f"{{company_name}} {item}趋势", f"{item}金额及变化。", TS))

for t, d, p in [
    ("{company_name} 经营现金流/净利润", "经营现金流与净利润的匹配度。", "high"),
    ("{company_name} 经营现金流偏离原因", "经营现金流与利润偏离的原因分析。", "high"),
    ("{company_name} 自由现金流趋势", "FCFF/FCFE的计算及趋势。", "high"),
    ("{company_name} 资本开支/折旧比", "CAPEX/折旧比率，判断是否处于扩张期。", "medium"),
    ("{company_name} 分红历史", "历年分红金额、分红率。", "medium"),
    ("{company_name} 分红政策", "利润分配政策及承诺。", "medium"),
    ("{company_name} 回购历史", "历年回购金额及价格区间。", "low"),
    ("{company_name} 现金流与利润表勾稽", "现金流与利润表、资产负债表的勾稽关系。", "medium"),
]: entries.append(F(FN, FC, p, t, d))

entries.append(D(FN, FC, "medium", "{company_name} 分红与回购历史",
    "历年分红、回购金额。", TS))

# 5.4 盈利能力
for item in ["ROE", "ROIC", "ROA"]:
    entries.append(D(FN, FC, "high",
        f"{{company_name}} {item}趋势", f"{item}及变化趋势。", TS))

entries.append(D(FN, FC, "medium", "{company_name} 杜邦分析",
    "净利率×周转率×杠杆的拆解。", TS))

for t, d, p in [
    ("{company_name} 盈利能力与同行对比", "ROE/净利率/毛利率等与可比公司比较。", "high"),
    ("{company_name} 超额盈利的持续性", "高于行业的盈利能力是否可持续。", "medium"),
]: entries.append(F(FN, FC, p, t, d))

# 5.5 偿债与流动性
for item in ["流动比率", "速动比率", "利息保障倍数", "现金比率"]:
    entries.append(D(FN, FC, "medium",
        f"{{company_name}} {item}趋势", f"{item}变化趋势。", TS))

for t, d in [
    ("{company_name} 偿债能力与同行对比", "偿债指标与可比公司对比。"),
    ("{company_name} 银行授信情况", "主要银行授信额度及使用情况。"),
    ("{company_name} 融资渠道多元性", "银行贷款、债券、股权等融资渠道。"),
]: entries.append(F(FN, FC, "medium", t, d))

# 5.6 营运效率
for item in ["应收周转天数", "存货周转天数", "应付周转天数", "现金转换周期"]:
    entries.append(D(FN, FC, "medium",
        f"{{company_name}} {item}趋势", f"{item}变化趋势。", TS))

entries.append(F(FN, FC, "medium", "{company_name} 营运效率与同行对比",
    "营运效率指标与可比公司对比。"))

# 5.7 税收与补贴
for t, d, p in [
    ("{company_name} 所得税率分析", "有效税率、法定税率差异及原因。", "medium"),
    ("{company_name} 税收优惠明细", "享受的各项税收优惠及金额。", "medium"),
    ("{company_name} 税收优惠持续性", "优惠到期时间、续期条件。", "medium"),
    ("{company_name} 政府补助明细", "政府补助金额及主要项目。", "medium"),
    ("{company_name} 政府补助依赖度", "政府补助占利润的比重。", "medium"),
    ("{company_name} 非经常性损益", "非经常性损益金额及构成。", "medium"),
    ("{company_name} 扣非后利润分析", "扣除非经常损益后的利润趋势。", "high"),
]: entries.append(F(FN, FC, p, t, d))

entries.append(D(FN, FC, "medium", "{company_name} 政府补助趋势", "政府补助金额及占利润比。", TS))
entries.append(D(FN, FC, "medium", "{company_name} 非经常性损益明细", "非经常损益构成。", TS))

# 5.8 会计政策与审计
for t, d in [
    ("{company_name} 会计政策关键选择", "折旧、坏账、跌价、资本化等关键估计。"),
    ("{company_name} 会计政策变更", "报告期会计政策或估计变更及影响。"),
    ("{company_name} 审计意见类型", "近三年审计意见类型。"),
    ("{company_name} 审计师变更", "报告期审计师是否变更及原因。"),
    ("{company_name} 内部控制审计", "内控审计意见及发现的缺陷。"),
]: entries.append(F(FN, FC, "medium", t, d))

# 5.9 盈利预测
entries.append(D(FN, FC, "high", "{company_name} 盈利预测假设",
    "收入增速、毛利率、费用率等核心预测假设。",
    ["假设项", "{期间3}A", "{预测期1}E", "{预测期2}E", "{预测期3}E", "依据", "来源"]))
entries.append(D(FN, FC, "high", "{company_name} 盈利预测结果",
    "预测的收入、EBITDA、净利润、EPS。",
    ["指标", "{期间3}A", "{预测期1}E", "{预测期2}E", "{预测期3}E", "单位", "来源"]))
entries.append(D(FN, FC, "medium", "{company_name} 敏感性分析",
    "关键假设变化对利润的敏感性。",
    ["假设变量", "乐观", "基准", "悲观", "来源"]))

# 5.10 估值
entries.append(D(FN, FC, "high", "{company_name} 历史估值区间",
    "历史PE/PB/EV-EBITDA区间及当前位置。",
    ["指标", "最低", "25分位", "中位数", "75分位", "最高", "当前", "来源"]))
entries.append(D(FN, FC, "high", "{company_name} 可比估值对比",
    "与可比公司估值倍数对比。", COMP))
entries.append(D(FN, FC, "medium", "{company_name} DCF估值参数",
    "DCF关键参数及估值结果。",
    ["参数", "数值", "来源"]))

for t, d in [
    ("{company_name} 估值方法选择", "适用的估值方法及理由。"),
    ("{company_name} 投资评级与目标价", "综合评级及目标价区间。"),
]: entries.append(F(FN, FC, "high", t, d))

# 5.11 财务判断
for t, d, p in [
    ("{company_name} 利润质量与现金转化是否可靠", "判断盈利质量、现金流匹配度。", "high"),
    ("{company_name} 资产负债表是否健康", "判断资本结构、流动性、资产质量。", "high"),
    ("{company_name} 财务增长的可持续性", "判断增长驱动因素是否可持续。", "high"),
    ("{company_name} 是否存在财务粉饰迹象", "判断收入确认、费用资本化等是否激进。", "high"),
    ("{company_name} 股东回报能力", "判断分红、回购能力及意愿。", "medium"),
    ("{company_name} 收入质量评估", "判断收入增长的健康度（是否依赖关联方、突击确认等）。", "high"),
    ("{company_name} 盈利预测的可信度", "判断预测假设是否审慎合理。", "medium"),
    ("{company_name} 估值是否合理", "综合各方法判断当前估值水平。", "high"),
]: entries.append(J(FN, FC, p, t, d))


# ══════════════════════════════════════════════
# 6. OPERATING METRICS  (~80 entries)
# ══════════════════════════════════════════════
O = "operating_metrics"; OC = "operating_metrics_crew"

# 产能
for t, d, p in [
    ("{company_name} 总产能（按产品）", "各产品的设计产能。", "high"),
    ("{company_name} 实际产量（按产品）", "各产品的实际产量。", "high"),
    ("{company_name} 产能利用率（按产品）", "各产品的产能利用率。", "high"),
    ("{company_name} 产能利用率变化原因", "利用率变化的驱动因素。", "medium"),
    ("{company_name} 产能扩张时间表", "在建/拟建产能的投产时间。", "high"),
    ("{company_name} 新增产能爬坡曲线", "新产能从投产到满产的预期时间。", "medium"),
    ("{company_name} 产能退出/关停", "是否有产能退出或关停计划。", "low"),
]: entries.append(F(O, OC, p, t, d))

entries.append(D(O, OC, "high", "{company_name} 产能产量汇总",
    "各产品产能、产量、利用率。",
    ["产品", "产能", "2022产量", "2023产量", "2024产量", "利用率", "来源"]))

# 产销匹配
for t, d, p in [
    ("{company_name} 产量与销量匹配", "产销率及库存变化。", "high"),
    ("{company_name} 在手订单/合同负债", "在手订单及同比变化。", "high"),
    ("{company_name} 订单交付周期", "从接单到交付的平均周期。", "medium"),
    ("{company_name} 订单取消率", "订单取消的比例及原因。", "low"),
]: entries.append(F(O, OC, p, t, d))

entries.append(D(O, OC, "high", "{company_name} 产销量趋势", "产量、销量、产销率。", TS))
entries.append(D(O, OC, "high", "{company_name} 在手订单趋势", "在手订单金额及变化。", TS))

# 价格
for t, d, p in [
    ("{company_name} 主要产品ASP（按产品）", "各产品平均售价及变化。", "high"),
    ("{company_name} ASP变化驱动因素", "ASP变化的原因（产品结构/竞争/成本等）。", "high"),
    ("{company_name} ASP与竞品对比", "主要产品ASP与竞品的对比。", "medium"),
    ("{company_name} 价格传导能力", "成本上涨时向下游传导价格的能力。", "medium"),
]: entries.append(F(O, OC, p, t, d))

entries.append(D(O, OC, "high", "{company_name} 产品ASP趋势", "各产品平均售价。", TS))

# 成本KPI
entries.append(D(O, OC, "high", "{company_name} 原材料单价趋势",
    "主要原材料单位采购价格。", TS))
entries.append(D(O, OC, "medium", "{company_name} 单位原材料消耗",
    "单位产品原材料消耗量。", TS))
entries.append(D(O, OC, "medium", "{company_name} 能耗指标",
    "水电气能源消耗量及与产量匹配。", TS))
entries.append(D(O, OC, "medium", "{company_name} 良品率/成品率趋势",
    "良品率变化趋势。", TS))

# 效率
entries.append(D(O, OC, "medium", "{company_name} 人均收入",
    "人均收入变化趋势。", TS))
entries.append(D(O, OC, "medium", "{company_name} 人均净利润",
    "人均净利润变化趋势。", TS))
entries.append(D(O, OC, "medium", "{company_name} 人均产出",
    "生产人员人均产出变化。", TS))
entries.append(D(O, OC, "medium", "{company_name} 固定资产周转率",
    "固定资产周转率变化。", TS))
entries.append(D(O, OC, "medium", "{company_name} 总资产周转率",
    "总资产周转率变化。", TS))

# 营运资本
entries.append(D(O, OC, "medium", "{company_name} 应收周转天数",
    "应收账款周转天数变化。", TS))
entries.append(D(O, OC, "medium", "{company_name} 存货周转天数",
    "存货周转天数变化。", TS))
entries.append(D(O, OC, "medium", "{company_name} 应付周转天数",
    "应付账款周转天数变化。", TS))
entries.append(D(O, OC, "medium", "{company_name} 现金转换周期",
    "现金转换周期变化。", TS))

# 投资
entries.append(D(O, OC, "medium", "{company_name} 资本开支趋势",
    "资本开支金额及占收入比。", TS))
entries.append(D(O, OC, "medium", "{company_name} 资本开支/折旧比",
    "CAPEX/折旧比率。", TS))
entries.append(D(O, OC, "medium", "{company_name} 在建工程转固进度",
    "在建工程转固的金额及时间。", TS))

# 行业特有KPI（通用模板）
for i in range(1, 11):
    entries.append(D(O, OC, "medium",
        f"{{company_name}} 行业特有KPI_{i}",
        f"行业特有的第{i}个关键运营指标（根据行业特性填充）。", TS))

# 运营判断
for t, d, p in [
    ("{company_name} 运营效率趋势是否改善", "判断利用率、交付效率、周转效率是否改善。", "high"),
    ("{company_name} 量价与扩产逻辑是否可持续", "判断产能、ASP、需求兑现能否持续。", "high"),
    ("{company_name} 运营指标与财务数据一致性", "判断产能/产量/销量与收入/成本/存货的勾稽关系。", "high"),
    ("{company_name} 运营效率与同行对比", "判断核心运营KPI相对可比公司的位置。", "medium"),
    ("{company_name} 运营杠杆分析", "判断固定成本占比及规模扩张对利润率的放大效应。", "medium"),
    ("{company_name} 产能瓶颈分析", "判断当前产能是否成为增长瓶颈。", "medium"),
]: entries.append(J(O, OC, p, t, d))


# ══════════════════════════════════════════════
# 7. RISK  (~80 entries)
# ══════════════════════════════════════════════
R = "risk"; RC = "risk_crew"

# 市场风险
for t, d, p in [
    ("{company_name} 行业下行风险", "需求下滑、周期见顶、增长放缓。", "high"),
    ("{company_name} 市场竞争加剧风险", "新进入者、价格战、份额流失。", "high"),
    ("{company_name} 技术替代风险", "技术路径变化导致产品被替代。", "high"),
    ("{company_name} 需求结构变化风险", "下游需求结构变化的风险。", "medium"),
    ("{company_name} 市场空间不及预期", "行业增速低于预期的风险。", "medium"),
    ("{company_name} 价格下行风险", "产品价格持续下降的风险。", "medium"),
    ("{company_name} 海外市场风险", "海外市场拓展不及预期的风险。", "medium"),
]: entries.append(F(R, RC, p, t, d))

# 经营风险
for t, d, p in [
    ("{company_name} 大客户流失风险", "前五大客户集中度过高、流失影响。", "high"),
    ("{company_name} 原材料涨价风险", "原材料大幅上涨且无法转嫁。", "high"),
    ("{company_name} 供应商断供风险", "核心供应商过于集中或断供。", "high"),
    ("{company_name} 产能过剩风险", "新建产能无法消化。", "medium"),
    ("{company_name} 扩产不及预期风险", "建设延期或投产后爬坡缓慢。", "medium"),
    ("{company_name} 产品质量风险", "质量问题引发召回或赔偿。", "medium"),
    ("{company_name} 安全生产风险", "安全事故导致停产。", "low"),
    ("{company_name} 新品研发失败风险", "新产品研发未达预期。", "medium"),
    ("{company_name} 技术路线选择风险", "技术路线错误导致投资浪费。", "medium"),
    ("{company_name} 核心人员流失风险", "核心技术/管理人员流失。", "medium"),
    ("{company_name} 经销商渠道风险", "经销商库存积压、退出或违规。", "medium"),
    ("{company_name} 客户信用风险", "大客户回款困难或违约。", "medium"),
    ("{company_name} 项目投资风险", "重大投资项目收益不及预期。", "medium"),
    ("{company_name} 并购整合风险", "并购标的整合失败。", "medium"),
]: entries.append(F(R, RC, p, t, d))

# 财务风险
for t, d, p in [
    ("{company_name} 应收坏账风险", "应收集中度高或账龄恶化。", "high"),
    ("{company_name} 存货跌价风险", "存货积压或市价下跌导致减值。", "medium"),
    ("{company_name} 商誉减值风险", "商誉面临减值的风险。", "medium"),
    ("{company_name} 汇率风险", "外币结算面临的汇率波动。", "medium"),
    ("{company_name} 融资/流动性风险", "短期偿债压力、再融资风险。", "medium"),
    ("{company_name} 利率风险", "利率上升对负债成本的影响。", "low"),
    ("{company_name} 担保代偿风险", "对外担保可能导致代偿。", "low"),
    ("{company_name} 财务造假/粉饰风险", "财务数据存在粉饰嫌疑的警示信号。", "high"),
]: entries.append(F(R, RC, p, t, d))

# 治理风险
for t, d, p in [
    ("{company_name} 实控人减持/变更风险", "实控人减持、质押过高或控制权变更。", "high"),
    ("{company_name} 关联交易利益输送风险", "关联交易不公允或利益输送。", "medium"),
    ("{company_name} 内部控制缺陷风险", "内控制度缺陷导致的运营风险。", "medium"),
    ("{company_name} 股权分散治理风险", "股权过于分散的治理风险。", "low"),
    ("{company_name} 家族企业治理风险", "家族控制可能带来的治理问题。", "medium"),
]: entries.append(F(R, RC, p, t, d))

# 政策与外部风险
for t, d, p in [
    ("{company_name} 产业政策变化风险", "行业政策收紧对业务的影响。", "high"),
    ("{company_name} 税收优惠取消风险", "高新认定失败、优惠到期不续。", "medium"),
    ("{company_name} 环保政策趋严风险", "环保标准提高导致成本增加。", "medium"),
    ("{company_name} 贸易摩擦/制裁风险", "国际贸易冲突对出口/供应链的影响。", "medium"),
    ("{company_name} 宏观经济下行风险", "宏观经济下行对公司的影响。", "low"),
    ("{company_name} 地缘政治风险", "地缘政治因素对供应链/市场的影响。", "medium"),
    ("{company_name} 自然灾害/疫情风险", "不可抗力对生产经营的影响。", "low"),
    ("{company_name} 数据安全/合规风险", "数据安全相关的合规要求变化。", "low"),
    ("{company_name} 反垄断风险", "反垄断调查或处罚风险（如适用）。", "low"),
]: entries.append(F(R, RC, p, t, d))

# 法律风险
for t, d, p in [
    ("{company_name} 重大诉讼/仲裁", "尚未了结的重大诉讼及可能影响。", "medium"),
    ("{company_name} 知识产权纠纷", "专利/商标侵权纠纷。", "medium"),
    ("{company_name} 合同履约风险", "重大合同的违约或履约不确定性。", "medium"),
    ("{company_name} 违规处罚风险", "因违反法规可能受到的处罚。", "medium"),
]: entries.append(F(R, RC, p, t, d))

entries.append(D(R, RC, "medium", "{company_name} 重大诉讼清单",
    "尚未了结的诉讼/仲裁事项。",
    ["案件", "对手方", "涉案金额", "进展", "预计影响", "来源"]))
entries.append(D(R, RC, "medium", "{company_name} 重大合同清单",
    "主要在执行合同的履约情况。",
    ["合同类型", "对手方", "金额", "状态", "风险点", "来源"]))

# 风险判断
for t, d, p in [
    ("{company_name} 关键风险清单", "梳理经营、财务、治理、外部的关键风险。", "high"),
    ("{company_name} 风险触发条件与监控", "每类风险的触发条件及领先指标。", "high"),
    ("{company_name} 下行情景分析", "关键风险同时发生时对收入/利润/估值的影响。", "high"),
    ("{company_name} 风险缓释措施评估", "公司已采取的风险缓释措施及有效性。", "medium"),
    ("{company_name} 风险收益比评估", "当前股价是否充分反映了主要风险。", "high"),
    ("{company_name} 最大单一风险因素", "对公司投资价值威胁最大的单一风险。", "high"),
]: entries.append(J(R, RC, p, t, d))


# ══════════════════════════════════════════════
# 8. SUPPLEMENTARY ENTRIES - 补充条目以达到1000+
# ══════════════════════════════════════════════

# ---- 8.1 Business: 按行业类型的通用细分维度 ----

# 销售收入真实性核查（买方视角）
for t, d, p in [
    ("{company_name} 收入与运费匹配性", "销售收入与运费的勾稽关系是否匹配。", "medium"),
    ("{company_name} 收入与产量匹配性", "收入变化与产量变化是否匹配。", "medium"),
    ("{company_name} 收入与库存变化匹配性", "收入与库存增减的勾稽关系。", "medium"),
    ("{company_name} 收入与预收款匹配性", "预收账款/合同负债与收入变化的匹配。", "medium"),
    ("{company_name} 收入与现金回款匹配性", "销售回款与收入确认的匹配关系。", "high"),
    ("{company_name} 收入增长是否依赖关联方", "关联交易收入占比是否在上升。", "high"),
    ("{company_name} 新增客户贡献的收入", "新客户贡献收入占比及可持续性。", "medium"),
    ("{company_name} 老客户收入增速", "存量客户的收入同比变化。", "medium"),
]: entries.append(F(B, BC, p, t, d))

# 采购成本真实性
for t, d, p in [
    ("{company_name} 采购与产量匹配性", "原材料采购量与产品产量的匹配关系。", "medium"),
    ("{company_name} 采购与存货变动匹配性", "采购金额与存货变动的勾稽。", "medium"),
    ("{company_name} 采购与应付账款匹配性", "采购与应付账款变动的匹配。", "medium"),
    ("{company_name} 采购价格与市场价对比", "主要原材料采购价与市场价的对比。", "medium"),
    ("{company_name} 采购退货情况", "是否存在大规模采购退货及原因。", "low"),
]: entries.append(F(B, BC, p, t, d))

# 各业务线/产品线更细颗粒度（补充5条×6维度=30条）
for dim in ["产品F", "产品G", "产品H", "产品I", "产品J"]:
    for t, d, p in [
        (f"{{company_name}} {dim}收入与毛利", f"{dim}的收入金额、增速及毛利率。", "medium"),
        (f"{{company_name}} {dim}客户结构", f"{dim}的客户类型及集中度。", "medium"),
        (f"{{company_name}} {dim}竞争格局", f"{dim}所在细分市场的竞争格局。", "medium"),
        (f"{{company_name}} {dim}增长预期", f"{dim}的未来增长潜力及驱动因素。", "medium"),
        (f"{{company_name}} {dim}生命周期", f"{dim}处于哪个生命周期阶段。", "low"),
        (f"{{company_name}} {dim}替代风险", f"{dim}面临的技术/产品替代风险。", "low"),
    ]: entries.append(F(B, BC, p, t, d))

# 客户维度细化
for i in range(1, 11):
    entries.append(F(B, BC, "medium",
        f"{{company_name}} 第{i}大客户画像",
        f"第{i}大客户的行业、规模、合作历史及未来合作预期。"))

# 供应商维度细化
for i in range(1, 6):
    entries.append(F(B, BC, "medium",
        f"{{company_name}} 第{i}大供应商画像",
        f"第{i}大供应商的行业地位、供应能力及替代可能性。"))

# 区域维度细化
for region in ["华东", "华南", "华北", "华中", "西南", "西北", "东北", "海外-北美", "海外-欧洲", "海外-东南亚", "海外-其他"]:
    entries.append(F(B, BC, "low",
        f"{{company_name}} {region}区域市场分析",
        f"{region}区域的收入占比、增速及竞争格局。"))

# ---- 8.2 Financial: 更多细分财务指标 ----

# 各季度利润率波动
for q in ["Q1", "Q2", "Q3", "Q4"]:
    entries.append(D(FN, FC, "low",
        f"{{company_name}} {q}财务指标",
        f"第{q}的收入、毛利率、净利率等指标。", TS))

# 月度运营数据（如公司披露）
for m in ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"]:
    entries.append(D(FN, FC, "low",
        f"{{company_name}} {m}收入数据",
        f"{m}的收入金额（如公司披露月度数据）。", TS))

# 各子公司财务贡献
for i in range(1, 6):
    entries.append(D(FN, FC, "medium",
        f"{{company_name}} 子公司{i}财务摘要",
        f"第{i}号主要子公司的收入、净利润贡献。", TS))

# 对外投资详情
for t, d, p in [
    ("{company_name} 长期股权投资明细", "各长期股权投资的对象、金额及收益。", "medium"),
    ("{company_name} 交易性金融资产", "交易性金融资产的构成及收益。", "low"),
    ("{company_name} 委托理财情况", "委托理财金额、收益率及风险。", "low"),
    ("{company_name} 衍生品使用情况", "是否使用衍生品对冲及品种规模。", "low"),
    ("{company_name} 对外担保明细", "对外担保对象、金额及风险。", "medium"),
    ("{company_name} 或有事项明细", "未确认的或有负债清单。", "medium"),
    ("{company_name} 承诺事项明细", "尚未履行的重大承诺事项。", "medium"),
]: entries.append(F(FN, FC, p, t, d))

# 财务比率完整清单（补充更多比率）
for ratio, p in [
    ("毛利率/同行中位数比", "medium"),
    ("净利率/同行中位数比", "medium"),
    ("ROE/同行中位数比", "medium"),
    ("应收周转/同行中位数比", "medium"),
    ("存货周转/同行中位数比", "medium"),
    ("经营现金流/收入比", "medium"),
    ("CAPEX/经营现金流比", "medium"),
    ("有息负债/EBITDA", "medium"),
    ("净负债/EBITDA", "medium"),
    ("股息支付率", "medium"),
    ("每股收益（EPS）", "high"),
    ("每股净资产（BPS）", "medium"),
    ("每股经营现金流", "medium"),
    ("每股自由现金流", "medium"),
    ("每股分红", "medium"),
    ("市盈率（PE）", "high"),
    ("市净率（PB）", "medium"),
    ("市销率（PS）", "medium"),
    ("EV/EBITDA", "high"),
    ("EV/Sales", "medium"),
    ("PEG", "medium"),
]:
    entries.append(D(FN, FC, p,
        f"{{company_name}} {ratio}趋势",
        f"{ratio}的历史趋势。", TS))

# ---- 8.3 Industry: 更多行业数据点 ----

# 行业定量指标
for t, d, p in [
    ("{industry} 行业平均毛利率", "行业平均毛利率水平及变化趋势。", "medium"),
    ("{industry} 行业平均净利率", "行业平均净利率水平。", "medium"),
    ("{industry} 行业平均ROE", "行业平均ROE水平。", "medium"),
    ("{industry} 行业平均研发费率", "行业平均研发投入占比。", "medium"),
    ("{industry} 行业平均PE", "行业平均估值倍数。", "medium"),
    ("{industry} 行业总产能", "行业总产能规模及变化。", "medium"),
    ("{industry} 行业产能利用率", "行业平均产能利用率。", "medium"),
    ("{industry} 行业库存水平", "行业整体库存水平及变化趋势。", "medium"),
    ("{industry} 行业价格指数", "行业产品价格指数及变化。", "medium"),
    ("{industry} 行业出口规模", "行业出口总量/金额及变化。", "medium"),
    ("{industry} 行业进口规模", "行业进口总量/金额及变化。", "medium"),
    ("{industry} 行业固定资产投资", "行业固定资产投资额及增速。", "medium"),
    ("{industry} 行业从业人员", "行业从业人员总量及变化趋势。", "low"),
    ("{industry} 行业平均薪酬", "行业从业人员平均薪酬水平。", "low"),
]: entries.append(F(I, IC, p, t, d))

for t, d in [
    ("{industry} 行业毛利率趋势", "行业平均毛利率历史数据。"),
    ("{industry} 行业产能变化趋势", "行业总产能历史及预测。"),
    ("{industry} 行业价格趋势", "行业产品价格历史走势。"),
    ("{industry} 行业进出口趋势", "行业进出口金额历史数据。"),
]: entries.append(D(I, IC, "medium", t, d, TS))

# 各竞争对手近况
for i in range(6, 11):
    entries.append(F(I, IC, "low",
        f"{{industry}} 竞争对手{i}概况",
        f"第{i}号竞争对手的基本情况及最新动态。"))

# ---- 8.4 Peer Info: 更多可比维度 ----

# 可比公司个股详情
for i in range(1, 11):
    entries.append(F(P, PC, "medium",
        f"{{company_name}} 可比公司{i}业务概况",
        f"第{i}号可比公司的主营业务、竞争策略及最新动态。"))

# 更多可比指标
for metric, cols in [
    ("毛利率趋势", ["公司", *PERIODS, "来源"]),
    ("净利率趋势", ["公司", *PERIODS, "来源"]),
    ("ROE趋势", ["公司", *PERIODS, "来源"]),
    ("收入增速趋势", ["公司", *PERIODS, "来源"]),
    ("资产负债率趋势", ["公司", *PERIODS, "来源"]),
    ("市值变化", ["公司", "1年前", "6月前", "当前", "来源"]),
    ("股价表现", ["公司", "近1月", "近3月", "近1年", "YTD", "来源"]),
]:
    entries.append(D(P, PC, "medium",
        f"{{company_name}} 可比公司{metric}对比",
        f"可比公司{metric}对比表。", cols))

# ---- 8.5 Operating Metrics: 更多行业特有KPI ----

# 更多通用KPI模板
for i in range(11, 21):
    entries.append(D(O, OC, "low",
        f"{{company_name}} 行业特有KPI_{i}",
        f"行业特有的第{i}个关键运营指标。", TS))

# 月度/季度KPI
for q in ["Q1", "Q2", "Q3", "Q4"]:
    entries.append(D(O, OC, "low",
        f"{{company_name}} {q}产量数据",
        f"第{q}的主要产品产量。", TS))
    entries.append(D(O, OC, "low",
        f"{{company_name}} {q}出货量数据",
        f"第{q}的主要产品出货量。", TS))
    entries.append(D(O, OC, "low",
        f"{{company_name}} {q}ASP数据",
        f"第{q}的主要产品平均售价。", TS))

# 效率变化深度分析
for t, d, p in [
    ("{company_name} 运费/收入比趋势", "运费占收入比例变化趋势。", "medium"),
    ("{company_name} 电力/产量比趋势", "单位产品电力消耗变化。", "medium"),
    ("{company_name} 人工成本/收入比", "人工成本占收入比例变化。", "medium"),
    ("{company_name} 折旧/收入比", "折旧占收入比例变化。", "medium"),
    ("{company_name} 维修费用趋势", "设备维修费用及占比变化。", "low"),
    ("{company_name} 研发成果转化率", "研发投入到产品商业化的转化效率。", "medium"),
    ("{company_name} 人均研发产出", "研发人员人均专利/论文/产品产出。", "low"),
    ("{company_name} 设备OEE", "关键设备综合效率（如适用）。", "low"),
]: entries.append(F(O, OC, p, t, d))

# ---- 8.6 History: 更多治理与合规细节 ----

for t, d, p in [
    ("{company_name} 董事会会议频率", "报告期董事会召开次数及出席率。", "low"),
    ("{company_name} 股东大会重要决议", "近年股东大会的重要决议事项。", "low"),
    ("{company_name} 独立董事意见", "独立董事是否发表过非标准意见。", "medium"),
    ("{company_name} 内部审计发现", "内部审计发现的主要问题及整改。", "low"),
    ("{company_name} 信息系统建设", "ERP/MES等信息系统建设情况。", "low"),
    ("{company_name} 数据安全管理", "数据安全制度及合规措施。", "low"),
    ("{company_name} 社会责任报告", "CSR报告发布情况及主要内容。", "low"),
    ("{company_name} 投资者关系管理", "IR活动频次、投资者沟通渠道。", "low"),
    ("{company_name} 媒体关注度", "主要媒体报道及舆情监控。", "low"),
    ("{company_name} 行业协会参与", "公司在行业协会中的地位及参与度。", "low"),
    ("{company_name} 标准制定参与", "公司是否参与行业标准制定。", "low"),
    ("{company_name} 政府关系", "与地方/行业主管部门的关系。", "low"),
]: entries.append(F(H, HC, p, t, d))

# ---- 8.7 Risk: 更多细分风险条目 ----

for t, d, p in [
    ("{company_name} 产品单一化风险", "收入过度依赖单一产品的风险。", "medium"),
    ("{company_name} 区域集中风险", "收入过度集中于单一区域的风险。", "medium"),
    ("{company_name} 行业单一化风险", "下游应用过度集中于单一行业。", "medium"),
    ("{company_name} 关键设备依赖风险", "关键设备故障或无法采购的风险。", "low"),
    ("{company_name} 供应链国产化风险", "被要求国产替代或供应链脱钩的风险。", "medium"),
    ("{company_name} 产品认证风险", "关键产品认证不通过或被撤销。", "medium"),
    ("{company_name} 股东减持压力", "大股东或机构集中减持的压力。", "medium"),
    ("{company_name} 股权质押平仓风险", "控股股东质押比例过高的平仓风险。", "medium"),
    ("{company_name} 管理层更迭风险", "管理层集中换届或突然离职的风险。", "medium"),
    ("{company_name} 信用评级下调风险", "信用评级被下调的可能性及影响。", "low"),
    ("{company_name} 商业秘密泄露风险", "核心技术或商业秘密泄露的风险。", "medium"),
    ("{company_name} 客户集中度恶化风险", "客户集中度进一步提高的风险。", "medium"),
    ("{company_name} 行业监管趋严风险", "行业监管政策变化的潜在影响。", "medium"),
    ("{company_name} 竞品降价风险", "竞争对手发起价格战的风险。", "medium"),
    ("{company_name} 新进入者冲击风险", "新进入者（包括跨界）的竞争冲击。", "medium"),
    ("{company_name} 技术封锁/制裁风险", "核心技术或设备被限制获取的风险。", "medium"),
    ("{company_name} 出口目的国政策风险", "出口目的国政策变化导致市场关闭。", "medium"),
    ("{company_name} 原材料依赖进口风险", "关键原材料进口渠道被限制。", "medium"),
    ("{company_name} 人才招聘困难风险", "关键岗位人才招聘困难的风险。", "low"),
    ("{company_name} 品牌声誉风险", "品牌负面事件导致声誉受损。", "medium"),
]: entries.append(F(R, RC, p, t, d))

# 风险量化
entries.append(D(R, RC, "high", "{company_name} 风险影响矩阵",
    "各风险的概率、影响程度及优先级。",
    ["风险项", "发生概率", "影响程度", "优先级", "监控指标", "来源"]))
entries.append(D(R, RC, "medium", "{company_name} 压力测试结果",
    "关键变量压力测试对利润的影响。",
    ["压力变量", "压力幅度", "对收入影响", "对净利影响", "对估值影响", "来源"]))


# ══════════════════════════════════════════════
# 9. 从xlsx触发的关键细节补充
# ══════════════════════════════════════════════

# ---- 9.1 股权关键细节（来自xlsx第1章1-3~1-7节）----

for t, d, p in [
    ("{company_name} 股东出资方式", "各股东是货币出资还是非货币出资（技术/实物/土地等）。", "medium"),
    ("{company_name} 非货币出资评估", "非货币出资的评估依据及公允性。", "low"),
    ("{company_name} 是否存在出资瑕疵", "是否存在出资不实、虚假出资、抽逃出资。", "high"),
    ("{company_name} 是否存在代持股", "是否存在委托持股、信托持股、股权代持。", "high"),
    ("{company_name} 股东持股是否存在权属争议", "股东持股是否存在权属纠纷或潜在争议。", "high"),
    ("{company_name} 控制权认定依据", "实际控制人的认定逻辑（持股/协议/其他）。", "high"),
    ("{company_name} 控制权稳定性", "控制权最近三年是否变化，未来是否有潜在变动。", "high"),
    ("{company_name} 实际控制人控制的其他企业", "实控人控制的所有企业清单及主营。", "medium"),
    ("{company_name} 股东违法违规记录", "控股股东、实控人是否存在重大违法违规记录。", "high"),
    ("{company_name} 股东诉讼/仲裁情况", "控股股东、实控人涉及的诉讼仲裁。", "medium"),
    ("{company_name} 股东失信记录", "控股股东、实控人是否存在失信被执行人记录。", "high"),
    ("{company_name} 违规入股情况", "是否存在违规入股、入股价格异常。", "medium"),
    ("{company_name} 私募基金股东备案", "股东中私募基金的备案及管理人登记情况。", "low"),
    ("{company_name} 股东间利益安排", "股东之间是否存在特殊利益安排（如优先清算权）。", "medium"),
    ("{company_name} 重要承诺及履行", "控股股东、实控人、董监高作出的重要承诺及履行情况。", "medium"),
]: entries.append(F(H, HC, p, t, d))

# ---- 9.2 历史关键细节（来自xlsx 1-1~1-2节）----

for t, d, p in [
    ("{company_name} 改制重组方案", "改制/重组时的业务、资产、人员安排方案。", "medium"),
    ("{company_name} 改制时资产评估", "改制时的资产评估方法及估值依据。", "medium"),
    ("{company_name} 国有资产处置合规", "如涉及国有资产，处置程序是否合规。", "medium"),
    ("{company_name} 重大重组对业绩的影响", "重组前后的业绩对比及影响程度。", "medium"),
    ("{company_name} 历史重大对外投资", "历史上的重大对外投资项目及收益情况。", "medium"),
    ("{company_name} 历史上的业务剥离", "历史上剥离的业务及剥离原因。", "medium"),
    ("{company_name} 前次融资的估值", "历次融资的投前/投后估值变化。", "high"),
]: entries.append(F(H, HC, p, t, d))

entries.append(D(H, HC, "medium", "{company_name} 历次融资估值变化",
    "历次融资的时间、金额及对应估值。",
    ["时间", "融资类型", "金额", "投后估值", "来源"]))

# ---- 9.3 法律关键细节（来自xlsx第7章）----

for t, d, p in [
    ("{company_name} 报告期行政处罚", "报告期内受到的行政处罚及整改情况。", "high"),
    ("{company_name} 税务合规情况", "是否存在补税、欠税、税务争议。", "medium"),
    ("{company_name} 海关合规情况", "进出口是否存在海关违规或纠纷。", "low"),
    ("{company_name} 工商违规情况", "是否存在工商违规记录。", "medium"),
    ("{company_name} 劳动争议情况", "是否存在重大劳动纠纷或仲裁。", "low"),
    ("{company_name} 产品责任风险", "产品责任诉讼或潜在产品责任风险。", "medium"),
    ("{company_name} 环保合规处罚", "环保违规处罚历史及整改情况。", "medium"),
    ("{company_name} 安全生产处罚", "安全生产违规处罚历史。", "low"),
    ("{company_name} 专利侵权风险评估", "是否存在被诉或涉嫌侵犯他人专利的风险。", "medium"),
    ("{company_name} 商标侵权风险评估", "商标是否存在侵权争议。", "low"),
    ("{company_name} 重大担保/反担保", "为外部主体提供的担保及反担保安排。", "medium"),
    ("{company_name} 关联方担保情况", "控股股东或关联方是否为公司提供担保。", "medium"),
]: entries.append(F(R, RC, p, t, d))

# ---- 9.4 财务关键细节（来自xlsx第3章未覆盖的）----

for t, d, p in [
    ("{company_name} 货币资金受限情况", "受限货币资金金额及原因（保证金/冻结等）。", "medium"),
    ("{company_name} 大额资金流水异常", "是否存在大额异常资金流转。", "high"),
    ("{company_name} 资金被关联方占用", "是否存在关联方非经营性资金占用。", "high"),
    ("{company_name} 财务公司/资金池", "是否参与关联方财务公司或资金池。", "medium"),
    ("{company_name} 应收票据背书/贴现", "应收票据背书及贴现的规模及占比。", "low"),
    ("{company_name} 预计负债详情", "预计负债的确认依据及估计方法。", "medium"),
    ("{company_name} 租赁负债分析", "使用权资产及租赁负债的规模。", "low"),
    ("{company_name} 递延所得税分析", "递延所得税资产/负债的构成。", "low"),
    ("{company_name} 资产负债表日后事项", "资产负债表日后发生的重大事项。", "medium"),
    ("{company_name} 境内外报表差异", "如有多套报表，各报表之间的差异。", "low"),
    ("{company_name} 合并报表范围变动", "报告期合并范围变动及原因。", "medium"),
    ("{company_name} 少数股东权益分析", "少数股东权益的规模及利润分配。", "medium"),
    ("{company_name} 财务信息与非财务信息印证", "财务数据与运营数据、行业数据的交叉验证。", "high"),
    ("{company_name} 产量与原材料/水电费匹配", "产量与原材料消耗、水电费的匹配关系。", "high"),
    ("{company_name} 销量与运费匹配", "销量与运费的匹配关系。", "medium"),
    ("{company_name} 毛利率与工艺改进匹配", "毛利率变化与工艺改进、原材料价格的匹配。", "medium"),
    ("{company_name} 经营模式与财报数据匹配", "经营模式与财务报表数据的一致性。", "medium"),
    ("{company_name} 业绩对赌完成情况", "如有对赌，精准完成是否存在人为调节嫌疑。", "high"),
]: entries.append(F(FN, FC, p, t, d))

# ---- 9.5 业务关键细节（来自xlsx第2章未覆盖的）----

for t, d, p in [
    ("{company_name} 销售收入异常增长检查", "期末(Q4/12月)收入是否异常偏高。", "high"),
    ("{company_name} 销售退回分析", "报告期销售退回的金额、占比及原因。", "medium"),
    ("{company_name} 售后服务体系", "售后服务组织、费用及客户服务能力。", "medium"),
    ("{company_name} 供应链金融安排", "是否涉及供应链金融，模式及规模。", "low"),
    ("{company_name} 客户/供应商重叠分析", "既是客户又是供应商的情况及原因。", "medium"),
    ("{company_name} 异常客户识别", "是否存在新增即大额、成立时间短、注册地异常等可疑客户。", "high"),
    ("{company_name} 异常供应商识别", "是否存在成立时间短、注册地异常、前员工开设等可疑供应商。", "high"),
    ("{company_name} 现金交易占比", "现金收付交易的占比及必要性。", "medium"),
    ("{company_name} 偶发交易分析", "非经常性的偶发交易及其合理性。", "low"),
    ("{company_name} 发行人/第三方数据引用可靠性", "引用的第三方行业数据的来源可信度。", "medium"),
    ("{company_name} 行业专家/协会意见", "行业专家或协会对公司的评价。", "low"),
]: entries.append(F(B, BC, p, t, d))

# ---- 9.6 行业关键数据（来自xlsx 2-1节）----

for t, d, p in [
    ("{industry} 行业主要产品价格走势", "行业核心产品价格近5年走势。", "high"),
    ("{industry} 行业产能扩张情况", "行业近年新建/在建/拟建产能汇总。", "high"),
    ("{industry} 行业库存周期位置", "行业当前库存水平及所处库存周期阶段。", "medium"),
    ("{industry} 行业集中度变化趋势", "CR3/CR5近5年变化及驱动因素。", "medium"),
    ("{industry} 行业平均产能利用率趋势", "行业平均利用率及与价格的关系。", "medium"),
    ("{industry} 行业供需缺口", "行业供给与需求之间的缺口及趋势。", "high"),
    ("{industry} 行业主要上市公司近期业绩", "主要上市公司最新季度业绩表现。", "medium"),
]: entries.append(F(I, IC, p, t, d))

entries.append(D(I, IC, "high", "{industry} 行业产品价格走势",
    "行业核心产品价格历史数据。", TS))
entries.append(D(I, IC, "high", "{industry} 行业供需平衡表",
    "行业总供给、总需求及缺口。",
    ["年份", "总供给", "总需求", "缺口", "单位", "来源"]))
entries.append(D(I, IC, "medium", "{industry} 行业产能扩张计划",
    "行业主要企业的在建/拟建产能汇总。",
    ["企业", "项目", "新增产能", "预计投产", "来源"]))


# ══════════════════════════════════════════════
# Assign IDs & Output
# ══════════════════════════════════════════════
abbr_map = {
    "history": "HIS", "industry": "IND", "business": "BUS",
    "peer_info": "PEER", "financial": "FIN",
    "operating_metrics": "OPS", "risk": "RISK",
}
prefix_map = {"fact": "F", "data": "D", "judgment": "J"}
counters = defaultdict(int)

for e in entries:
    t = e["topic"]
    et = e["entry_type"]
    key = (prefix_map[et], abbr_map[t])
    counters[key] += 1
    e["entry_id"] = f"{key[0]}_{key[1]}_{counters[key]:03d}"

# Validate uniqueness
ids = [e["entry_id"] for e in entries]
assert len(ids) == len(set(ids)), f"Duplicate IDs: {[x for x in ids if ids.count(x) > 1]}"

# Stats
print(f"Total entries: {len(entries)}", file=sys.stderr)
topic_counts = Counter(e["topic"] for e in entries)
for t, c in sorted(topic_counts.items()):
    print(f"  {t}: {c}", file=sys.stderr)
type_counts = Counter(e["entry_type"] for e in entries)
for t, c in sorted(type_counts.items()):
    print(f"  {t}: {c}", file=sys.stderr)

# Write YAML
class LiteralStr(str):
    pass

def literal_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

yaml.add_representer(LiteralStr, literal_str_representer)

for e in entries:
    for k in ("title", "description"):
        if k in e:
            e[k] = LiteralStr(e[k])

document = {
    "document_metadata": {
        "periods": {
            "{期间1}": "报告期第1年",
            "{期间2}": "报告期第2年",
            "{期间3}": "报告期第3年",
            "{期间4}": "报告期最近一季的上一年可比期间",
            "{期间5}": "报告期最近一季",
        },
        "forecast_periods": {
            "{预测期1}": "预测第1年",
            "{预测期2}": "预测第2年",
            "{预测期3}": "预测第3年",
        },
        "note": "使用时根据目标公司的实际报告期替换占位符。例如期间1=2021, 期间2=2022, 期间3=2023, 期间4=2024H1, 期间5=2025H1。",
    },
    "entries": entries,
}

with open("src/automated_research_report_generator/flow/config/registry_template.yaml", "w", encoding="utf-8") as f:
    yaml.dump(document, f, allow_unicode=True, default_flow_style=False, sort_keys=False, width=120)

print("Done!", file=sys.stderr)
