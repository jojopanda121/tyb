from __future__ import annotations

import json

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

# 设计目的：提供一个足够轻量的财务归一化工具，让财务 agent 不必手算基础比率。
# 模块功能：读取分期间财务 JSON，统一数值格式，并计算常见指标。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：`data_json`。
# 默认参数及原因：缺值统一保留为 `None`，原因是比随意补 0 更不容易误导估值和 QA。


class FinancialModelInput(BaseModel):
    """
    设计目的：定义财务归一化工具的输入格式。
    模块功能：约束调用方按“期间 -> 字段”的 JSON 结构传值。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`data_json`。
    默认参数及原因：无默认业务数据，原因是不同公司和期间结构差异较大。
    """

    data_json: str = Field(
        ...,
        description=(
            "JSON object keyed by period. Each period may contain revenue, gross_profit, "
            "net_income, equity, cfo, capex, debt, cash, and invested_capital."
        ),
    )


class FinancialModelTool(BaseTool):
    """
    设计目的：把常用财务归一化计算收口成可复用工具。
    模块功能：计算增长、利润率、ROE、ROIC、FCF 和现金转换率。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：输入 JSON 的期间和字段内容。
    默认参数及原因：输出保持 JSON 字符串，原因是 agent 和任务上下文更容易直接消费。
    """

    name: str = "financial_model_tool"
    description: str = (
        "Normalize period-based financial data and compute common ratios such as growth, "
        "gross margin, net margin, ROE, ROIC, and free cash flow."
    )
    args_schema: type[BaseModel] = FinancialModelInput

    def _run(self, data_json: str) -> str:
        """
        设计目的：把原始财务字段标准化为后续研究和估值都能直接引用的指标表。
        模块功能：逐期间计算利润率、回报率、自由现金流和收入增长。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`data_json`。
        默认参数及原因：输出保持 JSON 字符串，原因是 agent 和任务上下文更容易直接消费。
        """

        raw = json.loads(data_json)
        periods = sorted(raw.keys())
        normalized: dict[str, dict[str, float | None]] = {}

        for period in periods:
            row = raw.get(period, {}) or {}
            revenue = _to_float(row.get("revenue"))
            gross_profit = _to_float(row.get("gross_profit"))
            net_income = _to_float(row.get("net_income"))
            equity = _to_float(row.get("equity"))
            cfo = _to_float(row.get("cfo"))
            capex = _to_float(row.get("capex"))
            invested_capital = _to_float(row.get("invested_capital"))

            normalized[period] = {
                "revenue": revenue,
                "gross_margin": _safe_div(gross_profit, revenue),
                "net_margin": _safe_div(net_income, revenue),
                "roe": _safe_div(net_income, equity),
                "roic": _safe_div(net_income, invested_capital),
                "fcf": None if cfo is None or capex is None else cfo - capex,
                "cash_conversion": _safe_div(cfo, net_income),
            }

        for idx, period in enumerate(periods):
            revenue = normalized[period]["revenue"]
            if idx == 0:
                normalized[period]["revenue_growth"] = None
                continue
            prior_revenue = normalized[periods[idx - 1]]["revenue"]
            normalized[period]["revenue_growth"] = _growth(revenue, prior_revenue)

        return json.dumps(normalized, ensure_ascii=False, indent=2)


def _to_float(value: object) -> float | None:
    """
    设计目的：统一财务字段的数值转换方式。
    模块功能：把可解析值转成 `float`，无法解析时返回 `None`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`value`。
    默认参数及原因：错误时返回 `None` 而不是抛错，原因是原始财务数据常有空值或字符串占位。
    """

    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    """
    设计目的：避免财务比率计算里反复写空值和除零判断。
    模块功能：在分子分母都有效时返回比值，否则返回 `None`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`numerator` 和 `denominator`。
    默认参数及原因：分母为 0 或缺值时返回 `None`，原因是此时比率没有可靠含义。
    """

    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _growth(current: float | None, previous: float | None) -> float | None:
    """
    设计目的：统一收入增长率计算口径。
    模块功能：按 `(current - previous) / previous` 计算增长率。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`current` 和 `previous`。
    默认参数及原因：前值缺失或为 0 时返回 `None`，原因是这种场景下增长率不可靠。
    """

    if current is None or previous in (None, 0):
        return None
    return (current - previous) / previous
