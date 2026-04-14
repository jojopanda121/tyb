from __future__ import annotations

import json
import math

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

# 设计目的：把相对估值和简化 DCF 估值收敛为两个可复用工具，避免估值 crew 在 prompt 中重复描述计算过程。
# 模块功能：计算可比倍数、统计中位数，并运行简化版 DCF。
# 实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
# 可调参数：可比估值的 `peers_json`，以及 DCF 的增长率、折现率、终值增长率和预测年数。
# 默认参数及原因：DCF 默认预测 5 年，原因是兼顾可解释性和常见 buy-side 建模习惯。


class ComparableValuationInput(BaseModel):
    """
    设计目的：定义可比估值工具的输入格式。
    模块功能：约束调用方按 peer 列表 JSON 传入原始可比数据。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`peers_json`。
    默认参数及原因：无默认 peer 数据，原因是每家公司可比池都不同。
    """

    peers_json: str = Field(
        ...,
        description=(
            "JSON list of peer dicts. Each row may include company, market_cap, enterprise_value, "
            "revenue, ebitda, net_income, and book_value."
        ),
    )


class ComparableValuationTool(BaseTool):
    """
    设计目的：把原始 peer 数据快速转成可比较的估值倍数表。
    模块功能：计算 EV/Revenue、EV/EBITDA、PE、PB 和各指标中位数。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`peers_json` 的字段内容。
    默认参数及原因：只输出中位数摘要，原因是当前 v2 更强调稳健基准而不是复杂统计。
    """

    name: str = "valuation_comparable_tool"
    description: str = (
        "Build normalized comparable trading multiples and median summary statistics from peer data."
    )
    args_schema: type[BaseModel] = ComparableValuationInput

    def _run(self, peers_json: str) -> str:
        """
        设计目的：快速把原始 peer 数据转换成可比较的倍数表。
        模块功能：逐个 peer 计算 EV/Revenue、EV/EBITDA、PE 和 PB，并给出中位数摘要。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：`peers_json`。
        默认参数及原因：只输出中位数，不输出更复杂统计，原因是当前 v2 先追求稳健的基准估值。
        """

        try:
            peers = json.loads(peers_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"peers_json is not valid JSON: {exc}") from exc
        if not isinstance(peers, list):
            raise ValueError("peers_json must be a JSON array.")
        enriched: list[dict[str, object]] = []
        metrics: dict[str, list[float]] = {
            "ev_revenue": [],
            "ev_ebitda": [],
            "pe": [],
            "pb": [],
        }

        for peer in peers:
            enterprise_value = _to_float(peer.get("enterprise_value"))
            revenue = _to_float(peer.get("revenue"))
            ebitda = _to_float(peer.get("ebitda"))
            market_cap = _to_float(peer.get("market_cap"))
            net_income = _to_float(peer.get("net_income"))
            book_value = _to_float(peer.get("book_value"))

            row = dict(peer)
            row["ev_revenue"] = _safe_div(enterprise_value, revenue)
            row["ev_ebitda"] = _safe_div(enterprise_value, ebitda)
            row["pe"] = _safe_div(market_cap, net_income)
            row["pb"] = _safe_div(market_cap, book_value)
            enriched.append(row)

            for metric_name in metrics:
                metric_value = row.get(metric_name)
                if metric_value is not None:
                    metrics[metric_name].append(metric_value)

        summary = {
            metric_name: _median(values)
            for metric_name, values in metrics.items()
        }
        return json.dumps({"peers": enriched, "summary": summary}, ensure_ascii=False, indent=2)


class IntrinsicValuationInput(BaseModel):
    """
    设计目的：定义简化 DCF 工具的输入格式。
    模块功能：约束基础 FCF、增长率、折现率和股本等关键参数。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：所有字段都可由调用方覆盖。
    默认参数及原因：`years` 默认 5，原因是兼顾说明性和常见研究习惯。
    """

    base_free_cash_flow: float = Field(..., description="Base free cash flow for year 1 projection.")
    growth_rate: float = Field(..., description="Annual growth rate during forecast period, such as 0.12.")
    discount_rate: float = Field(..., description="Discount rate / WACC, such as 0.11.")
    terminal_growth_rate: float = Field(..., description="Terminal growth rate, such as 0.02.")
    years: int = Field(default=5, ge=1, le=10, description="Number of explicit forecast years.")
    net_cash: float = Field(default=0.0, description="Net cash to add after enterprise value.")
    shares_outstanding: float = Field(default=1.0, gt=0.0, description="Shares outstanding for per-share value.")


class IntrinsicValuationTool(BaseTool):
    """
    设计目的：提供一个够小、够直接的内在价值工具。
    模块功能：显式预测 FCF、折现终值，并输出企业价值和每股价值。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：增长率、折现率、终值增长率、年数、净现金和股本。
    默认参数及原因：`shares_outstanding` 默认 1.0，原因是缺少股本时也能先输出占位每股价值。
    """

    name: str = "valuation_model_tool"
    description: str = (
        "Run a compact discounted cash flow model with an explicit forecast period and terminal value."
    )
    args_schema: type[BaseModel] = IntrinsicValuationInput

    def _run(
        self,
        base_free_cash_flow: float,
        growth_rate: float,
        discount_rate: float,
        terminal_growth_rate: float,
        years: int = 5,
        net_cash: float = 0.0,
        shares_outstanding: float = 1.0,
    ) -> str:
        """
        设计目的：提供一个够小、够直接的内在价值模型，用于草拟估值区间。
        模块功能：显式预测 FCF，折现后叠加终值，最终得到股东价值和每股价值。
        实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
        可调参数：增长率、折现率、终值增长率、年数、净现金、股本。
        默认参数及原因：`net_cash=0.0`，原因是有些早期分析先只想看企业价值主体，不想强依赖资产负债表细项。
        """
        if discount_rate <= terminal_growth_rate:
            raise ValueError(
                f"discount_rate ({discount_rate}) must exceed terminal_growth_rate ({terminal_growth_rate})."
            )

        projected_fcfs: list[float] = []
        discounted_fcfs: list[float] = []

        current_fcf = base_free_cash_flow
        for year in range(1, years + 1):
            current_fcf = current_fcf * (1 + growth_rate)
            projected_fcfs.append(current_fcf)
            discounted_fcfs.append(current_fcf / ((1 + discount_rate) ** year))

        terminal_fcf = projected_fcfs[-1] * (1 + terminal_growth_rate)
        terminal_value = terminal_fcf / (discount_rate - terminal_growth_rate)
        discounted_terminal_value = terminal_value / ((1 + discount_rate) ** years)
        enterprise_value = sum(discounted_fcfs) + discounted_terminal_value
        equity_value = enterprise_value + net_cash
        value_per_share = equity_value / shares_outstanding

        payload = {
            "projected_fcfs": projected_fcfs,
            "discounted_fcfs": discounted_fcfs,
            "discounted_terminal_value": discounted_terminal_value,
            "enterprise_value": enterprise_value,
            "equity_value": equity_value,
            "value_per_share": value_per_share,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)


class FootballFieldInput(BaseModel):
    """
    设计目的：定义估值 football field 图表工具的输入格式。
    模块功能：约束估值区间、标题、显示单位和数值前后缀，避免代理拼接出不稳定的图表参数。
    实现逻辑：使用 JSON 字符串承载多个估值区间，再配合标题与格式参数生成可直接嵌入 Markdown 的 SVG。
    可调参数：`ranges_json`、`title`、`value_prefix`、`value_suffix`。
    默认参数及原因：标题默认使用 `Valuation Football Field`，前后缀默认留空，原因是兼容倍数估值和每股价值两类常见展示口径。
    """

    ranges_json: str = Field(
        ...,
        description=(
            "JSON list of valuation ranges. Each row must include label, low, and high, "
            "and may include base, note, and color."
        ),
    )
    title: str = Field(
        default="Valuation Football Field",
        description="Chart title shown above the football field.",
    )
    value_prefix: str = Field(
        default="",
        description="Prefix added before numeric labels, for example '$' or '¥'.",
    )
    value_suffix: str = Field(
        default="",
        description="Suffix added after numeric labels, for example 'x' or '元/股'.",
    )


class FootballFieldTool(BaseTool):
    """
    设计目的：把估值区间图的生成逻辑收口成稳定工具，避免代理手写 SVG 时出现排版和比例错误。
    模块功能：读取多个估值区间，自动计算坐标轴和条带位置，并输出可直接嵌入 Markdown/PDF 的 SVG 图表。
    实现逻辑：先标准化输入区间，再根据全局最小值和最大值换算像素位置，最后拼接带标签、刻度和基准点的 SVG。
    可调参数：估值区间 JSON、图表标题、数值前缀和后缀。
    默认参数及原因：默认输出自适应坐标范围和中性色配色，原因是让不同公司、不同币种和不同估值口径都能直接复用。
    """

    name: str = "valuation_football_field_tool"
    description: str = (
        "Build a valuation football field chart as inline SVG from multiple valuation ranges "
        "so the result can be embedded directly into markdown or HTML reports."
    )
    args_schema: type[BaseModel] = FootballFieldInput

    def _run(
        self,
        ranges_json: str,
        title: str = "Valuation Football Field",
        value_prefix: str = "",
        value_suffix: str = "",
    ) -> str:
        """
        设计目的：生成可直接嵌入估值报告的 football field SVG。
        模块功能：校验输入区间、自动计算比例尺、绘制估值区间条带和基准点，并返回 HTML 包裹的 SVG 字符串。
        实现逻辑：先解析 JSON，再统一 low/high/base 的口径，随后按全局范围映射到像素坐标，最后组装 SVG 元素。
        可调参数：`ranges_json`、`title`、`value_prefix`、`value_suffix`。
        默认参数及原因：默认前后缀为空，原因是不同任务可能输出倍数、企业价值或每股价格，交给调用方指定更稳妥。
        """

        raw_ranges = json.loads(ranges_json)
        if not isinstance(raw_ranges, list) or not raw_ranges:
            raise ValueError("ranges_json must be a non-empty JSON list.")

        normalized_ranges: list[dict[str, object]] = []
        palette = ["#1f4e79", "#2f7d32", "#8a5a00", "#7a3e9d", "#9b2c2c", "#006d77"]

        for index, item in enumerate(raw_ranges):
            if not isinstance(item, dict):
                raise ValueError("Each valuation range must be a JSON object.")

            label = str(item.get("label", "")).strip()
            if not label:
                raise ValueError("Each valuation range must include a non-empty label.")

            low = _to_float(item.get("low"))
            high = _to_float(item.get("high"))
            base = _to_float(item.get("base"))
            if low is None or high is None:
                raise ValueError("Each valuation range must include numeric low and high values.")

            range_low = min(low, high)
            range_high = max(low, high)
            range_base = base if base is not None else (range_low + range_high) / 2
            if range_base < range_low:
                range_base = range_low
            if range_base > range_high:
                range_base = range_high

            note = str(item.get("note", "")).strip()
            color = str(item.get("color", "")).strip() or palette[index % len(palette)]

            normalized_ranges.append(
                {
                    "label": label,
                    "low": range_low,
                    "high": range_high,
                    "base": range_base,
                    "note": note,
                    "color": color,
                }
            )

        min_value = min(float(item["low"]) for item in normalized_ranges)
        max_value = max(float(item["high"]) for item in normalized_ranges)
        if math.isclose(min_value, max_value):
            padding = max(abs(min_value) * 0.1, 1.0)
        else:
            padding = max((max_value - min_value) * 0.08, abs(max_value) * 0.02, 0.5)

        axis_min = min_value - padding
        axis_max = max_value + padding
        axis_span = axis_max - axis_min

        width = 1040
        height = 150 + len(normalized_ranges) * 58
        left_margin = 260
        right_margin = 220
        chart_width = width - left_margin - right_margin
        top_margin = 78
        row_gap = 58
        tick_count = 5

        def scale(value: float) -> float:
            """
            设计目的：把估值数值稳定映射到 SVG 横坐标。
            模块功能：按照全局坐标轴范围把输入值换算成图表内部像素位置。
            实现逻辑：使用线性比例 `(value - axis_min) / axis_span` 计算相对位置，再叠加左边距。
            可调参数：`value`。
            默认参数及原因：坐标轴范围默认来自所有估值区间并加缓冲，原因是保证图形既不贴边也不失真。
            """

            return left_margin + ((value - axis_min) / axis_span) * chart_width

        def format_value(value: float) -> str:
            """
            设计目的：统一 football field 中的数值标签格式。
            模块功能：根据数值大小自动选择小数位，并加上调用方指定的前后缀。
            实现逻辑：对整数附近值保留 0 位，其余常见估值场景保留 1 到 2 位小数，再拼接前后缀。
            可调参数：`value`。
            默认参数及原因：默认最多保留 2 位小数，原因是估值区间图重在比较范围，不需要过细精度。
            """

            if math.isclose(value, round(value), abs_tol=1e-9):
                body = f"{value:.0f}"
            elif abs(value) >= 100:
                body = f"{value:.1f}"
            else:
                body = f"{value:.2f}".rstrip("0").rstrip(".")
            return f"{value_prefix}{body}{value_suffix}"

        svg_parts = [
            (
                f'<div style="margin: 16px 0 20px 0;">'
                f'<div style="font-weight: 700; font-size: 16px; margin-bottom: 10px;">{_escape_svg_text(title)}</div>'
                f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
                f'viewBox="0 0 {width} {height}" role="img" aria-label="{_escape_svg_text(title)}">'
            ),
            '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff" />',
        ]

        axis_y = top_margin - 24
        svg_parts.append(
            f'<line x1="{left_margin}" y1="{axis_y}" x2="{left_margin + chart_width}" y2="{axis_y}" '
            'stroke="#666666" stroke-width="1.2" />'
        )

        for tick_index in range(tick_count + 1):
            tick_value = axis_min + axis_span * tick_index / tick_count
            tick_x = scale(tick_value)
            svg_parts.append(
                f'<line x1="{tick_x:.2f}" y1="{axis_y - 6}" x2="{tick_x:.2f}" y2="{height - 40}" '
                'stroke="#d0d7de" stroke-width="1" />'
            )
            svg_parts.append(
                f'<text x="{tick_x:.2f}" y="{axis_y - 10}" text-anchor="middle" '
                'font-size="12" fill="#444444">'
                f"{_escape_svg_text(format_value(tick_value))}</text>"
            )

        for row_index, item in enumerate(normalized_ranges):
            y_center = top_margin + row_index * row_gap
            low = float(item["low"])
            high = float(item["high"])
            base = float(item["base"])
            color = str(item["color"])
            note = str(item["note"])

            bar_x = scale(low)
            bar_end_x = scale(high)
            bar_width = max(bar_end_x - bar_x, 2.0)
            base_x = scale(base)
            text_y = y_center + 5

            svg_parts.append(
                f'<text x="16" y="{text_y}" font-size="13" font-weight="700" fill="#1f2328">'
                f'{_escape_svg_text(str(item["label"]))}</text>'
            )
            svg_parts.append(
                f'<rect x="{bar_x:.2f}" y="{y_center - 8}" width="{bar_width:.2f}" height="16" rx="8" '
                f'fill="{_escape_svg_text(color)}" fill-opacity="0.18" stroke="{_escape_svg_text(color)}" '
                'stroke-width="1.5" />'
            )
            svg_parts.append(
                f'<line x1="{base_x:.2f}" y1="{y_center - 14}" x2="{base_x:.2f}" y2="{y_center + 14}" '
                f'stroke="{_escape_svg_text(color)}" stroke-width="2.5" />'
            )
            svg_parts.append(
                f'<text x="{bar_x:.2f}" y="{y_center - 14}" text-anchor="start" font-size="11" fill="#555555">'
                f"{_escape_svg_text(format_value(low))}</text>"
            )
            svg_parts.append(
                f'<text x="{bar_end_x:.2f}" y="{y_center - 14}" text-anchor="end" font-size="11" fill="#555555">'
                f"{_escape_svg_text(format_value(high))}</text>"
            )
            svg_parts.append(
                f'<text x="{left_margin + chart_width + 18}" y="{text_y}" font-size="12" fill="#444444">'
                f"{_escape_svg_text(note or '基准点为区间中点或任务指定值')}</text>"
            )

        svg_parts.append("</svg></div>")
        return "".join(svg_parts)


def _to_float(value: object) -> float | None:
    """
    设计目的：统一估值工具里的数值转换逻辑。
    模块功能：把可解析值转成 `float`，无法解析时返回 `None`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`value`。
    默认参数及原因：错误时返回 `None`，原因是原始市场数据经常有缺值或占位字符串。
    """

    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _escape_svg_text(value: str) -> str:
    """
    设计目的：避免图表文字内容破坏 SVG 结构。
    模块功能：转义 SVG 文本节点和属性中常见的特殊字符。
    实现逻辑：按最小必需集合替换 `&`、`<`、`>`、单双引号。
    可调参数：`value`。
    默认参数及原因：默认只处理最常见的五类字符，原因是 football field 的文字内容以普通文本为主，不需要更重的 HTML 处理。
    """

    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    """
    设计目的：统一估值倍数里的安全除法逻辑。
    模块功能：在分子分母有效时返回比值，否则返回 `None`。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`numerator` 和 `denominator`。
    默认参数及原因：分母为 0 或缺值时返回 `None`，原因是此时倍数没有可靠意义。
    """

    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _median(values: list[float]) -> float | None:
    """
    设计目的：给可比估值摘要提供统一的中位数算法。
    模块功能：在输入列表非空时返回中位数。
    实现逻辑：按当前定义的输入、处理和返回顺序执行，直接复用本函数或类里已经写好的步骤。
    可调参数：`values`。
    默认参数及原因：空列表返回 `None`，原因是没有样本时不应伪造统计结果。
    """

    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2
