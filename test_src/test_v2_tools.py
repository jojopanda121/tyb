import json

from automated_research_report_generator.tools.financial_model_tool import FinancialModelTool
from automated_research_report_generator.tools.valuation_tools import (
    ComparableValuationTool,
    FootballFieldTool,
    IntrinsicValuationTool,
)


def test_financial_model_tool_returns_expected_fields():
    """
    设计目的：确认财务模型工具的核心派生字段没有回归。
    模块功能：喂入两期简化财务数据，并断言毛利率、自由现金流和收入增速结果。
    实现逻辑：先构造输入 JSON，再调用工具，最后把结果解析出来做字段级断言。
    可调参数：无。
    默认参数及原因：默认只测最关键的三个派生字段，原因是这几个字段最能代表计算链是否正常。
    """
    tool = FinancialModelTool()
    payload = {
        "2023": {"revenue": 100, "gross_profit": 40, "net_income": 10, "equity": 50, "cfo": 12, "capex": 3},
        "2024": {"revenue": 120, "gross_profit": 54, "net_income": 14, "equity": 60, "cfo": 15, "capex": 4},
    }
    result = json.loads(tool._run(json.dumps(payload)))
    assert result["2024"]["gross_margin"] == 0.45
    assert result["2024"]["fcf"] == 11.0
    assert result["2024"]["revenue_growth"] == 0.2


def test_valuation_tools_return_summary_and_value():
    """
    设计目的：确认可比估值和内在价值工具都能返回基本可用结果。
    模块功能：分别断言相对估值摘要里的关键倍数，以及内在价值结果里的总价值和每股价值。
    实现逻辑：先构造可比公司样本，再跑可比估值和内在价值两条路径，最后分别做结果断言。
    可调参数：无。
    默认参数及原因：默认只验证摘要值和正数结果，原因是这能覆盖两条工具链的基本正确性。
    """
    comparable_tool = ComparableValuationTool()
    comparable_payload = [
        {"company": "Peer A", "market_cap": 100, "enterprise_value": 120, "revenue": 40, "ebitda": 12, "net_income": 8, "book_value": 30},
        {"company": "Peer B", "market_cap": 140, "enterprise_value": 160, "revenue": 50, "ebitda": 16, "net_income": 10, "book_value": 35},
    ]
    comparable_result = json.loads(comparable_tool._run(json.dumps(comparable_payload)))
    assert comparable_result["summary"]["ev_revenue"] == 3.1

    intrinsic_tool = IntrinsicValuationTool()
    intrinsic_result = json.loads(
        intrinsic_tool._run(
            base_free_cash_flow=10,
            growth_rate=0.1,
            discount_rate=0.12,
            terminal_growth_rate=0.03,
            years=5,
            net_cash=5,
            shares_outstanding=10,
        )
    )
    assert intrinsic_result["enterprise_value"] > 0
    assert intrinsic_result["value_per_share"] > 0


def test_football_field_tool_returns_svg_markup():
    """
    设计目的：确认 football field 工具能稳定输出可嵌入 Markdown/PDF 的 SVG 图表。
    模块功能：构造两组估值区间，检查返回值是否同时包含 SVG 标记、标题和方法标签。
    实现逻辑：先组织 JSON 输入，再调用工具，最后对关键片段做字符串级断言。
    可调参数：无。
    默认参数及原因：默认只验证最关键的结构字段，原因是该工具的核心风险在于图表片段缺失或无法嵌入，而不是像素级样式差异。
    """

    tool = FootballFieldTool()
    result = tool._run(
        ranges_json=json.dumps(
            [
                {"label": "可比公司估值", "low": 18.5, "high": 28.0, "base": 23.6, "note": "EV/EBITDA 中位数"},
                {"label": "DCF 估值", "low": 21.0, "high": 34.0, "base": 27.8, "note": "Base case"},
            ],
            ensure_ascii=False,
        ),
        title="估值 Football Field",
        value_suffix="x",
    )

    assert "<svg" in result
    assert "估值 Football Field" in result
    assert "可比公司估值" in result
    assert "DCF 估值" in result
