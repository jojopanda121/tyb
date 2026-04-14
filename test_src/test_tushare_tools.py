from __future__ import annotations

from automated_research_report_generator.tools.tushare_tools import (
    _normalize_market_value_to_cny,
    _parse_companies_input,
)


def test_parse_companies_input_supports_json_and_csv() -> None:
    """
    设计目的：锁住 Tushare 工具的公司列表输入兼容性。
    模块功能：验证 JSON 数组和逗号分隔字符串都能被解析成统一列表。
    实现逻辑：分别传入两种常见格式，再比较标准化结果。
    可调参数：无。
    默认参数及原因：测试只覆盖最常见两种输入，原因是这已经覆盖 valuation crew 的主要调用方式。
    """

    assert _parse_companies_input('["宁德时代", "300750.SZ"]') == ["宁德时代", "300750.SZ"]
    assert _parse_companies_input("宁德时代, 比亚迪") == ["宁德时代", "比亚迪"]


def test_normalize_market_value_to_cny_converts_wan_to_yuan() -> None:
    """
    设计目的：锁住 Tushare 每日指标市值字段的单位换算。
    模块功能：验证 `daily_basic.total_mv` 从万元转换为元的逻辑。
    实现逻辑：用简单数字样例做断言，确保乘数没有写错。
    可调参数：无。
    默认参数及原因：测试使用整数样例，原因是更容易直接看出换算结果是否正确。
    """

    assert _normalize_market_value_to_cny(123.45) == 1_234_500.0
