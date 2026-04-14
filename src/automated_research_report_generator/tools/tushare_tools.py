from __future__ import annotations

import json
import os
import re
from typing import Any

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

# 设计目的：给估值阶段提供一个面向 Tushare Pro 的专用数据入口，避免 agent 在 prompt 里自己拼接接口细节。
# 模块功能：解析公司名称或代码，拉取最新市场数据、财务指标和三大报表摘要，并整理成估值可直接使用的结构。
# 实现逻辑：先解析输入公司列表，再用 Tushare 解析证券代码，最后聚合 `daily_basic`、`fina_indicator`、`income`、`balancesheet` 和 `cashflow`。
# 可调参数：公司列表、交易日期、报告期和接口字段列表。
# 默认参数及原因：默认读取最新可得数据，原因是估值 crew 当前更需要最新横截面对比而不是完整历史序列。

TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"
TUSHARE_STOCK_BASIC_FIELDS = "ts_code,symbol,name,area,industry,market,list_date"
TUSHARE_DAILY_BASIC_FIELDS = (
    "ts_code,trade_date,close,turnover_rate,pe,pe_ttm,pb,ps_ttm,dv_ttm,total_share,float_share,"
    "free_share,total_mv,circ_mv"
)
TUSHARE_FINA_INDICATOR_FIELDS = (
    "ts_code,ann_date,end_date,eps,dt_eps,bps,roe,roe_dt,roa,grossprofit_margin,netprofit_margin,"
    "ocfps,ocf_to_or,q_sales_yoy,yoy_sales,q_profit_yoy,yoy_profit,assets_yoy,equity_yoy"
)
TUSHARE_INCOME_FIELDS = (
    "ts_code,ann_date,f_ann_date,end_date,total_revenue,revenue,operate_profit,n_income_attr_p,ebit,ebitda"
)
TUSHARE_BALANCESHEET_FIELDS = (
    "ts_code,ann_date,f_ann_date,end_date,total_share,money_cap,total_assets,total_liab,"
    "total_hldr_eqy_exc_min_int,total_cur_assets,total_nca,total_cur_liab,total_ncl"
)
TUSHARE_CASHFLOW_FIELDS = (
    "ts_code,ann_date,f_ann_date,end_date,n_cashflow_act,c_inf_fr_operate_a,n_cashflow_inv_act,"
    "n_cash_flows_fnc_act"
)
TS_CODE_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$", re.IGNORECASE)
SYMBOL_PATTERN = re.compile(r"^\d{6}$")
MARKET_VALUE_TO_CNY_MULTIPLIER = 10_000.0

_TUSHARE_PRO_CLIENT: Any | None = None
_STOCK_BASIC_RECORDS_CACHE: list[dict[str, Any]] | None = None


class TushareValuationDataInput(BaseModel):
    """
    设计目的：定义 Tushare 估值数据工具的输入格式。
    模块功能：约束公司列表、交易日期和报告期输入。
    实现逻辑：通过 Pydantic 保证工具收到结构稳定的字符串参数。
    可调参数：`companies`、`trade_date` 和 `period`。
    默认参数及原因：日期和报告期默认留空，原因是多数估值场景先读取最新可得数据。
    """

    companies: str = Field(
        ...,
        description=(
            "公司列表。支持 JSON 数组或逗号分隔字符串。每项可以是公司简称、六位股票代码，"
            "也可以是 TS 代码，如 `宁德时代,比亚迪,300750.SZ`。"
        ),
    )
    trade_date: str = Field(
        default="",
        description="可选交易日期，格式为 YYYYMMDD。留空时读取最新可得每日指标。",
    )
    period: str = Field(
        default="",
        description="可选报告期，格式为 YYYYMMDD。留空时读取最新可得财务数据。",
    )


def _get_tushare_pro_client() -> Any:
    """
    设计目的：集中管理 Tushare Pro 客户端初始化。
    模块功能：读取环境变量并返回可复用的 `pro_api` 客户端。
    实现逻辑：先检查 `TUSHARE_TOKEN`，再按需懒加载 `tushare` 并缓存客户端实例。
    可调参数：环境变量 `TUSHARE_TOKEN`。
    默认参数及原因：客户端默认做进程内缓存，原因是同一轮估值任务会重复调用多个接口。
    """

    global _TUSHARE_PRO_CLIENT
    if _TUSHARE_PRO_CLIENT is not None:
        return _TUSHARE_PRO_CLIENT

    token = os.getenv(TUSHARE_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(
            f"Missing {TUSHARE_TOKEN_ENV}. Set the Tushare token before using TushareValuationDataTool."
        )

    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError(
            "tushare is not installed. Run `uv sync` after adding the dependency, then retry."
        ) from exc

    _TUSHARE_PRO_CLIENT = ts.pro_api(token)
    return _TUSHARE_PRO_CLIENT


def _parse_companies_input(companies: str) -> list[str]:
    """
    设计目的：统一解析 agent 传入的公司列表格式。
    模块功能：支持 JSON 数组和逗号分隔两种输入方式。
    实现逻辑：优先按 JSON 解析，失败后回退到逗号分隔，再清洗空白项。
    可调参数：`companies`。
    默认参数及原因：不支持嵌套复杂对象，原因是估值工具当前只需要一维公司标识列表。
    """

    cleaned = (companies or "").strip()
    if not cleaned:
        raise ValueError("`companies` cannot be empty.")

    items: list[Any]
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            items = parsed
        else:
            items = [parsed]
    except json.JSONDecodeError:
        items = [part.strip() for part in cleaned.split(",")]

    normalized_items: list[str] = []
    for item in items:
        if isinstance(item, dict):
            candidate = item.get("ts_code") or item.get("company") or item.get("name") or ""
        else:
            candidate = str(item or "")
        candidate = " ".join(str(candidate).split()).strip()
        if candidate:
            normalized_items.append(candidate)

    if not normalized_items:
        raise ValueError("No valid company identifiers were parsed from `companies`.")
    return normalized_items


def _load_stock_basic_records() -> list[dict[str, Any]]:
    """
    设计目的：集中缓存股票基础信息，避免重复打同一个全量接口。
    模块功能：拉取上市股票基础列表并缓存为字典列表。
    实现逻辑：首次调用时请求 `stock_basic`，后续直接复用进程内缓存。
    可调参数：字段列表固定为估值场景所需的最小集合。
    默认参数及原因：默认只读取上市状态 `L`，原因是估值对比优先关注当前可交易公司。
    """

    global _STOCK_BASIC_RECORDS_CACHE
    if _STOCK_BASIC_RECORDS_CACHE is not None:
        return _STOCK_BASIC_RECORDS_CACHE

    pro = _get_tushare_pro_client()
    dataframe = pro.stock_basic(
        list_status="L",
        fields=TUSHARE_STOCK_BASIC_FIELDS,
    )
    _STOCK_BASIC_RECORDS_CACHE = dataframe.to_dict(orient="records")
    return _STOCK_BASIC_RECORDS_CACHE


def _resolve_company_identifier(identifier: str) -> dict[str, Any]:
    """
    设计目的：把公司简称、六位代码或 TS 代码解析成统一证券身份。
    模块功能：在 `stock_basic` 结果中找到唯一匹配的上市公司。
    实现逻辑：按 TS 代码、六位代码、精确简称、唯一模糊匹配的顺序依次解析。
    可调参数：`identifier`。
    默认参数及原因：模糊匹配只在唯一命中时采用，原因是估值数据不能容忍错配公司。
    """

    records = _load_stock_basic_records()
    normalized_identifier = " ".join((identifier or "").split()).strip()
    upper_identifier = normalized_identifier.upper()

    if TS_CODE_PATTERN.match(upper_identifier):
        for record in records:
            if str(record.get("ts_code", "")).upper() == upper_identifier:
                return record

    if SYMBOL_PATTERN.match(normalized_identifier):
        exact_symbol_matches = [record for record in records if str(record.get("symbol", "")) == normalized_identifier]
        if len(exact_symbol_matches) == 1:
            return exact_symbol_matches[0]

    exact_name_matches = [record for record in records if str(record.get("name", "")) == normalized_identifier]
    if len(exact_name_matches) == 1:
        return exact_name_matches[0]

    fuzzy_matches = [
        record
        for record in records
        if normalized_identifier in str(record.get("name", ""))
        or str(record.get("name", "")) in normalized_identifier
    ]
    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0]

    if fuzzy_matches:
        candidate_names = [str(record.get("name", "")) for record in fuzzy_matches[:5]]
        raise ValueError(
            f"Ambiguous company identifier `{identifier}`. Candidates: {candidate_names}"
        )

    raise ValueError(f"Unable to resolve company identifier `{identifier}` from Tushare stock_basic.")


def _pick_latest_row(dataframe: Any) -> dict[str, Any] | None:
    """
    设计目的：统一从 Tushare DataFrame 中取最新一条记录。
    模块功能：把 DataFrame 结果转换成单条字典。
    实现逻辑：优先按常见日期字段倒序排序，再取首条记录。
    可调参数：Tushare 返回的 DataFrame。
    默认参数及原因：空结果返回 `None`，原因是部分公司或权限条件下确实可能没有数据。
    """

    if dataframe is None or getattr(dataframe, "empty", True):
        return None

    rows = dataframe.to_dict(orient="records")
    if not rows:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[str, str, str]:
        """
        目的：给 Tushare 行记录提供统一排序键。
        功能：把交易日、报告期和公告日拼成可比较元组。
        实现逻辑：依次读取 `trade_date`、`end_date` 和 `ann_date`，缺值时回退空串。
        可调参数：`row`。
        默认参数及原因：默认按这三个字段排序，原因是它们最能代表一条财务或行情记录的新旧程度。
        """

        return (
            str(row.get("trade_date", "") or ""),
            str(row.get("end_date", "") or ""),
            str(row.get("ann_date", "") or ""),
        )

    rows.sort(key=sort_key, reverse=True)
    return rows[0]


def _safe_dataframe_call(interface_name: str, **kwargs: Any) -> tuple[dict[str, Any] | None, str | None]:
    """
    设计目的：把单个 Tushare 接口调用包装成稳妥的错误边界。
    模块功能：返回最新一条记录，或者返回可追踪的错误说明。
    实现逻辑：动态调用接口，成功则取最新记录，失败则把错误文本包装回传。
    可调参数：接口名称和接口参数。
    默认参数及原因：错误不直接中断整个工具，原因是估值阶段允许部分接口缺失但不应整体失败。
    """

    pro = _get_tushare_pro_client()
    filtered_kwargs = {key: value for key, value in kwargs.items() if value is not None}
    try:
        dataframe = getattr(pro, interface_name)(**filtered_kwargs)
        return _pick_latest_row(dataframe), None
    except Exception as exc:
        return None, f"{interface_name}: {type(exc).__name__}: {exc}"


def _to_number(value: Any) -> float | None:
    """
    设计目的：统一处理 Tushare 原始结果里的数值转换。
    模块功能：把可解析值转成 `float`，无法解析时返回 `None`。
    实现逻辑：过滤空值后调用 `float()`。
    可调参数：任意原始值。
    默认参数及原因：错误时返回 `None`，原因是金融数据常见缺失值和字符串占位。
    """

    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_market_value_to_cny(value_in_ten_thousand: Any) -> float | None:
    """
    设计目的：把 `daily_basic` 的市值字段统一转换为人民币元。
    模块功能：将 Tushare 文档标明的“万元”单位换算成“元”。
    实现逻辑：先转成数字，再乘以 10000。
    可调参数：`daily_basic.total_mv` 或 `daily_basic.circ_mv`。
    默认参数及原因：空值返回 `None`，原因是部分公司或日期可能拿不到完整行情字段。
    """

    numeric_value = _to_number(value_in_ten_thousand)
    if numeric_value is None:
        return None
    return numeric_value * MARKET_VALUE_TO_CNY_MULTIPLIER


def _build_peer_input(company_snapshot: dict[str, Any]) -> dict[str, Any]:
    """
    设计目的：把聚合后的单家公司数据整理成可比估值工具可直接消费的行格式。
    模块功能：抽取市场值、利润表和资产负债表关键字段，形成标准 peer row。
    实现逻辑：优先使用已转换单位后的市值，再补齐收入、EBITDA、净利润和账面净资产。
    可调参数：单家公司聚合后的快照字典。
    默认参数及原因：无法安全计算的字段保留 `None`，原因是比错算更稳妥。
    """

    market_data = company_snapshot.get("market_data", {}) or {}
    income_statement = company_snapshot.get("income_statement", {}) or {}
    balance_sheet = company_snapshot.get("balance_sheet", {}) or {}

    market_cap = market_data.get("total_mv_cny")
    total_liab = _to_number(balance_sheet.get("total_liab"))
    money_cap = _to_number(balance_sheet.get("money_cap"))
    enterprise_value = None
    if market_cap is not None and total_liab is not None and money_cap is not None:
        enterprise_value = market_cap + total_liab - money_cap

    return {
        "company": company_snapshot.get("name"),
        "ts_code": company_snapshot.get("ts_code"),
        "industry": company_snapshot.get("industry"),
        "market_cap": market_cap,
        "enterprise_value": enterprise_value,
        "revenue": _to_number(income_statement.get("revenue")) or _to_number(income_statement.get("total_revenue")),
        "ebitda": _to_number(income_statement.get("ebitda")),
        "net_income": _to_number(income_statement.get("n_income_attr_p")),
        "book_value": _to_number(balance_sheet.get("total_hldr_eqy_exc_min_int")),
    }


class TushareValuationDataTool(BaseTool):
    """
    设计目的：给估值 crew 提供一个面向 Tushare 的统一取数工具。
    模块功能：解析公司标识、抓取市场与财务数据，并输出可直接用于估值比较的 JSON。
    实现逻辑：按公司逐个聚合 `stock_basic`、`daily_basic`、`fina_indicator`、`income`、`balancesheet` 和 `cashflow`。
    可调参数：公司列表、交易日期和报告期。
    默认参数及原因：默认返回最新可得记录，原因是估值阶段主要做当前时点比较。
    """

    name: str = "tushare_valuation_data_tool"
    description: str = (
        "Resolve China A-share company names/codes through Tushare, then fetch latest market data, "
        "financial indicators, and normalized comparable-company inputs for valuation analysis."
    )
    args_schema: type[BaseModel] = TushareValuationDataInput

    def _run(self, companies: str, trade_date: str = "", period: str = "") -> str:
        """
        设计目的：为估值 agent 提供单次聚合取数入口。
        模块功能：返回每家公司最新行情、财务指标、报表摘要和可比估值输入行。
        实现逻辑：先解析公司列表，再逐家公司拉取各接口最新记录，最后统一序列化成 JSON。
        可调参数：`companies`、`trade_date` 和 `period`。
        默认参数及原因：日期留空时读取最新可得数据，原因是当前估值任务优先关注最新横截面。
        """

        company_identifiers = _parse_companies_input(companies)
        snapshots: list[dict[str, Any]] = []
        peer_inputs: list[dict[str, Any]] = []

        for identifier in company_identifiers:
            resolved = _resolve_company_identifier(identifier)
            ts_code = str(resolved.get("ts_code", ""))
            interface_errors: list[str] = []

            daily_basic, daily_basic_error = _safe_dataframe_call(
                "daily_basic",
                ts_code=ts_code,
                trade_date=trade_date or None,
                fields=TUSHARE_DAILY_BASIC_FIELDS,
            )
            if daily_basic_error:
                interface_errors.append(daily_basic_error)

            fina_indicator, fina_indicator_error = _safe_dataframe_call(
                "fina_indicator",
                ts_code=ts_code,
                period=period or None,
                fields=TUSHARE_FINA_INDICATOR_FIELDS,
            )
            if fina_indicator_error:
                interface_errors.append(fina_indicator_error)

            income_statement, income_error = _safe_dataframe_call(
                "income",
                ts_code=ts_code,
                period=period or None,
                fields=TUSHARE_INCOME_FIELDS,
            )
            if income_error:
                interface_errors.append(income_error)

            balance_sheet, balance_sheet_error = _safe_dataframe_call(
                "balancesheet",
                ts_code=ts_code,
                period=period or None,
                fields=TUSHARE_BALANCESHEET_FIELDS,
            )
            if balance_sheet_error:
                interface_errors.append(balance_sheet_error)

            cashflow_statement, cashflow_error = _safe_dataframe_call(
                "cashflow",
                ts_code=ts_code,
                period=period or None,
                fields=TUSHARE_CASHFLOW_FIELDS,
            )
            if cashflow_error:
                interface_errors.append(cashflow_error)

            market_snapshot = dict(daily_basic or {})
            if market_snapshot:
                market_snapshot["total_mv_cny"] = _normalize_market_value_to_cny(market_snapshot.get("total_mv"))
                market_snapshot["circ_mv_cny"] = _normalize_market_value_to_cny(market_snapshot.get("circ_mv"))

            company_snapshot = {
                "input_identifier": identifier,
                "ts_code": ts_code,
                "symbol": resolved.get("symbol"),
                "name": resolved.get("name"),
                "area": resolved.get("area"),
                "industry": resolved.get("industry"),
                "market": resolved.get("market"),
                "list_date": resolved.get("list_date"),
                "market_data": market_snapshot,
                "financial_indicator": fina_indicator or {},
                "income_statement": income_statement or {},
                "balance_sheet": balance_sheet or {},
                "cashflow_statement": cashflow_statement or {},
                "interface_errors": interface_errors,
            }
            company_snapshot["valuation_peer_input"] = _build_peer_input(company_snapshot)
            snapshots.append(company_snapshot)
            peer_inputs.append(company_snapshot["valuation_peer_input"])

        payload = {
            "requested_companies": company_identifiers,
            "trade_date": trade_date or None,
            "period": period or None,
            "topic_scope": "valuation_market_and_financial_data",
            "units_note": {
                "daily_basic.total_mv": "Tushare 文档标明单位为万元，工具已转换为 total_mv_cny（元）",
                "daily_basic.circ_mv": "Tushare 文档标明单位为万元，工具已转换为 circ_mv_cny（元）",
                "income/balancesheet/cashflow": "保留 Tushare 原始字段命名和原始数量级，便于追溯接口定义",
            },
            "companies": snapshots,
            "valuation_peer_inputs": peer_inputs,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)
