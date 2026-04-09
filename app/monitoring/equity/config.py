"""场内标的盯盘配置（list[dict]，可按需为单条标的扩展字段）。"""

from __future__ import annotations

from typing import Any


def _fund_etf_short_names() -> dict[str, str]:
    """6 位基金/场内 ETF 代码 → 简称，与 query_my_funds.MY_FUNDS / MY_ETFS 一致。"""
    from query_my_funds import MY_ETFS, MY_FUNDS

    m: dict[str, str] = {}
    for code, name in MY_FUNDS:
        d = "".join(ch for ch in code if ch.isdigit())
        if not d:
            continue
        m[d[-6:].zfill(6)] = name
    for code, name, _ in MY_ETFS:
        d = "".join(ch for ch in code if ch.isdigit())
        if not d:
            continue
        m[d[-6:].zfill(6)] = name
    return m


def _six_digits(code: str) -> str:
    d = "".join(c for c in code if c.isdigit())
    return d[-6:].zfill(6) if d else ""


def _build_watchlist() -> list[dict[str, Any]]:
    N = _fund_etf_short_names()

    # 第一个参数应与本条 "code" 一致（仅取 6 位查表，写 sh/sz 前缀或裸 6 位等价）
    def alias(code: str, fallback: str) -> str:
        return N.get(_six_digits(code), fallback)

    return [
        # ==============平安普==============
        {
            "code": "sh600377",
            "alias": alias("sh600377", "宁沪高速"),
            "price_targets": [{"price": 11.50, "direction": "买入"}],
            "memo": "跌到11.5加仓，看股息率逢低分批",
            "investment_notes": [
                "宁沪高速，高速公路龙头，高股息防御品种",
                "看股息率逢低分批加仓，跌到11.5目标价加仓",
            ],
        },
        # ==============平安信==============
        {
            "code": "sz002100",
            "alias": alias("sz002100", "天康生物"),
            "price_targets": [{"price": 7.64, "direction": "卖出"}],
            "memo": ">=7.64减仓1000股，大涨减1000股",
            "investment_notes": [
                "天康生物，畜牧养殖个股",
                ">=7.64 减仓1000股，大涨>=7.81 再减1000股",
                "猪周期下行阶段逐步减仓（徐老师329课程）",
            ],
        },
        {
            "code": "sh603053",
            "alias": alias("sh603053", "成都燃气"),
            "price_targets": [{"price": 9.48, "direction": "买入"}],
            "memo": "跌到9.48加仓500股（徐-操作备忘）",
        },
        {
            "code": "sh601166",
            "alias": alias("sh601166", "兴业银行"),
            "price_targets": [],
            "memo": "开盘大跌3%以上增持",
            "open_drop_alert_pct": -3.0,
        },
        {
            "code": "sz000001",
            "alias": alias("sz000001", "平安银行"),
            "price_targets": [],
            "memo": "开盘大跌3%以上增持",
            "open_drop_alert_pct": -3.0,
            "investment_notes": [
                "平安银行",
                "反转可能还需2-3年（徐329）",
            ],
        },
        # ==============ETF==============
        {
            "code": "sz159632",
            "alias": alias("sz159632", "纳斯达克ETF华安"),
            "price_targets": [{"price": 1.600, "direction": "买入"}],
            "memo": "1.6分批建仓，等暴跌机会",
            "investment_notes": [
                "纳斯达克ETF华安，跟踪纳斯达克100指数",
                "纳斯达克一定会跌，可能经历暴跌（徐329）",
                "1.6分批建仓，耐心等待暴跌机会",
            ],
        },
        {
            "code": "sz159905",
            "alias": alias("sz159905", "红利ETF工银"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "深证红利指数，高股息策略，历史 PE<15 为低估区间",
                "长期定投品种，红利再投资复利效应",
            ],
        },
        {
            "code": "sh512690",
            "alias": alias("sh512690", "酒ETF"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "白酒板块周期性强，消费复苏逻辑",
                "PE 20-25 为合理区间，低于 20 可加仓",
            ],
        },
        {
            "code": "sh516670",
            "alias": alias("sh516670", "畜牧养殖ETF招商"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "猪肉不再关注（徐老师329课程）",
            ],
        },
        {
            "code": "sh562500",
            "alias": alias("sh562500", "机器人ETF"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "机器人/AI 产业趋势，中长期看好",
                "波段操作为主，涨幅超 30% 可分批止盈",
            ],
        },
        {
            "code": "sz159611",
            "alias": alias("sz159611", "电力ETF广发"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "电力公用事业，防御性配置",
                "电改+新能源消纳，长期逻辑不变",
            ],
        },
        {
            "code": "sh510300",
            "alias": alias("sh510300", "沪深300ETF"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "沪深300，大盘风向标",
            ],
        },
        {
            "code": "sh510900",
            "alias": alias("sh510900", "恒生中国企业ETF"),
            "price_targets": [],
            "memo": "",
            "investment_notes": [
                "恒生国企，港股估值洼地",
                "关注中美关系、港股流动性变化",
            ],
        },
        # ==============场外基金==============
        {
            "code": "022930",
            "alias": alias("022930", "易方达中证A500ETF联接Y"),
            "price_targets": [],
            "memo": "",
            "poll": False,
            "investment_notes": [
                "易方达中证A500ETF联接Y",
                "中国A500徐老师不投（中国没有500加优质企业）",
            ],
        },
        # 160127、159781、159941、159611、562500 14:50 输出当日涨跌幅
    ]


# 常用键（均可按需增删）:
#   code: 腾讯行情格式 sh/sz+6 位；场外基金可写 6 位代码并设 poll=False
#   alias: 简称；未在 query_my_funds 中的标的可写 fallback，在表内的自动对齐 MY_ETFS/MY_FUNDS
#   price_targets: [{"price": float, "direction": "买入"|"卖出"}, ...]
#   memo: 操作备忘
#   investment_notes: 投资逻辑条目标（可选）
#   open_drop_alert_pct: 可选，开盘大跌增持阈值(%)
#   poll: 可选，默认 True；False 时不参与行情轮询（仅保留备忘）

WATCHLIST: list[dict[str, Any]] = _build_watchlist()

POLL_INTERVAL = 5
