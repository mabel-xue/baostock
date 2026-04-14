"""场内标的盯盘配置（list[dict]，可按需为单条标的扩展字段）。"""

from __future__ import annotations

from typing import Any


WATCHLIST: list[dict[str, Any]] = [
    # ==============平安普==============
    {
        "code": "sh600377",
        "alias": "宁沪高速",
        "price_targets": [{"price": 11.10, "direction": "买入"}],
        "memo": "跌到11.1加仓，看股息率逢低分批",
        "investment_notes": [
            "每天以11.1元低价挂单买宁沪高速(徐411)",
        ],
        "buy_reason": [
            "徐老师131高速路课程推荐：黄金通道，扛周期能力强",
        ],
        "reviews": [],
    },
    {
        "code": "sz000089",
        "alias": "深圳机场",
        "price_targets": [],
        "memo": "在6.5-7.5做大波段，逢高减仓",
        "investment_notes": [
            "深圳国资委控股，市盈率偏高，股息率低",
        ],
        "buy_reason": [
            "为打新深市股票，徐老师推荐",
        ],
        "reviews": [
            {
                "date": "2026-04-11",
                "buy_reason_still_valid": False,
                "notes": "已有平安银行(深市)，持仓理由不再，可在6.5-7.5之间做波段，长期趋势是逢高减仓",
            },
        ],
    },
    # ==============平安信==============
    {
        "code": "sz002100",
        "alias": "天康生物",
        "price_targets": [{"rate": 8.0, "direction": "卖出"}],
        "memo": "较前收涨8%提醒（减仓）",
        "investment_notes": [
            "天康生物，畜牧养殖个股",
            ">=7.64 减仓1000股，大涨>=7.81 再减1000股 √",
            "猪周期下行阶段逐步减仓（徐老师329课程）",
        ],
    },
    {
        "code": "sh603053",
        "alias": "成都燃气",
        "price_targets": [{"price": 9.48, "direction": "买入"}],
        "memo": "跌到9.48加仓500股（徐-操作备忘）",
    },
    {
        "code": "sh601166",
        "alias": "兴业银行",
        "price_targets": [],
        "memo": "开盘大跌3%以上增持",
        "open_drop_alert_pct": -3.0,
    },
    {
        "code": "sz000001",
        "alias": "平安银行",
        "price_targets": [],
        "memo": "开盘大跌3%以上增持",
        "open_drop_alert_pct": -3.0,
        "investment_notes": [
            "平安银行",
            "反转可能还需2-3年（徐329）",
        ],
        "buy_thesis": ["徐老师看好业绩反转"],
    },
    # ==============ETF==============
    {"code": "sh159363", "alias": "创业板人工智能ETF"},
    {
        "code": "sz159632",
        "alias": "纳斯达克ETF华安",
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
        "alias": "红利ETF工银",
        "price_targets": [],
        "memo": "",
        "investment_notes": [
            "深证红利指数，高股息策略，历史 PE<15 为低估区间",
            "长期定投品种，红利再投资复利效应",
        ],
    },
    {
        "code": "sh512690",
        "alias": "酒ETF",
        "price_targets": [],
        "memo": "",
        "investment_notes": [
            "白酒板块周期性强，消费复苏逻辑",
            "PE 20-25 为合理区间，低于 20 可加仓",
        ],
    },
    {
        "code": "sh516670",
        "alias": "畜牧养殖ETF招商",
        "price_targets": [],
        "memo": "",
        "investment_notes": [
            "猪肉不再关注（徐老师329课程）",
        ],
    },
    {
        "code": "sz160127",
        "alias": "南方消费LOF",
        "price_targets": [],
        "memo": "",
        "pre_close_change_pct_notify_at": "14:50",
        "investment_notes": [
            "南方新兴消费增长股票(LOF)A",
        ],
    },
    {
        "code": "sz159781",
        "alias": "科创创业ETF易方达",
        "price_targets": [],
        "memo": "",
        "pre_close_change_pct_notify_at": "14:50",
        "investment_notes": [
            "易方达中证科创创业50ETF",
        ],
    },
    {
        "code": "sz159941",
        "alias": "广发纳斯达克100ETF",
        "price_targets": [],
        "memo": "",
        "pre_close_change_pct_notify_at": "14:50",
        "investment_notes": [
            "广发纳斯达克100ETF(QDII)",
        ],
    },
    {
        "code": "sh562500",
        "alias": "机器人ETF",
        "price_targets": [],
        "memo": "",
        "pre_close_change_pct_notify_at": "14:50",
        "investment_notes": [
            "机器人/AI 产业趋势，中长期看好",
            "波段操作为主，涨幅超 30% 可分批止盈",
        ],
    },
    {
        "code": "sz159611",
        "alias": "电力ETF广发",
        "price_targets": [],
        "memo": "",
        "pre_close_change_pct_notify_at": "14:50",
        "investment_notes": [
            "电力公用事业，防御性配置",
            "电改+新能源消纳，长期逻辑不变",
        ],
    },
    {
        "code": "sh510300",
        "alias": "沪深300ETF",
        "price_targets": [],
        "memo": "",
        "investment_notes": [
            "沪深300，大盘风向标",
        ],
    },
    {
        "code": "sh510900",
        "alias": "恒生中国企业ETF",
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
        "alias": "易方达中证A500ETF联接Y",
        "price_targets": [],
        "memo": "",
        "poll": False,
        "investment_notes": [
            "易方达中证A500ETF联接Y",
            "中国A500徐老师不投（中国没有500加优质企业）",
        ],
    },
]


# 常用键（均可按需增删）:
#   code: 腾讯行情格式 sh/sz+6 位；场外基金可写 6 位代码并设 poll=False
#   alias: 简称（本文件内写死）
#   price_targets: [{"price": float, "direction": "买入"|"卖出"}, ...]
#                或 {"rate": float, "direction": ...} — rate 为相对昨收涨跌幅阈值(%)：
#                卖出：change_pct >= rate 触发；买入：change_pct <= -rate 触发（配置了 rate 时不看 price）
#   memo: 操作备忘
#   investment_notes: 投资逻辑条目标（可选）
#   open_drop_alert_pct: 可选；仅配置此项的标的会推送「开盘价」飞书/高亮日志，并在此阈值(%)
#       下推送「大跌增持」（开盘涨跌 ≤ 该值时触发，一般为负数如 -3.0）
#   poll: 可选，默认 True；False 时不参与行情轮询（仅保留备忘）
#   pre_close_change_pct_notify_at: 可选，HH:MM；配置后该标的在「收盘前」推送较昨收涨跌幅（日志高亮 + 飞书）
#       当时刻 < 15:00：在午后/尾盘竞价段，本地时间达到该时刻后的首次轮询触发；
#       当时刻 >= 15:00：A 股 15:00 收盘，改为在尾盘竞价段（约 14:57–15:00）首次进入该阶段时触发

POLL_INTERVAL = 5
