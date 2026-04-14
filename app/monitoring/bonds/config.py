"""可转债监控规则（可按需增删）。

持久化与控制台日志使用「代码:kind」稳定键（见 bonds/service._rule_state_keys），
与 value 无关；同 code+kind 多条规则时可设 state_key 区分。
id 仅作人工备忘，可不写或随意命名，不参与求值。

可选字段（监控逻辑暂不读取，仅作配置侧记录）:
  trade_history: 买卖历史，list[dict]。示例元素:
      {'date': '2026-04-01', 'side': '买', 'note': '110 元附近首批', 'price': 110.0}
      side 建议用「买」「卖」；可另加数量、交割渠道等任意键。
"""

from __future__ import annotations

from typing import Any

# 与 app/query_convertible_bonds.py 默认参数一致（日终快照与 CLI 复用）
CB_QUERY_DEFAULTS: dict[str, float] = {
    "min_price": 90.0,
    "max_years": 1.0,
    "max_price": 118.0,
}

# 轮询内是否执行：① 尾盘竞价窗口写当日 query 快照 ② 轻量比对转债代码是否新增
CB_DAILY_SNAPSHOT_ENABLED: bool = True
CB_NEW_BOND_POLL_ENABLED: bool = True
# 日终快照落盘后：与上一交易日「保留池」对比，有新进入保留池的转债则飞书通知（与全市场新代码轮询不同）
CB_KEPT_NEW_NOTIFY_ENABLED: bool = True

CB_MONITOR_RULES: list[dict[str, Any]] = [
    {
        "id": "128119_price_lt_80",
        "code": "128119",
        "kind": "price_lt",
        "value": 80.0,
        "note": "龙大转债",
    },
    {
        "id": "113049_price_lt_102",
        "code": "113049",
        "kind": "price_lt",
        "value": 102.0,
        "note": "长汽转债",
    },
    {
        "id": "110081_price_lt",
        "code": "110081",
        "kind": "price_lt",
        "value": 102.0,
        "note": "闻泰转债",
        "memo": "现金流充足、子公司安世半导体是全球功率分立器件巨头，"
        "车规级逻辑器件/ESD保护器件细分市场全球第一或第二，"
        "客户涵盖几乎所有主流汽车一级供应商。"
        "目前因海外监管因素计提巨额资产减值准备（临时性）",
        "trade_history": [
            {
                "date": "2026-04-01",
                "side": "买",
                "price": 108.0,
                "note": "108 元附近首批",
            }
        ],
    },
    {
        "id": "110081_notify_open",
        "code": "110081",
        "kind": "notify_open",
        "note": "闻泰转债",
        "memo": "安世半导体：车规级分立器件全球龙头，减值为临时因素",
    },
    {
        "id": "127049_price_lt_112",
        "code": "127049",
        "kind": "price_lt",
        "value": 112.0,
        "note": "希望转2",
    },
    {
        "id": "123142_price_lt_105",
        "code": "123142",
        "kind": "price_lt",
        "value": 105.0,
        "note": "申昊转债",
        "memo": "公司连年亏损，关注2026经营状况再定",
    },
    {
        "id": "118027_price_lt_90",
        "code": "118027",
        "kind": "price_lt",
        "value": 90.0,
        "note": "宏图转债",
        "memo": "遥感应用领域第一梯队，但生存空间被压榨，现金流为负，"
        "关注ST风险，90以下可考虑博资产重组",
    },
    {
        "id": "110092_price_lt_99",
        "code": "110092",
        "kind": "price_lt",
        "value": 99.0,
        "note": "三房转债",
        "memo": "现金流压力大，剩余规模24亿，" "但和地方国资有深层链接，有护盘可能",
    },
]
