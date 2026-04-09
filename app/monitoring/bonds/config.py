"""可转债监控规则（可按需增删）。"""

from __future__ import annotations

from typing import Any

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
        "id": "110081_price_lt_108",
        "code": "110081",
        "kind": "price_lt",
        "value": 108.0,
        "note": "闻泰转债",
        "memo": "现金流充足、子公司安世半导体是全球功率分立器件巨头，"
                "车规级逻辑器件/ESD保护器件细分市场全球第一或第二，"
                "客户涵盖几乎所有主流汽车一级供应商。"
                "目前因海外监管因素计提巨额资产减值准备（临时性）",
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
        "memo": "现金流压力大，剩余规模24亿，"
                "但和地方国资有深层链接，有护盘可能",
    },
]
