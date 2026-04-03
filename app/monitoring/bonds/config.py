"""可转债监控规则（可按需增删）。"""

from __future__ import annotations

from typing import Any

CB_MONITOR_RULES: list[dict[str, Any]] = [
    {
        "id": "113575_price_lt_65",
        "code": "113575",
        "kind": "price_lt",
        "value": 65.0,
        "note": "东时转债",
    },
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
    },
    {
        "id": "110081_notify_open",
        "code": "110081",
        "kind": "notify_open",
        "note": "闻泰转债",
    },
]
