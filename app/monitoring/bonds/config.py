"""可转债监控规则（可按需增删）。"""

from __future__ import annotations

from typing import Any

CB_MONITOR_RULES: list[dict[str, Any]] = [
    {
        "id": "113575_price_lt_65",
        "code": "113575",
        "kind": "price_lt",
        "value": 65.0,
        "note": "东杰转债",
    },
]
