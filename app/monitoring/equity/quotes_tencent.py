"""腾讯行情数据源（Adapter / Gateway）。"""

from __future__ import annotations

import logging

import requests as http_requests

logger = logging.getLogger(__name__)

QUOTE_URL = "https://qt.gtimg.cn/q="


def fetch_realtime_quotes(symbols: list[str]) -> dict[str, dict]:
    if not symbols:
        return {}
    query = ",".join(symbols)
    try:
        r = http_requests.get(f"{QUOTE_URL}{query}", timeout=5)
        r.encoding = "gbk"
        text = r.text
    except Exception as e:
        logger.warning("行情请求失败: %s", e)
        return {}

    results: dict[str, dict] = {}
    for line in text.strip().split("\n"):
        line = line.strip()
        if "~" not in line:
            continue
        parts = line.split("~")
        if len(parts) < 49:
            continue
        sym_raw = line.split("=")[0].split("_")[-1] if "=" in line else ""
        if not sym_raw:
            continue
        try:
            current = float(parts[3]) if parts[3] else 0.0
            prev_close = float(parts[4]) if parts[4] else 0.0
            today_open = float(parts[5]) if parts[5] else 0.0
            high = float(parts[33]) if parts[33] else 0.0
            low = float(parts[34]) if parts[34] else 0.0
            change_pct = float(parts[32]) if parts[32] else 0.0
            limit_up = float(parts[47]) if parts[47] else 0.0
            limit_down = float(parts[48]) if parts[48] else 0.0
        except (ValueError, IndexError):
            continue
        results[sym_raw] = {
            "name": parts[1],
            "code": parts[2],
            "price": current,
            "prev_close": prev_close,
            "open": today_open,
            "high": high,
            "low": low,
            "change_pct": change_pct,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "time": parts[30] if len(parts) > 30 else "",
        }
    return results
