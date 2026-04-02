"""交易时段（与数据源解耦，便于单测替换）。"""

from __future__ import annotations

from datetime import datetime


def get_market_phase() -> str:
    now = datetime.now().strftime("%H:%M")
    if now < "09:15":
        return "pre"
    if now < "09:25":
        return "auction"
    if now < "09:30":
        return "pre_open"
    if now < "11:30":
        return "morning"
    if now < "13:00":
        return "lunch"
    if now < "14:57":
        return "afternoon"
    if now < "15:00":
        return "close_auction"
    return "closed"


PHASE_CN = {
    "pre": "盘前",
    "auction": "集合竞价",
    "pre_open": "待开盘",
    "morning": "上午交易",
    "lunch": "午休",
    "afternoon": "下午交易",
    "close_auction": "尾盘竞价",
    "closed": "已收盘",
}

ACTIVE_PHASES = frozenset({"auction", "pre_open", "morning", "afternoon", "close_auction"})
