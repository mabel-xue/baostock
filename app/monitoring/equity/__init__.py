"""场内股票 / ETF 监控子域。"""

from .config import INVESTMENT_NOTES, POLL_INTERVAL, WATCHLIST
from .models import PriceTarget, SymbolState
from .service import load_watchlist, monitor, parse_targets_cli

__all__ = [
    "WATCHLIST",
    "INVESTMENT_NOTES",
    "POLL_INTERVAL",
    "PriceTarget",
    "SymbolState",
    "load_watchlist",
    "monitor",
    "parse_targets_cli",
]
