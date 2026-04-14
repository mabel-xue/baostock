"""领域模型：标的状态与目标价。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PriceTarget:
    symbol: str
    target_price: float
    direction: str  # "买入" or "卖出"
    memo: str = ""
    notes: list[str] | None = None
    triggered: bool = False
    # 相对昨收涨跌幅阈值（%），与行情 change_pct 同号；配置了 rate 时按涨跌幅判定，忽略 price
    rate: float | None = None

    def __post_init__(self) -> None:
        if self.notes is None:
            self.notes = []


@dataclass
class SymbolState:
    symbol: str
    alias: str = ""
    name: str = ""
    memo: str = ""
    prev_close: float = 0.0
    open_price: float = 0.0
    open_notified: bool = False
    last_price: float = 0.0
    high: float = 0.0
    low: float = 0.0
    change_pct: float = 0.0
    auction_price: float = 0.0
    auction_notified_at: float = 0.0
    pre_close_chg_notified: bool = False
