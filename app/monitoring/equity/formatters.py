"""展示格式化（与领域规则无关）。"""

from __future__ import annotations

from .models import SymbolState


def fmt_change(price: float, prev_close: float) -> str:
    if prev_close <= 0:
        return ""
    diff = price - prev_close
    pct = diff / prev_close * 100
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.3f} ({sign}{pct:.2f}%)"


def fmt_price_line(state: SymbolState) -> str:
    label = state.alias or state.name
    change = fmt_change(state.last_price, state.prev_close)
    return (
        f"  {label}({state.symbol})  "
        f"现价={state.last_price:.3f}  {change}  "
        f"高={state.high:.3f}  低={state.low:.3f}"
    )
