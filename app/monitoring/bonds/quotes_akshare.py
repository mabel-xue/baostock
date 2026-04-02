"""AkShare 可转债现货行情（Adapter）。"""

from __future__ import annotations

import akshare as ak
import pandas as pd

from anti_throttle import throttle


def _effective_price(df: pd.DataFrame) -> pd.Series:
    trade = pd.to_numeric(df["trade"], errors="coerce")
    settle = pd.to_numeric(df["settlement"], errors="coerce")
    return trade.where(trade > 0, settle)


def _code6(s: str) -> str:
    return str(s).strip().replace(".0", "").zfill(6)


def fetch_spot_by_code() -> pd.DataFrame:
    throttle.patch_akshare()
    df = ak.bond_zh_hs_cov_spot()
    df = df.copy()
    df["_code6"] = df["code"].map(_code6)
    df["现价"] = _effective_price(df)
    return df
