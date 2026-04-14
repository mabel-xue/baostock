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


def fetch_bond_zh_hs_cov_spot() -> pd.DataFrame:
    """
    新浪财经沪深可转债现货（AkShare bond_zh_hs_cov_spot）。

    接口偶发返回空或非 JSON 正文时，底层 demjson 会报「No value to decode」；
    此处带 UA 刷新与指数退避重试，降低监控轮询中的误报。
    """
    throttle.patch_akshare()
    df = throttle.retry(
        lambda: ak.bond_zh_hs_cov_spot(),
        "沪深可转债现货(sina)",
        retries=5,
    )
    if df is None:
        raise RuntimeError(
            "获取沪深可转债现货失败：新浪财经接口多次返回无效数据（空或非 JSON），"
            "常见于网络抖动或限流。请稍后重试。"
        )
    return df


def fetch_spot_by_code() -> pd.DataFrame:
    df = fetch_bond_zh_hs_cov_spot()
    df = df.copy()
    df["_code6"] = df["code"].map(_code6)
    df["现价"] = _effective_price(df)
    return df
