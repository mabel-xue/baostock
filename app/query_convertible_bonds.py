"""
沪深可转债列表：过滤掉「剩余年限不足且价格偏高」的标的。

数据源：
- bond_zh_hs_cov_spot（AkShare）：实时/快照行情（现价优先取 trade，为 0 时用 settlement）
- 东方财富 datacenter RPT_BOND_CB_LIST：字段 **CEASE_DATE**（输出列名「停止交易日」），
  一般为摘牌前最后交易日，常比 EXPIRE_DATE 早一日。

默认规则：剔除满足以下任一条件的转债——
  (1) 东财 CEASE_DATE 已合并、距停止交易日大于 0 且不足 1 年（按自然日折算）且现价 > 90；
  (2) 成交量 volume = 0；
  (3) 现价 > 118（与有效现价列「现价」一致）。
其余保留。输出含「停止交易日」（接口原值）及「剩余期限」（如 1年70天、10天，按 365 天为 1 年折算）。

用法：
  项目根目录：python app/query_convertible_bonds.py
  app 目录内： python query_convertible_bonds.py

  参数（均有默认值，可组合使用）：
    --min-price   与短久期联动，默认 90（年限不足且现价高于此值则剔除）
    --max-years   与 min-price 联动，默认 1（年）
    --max-price   现价高于此值即剔除（与年限无关），默认 118
    --no-save     不写 output/ 下 CSV
    --preview N   控制台只打印保留表前 N 行，默认 20；0 表示打印全部

  示例：
    python app/query_convertible_bonds.py --min-price 95 --max-years 0.5 --max-price 120
    python app/query_convertible_bonds.py --no-save --preview 0

  输出（默认写入项目 output/，文件名带时间戳）：
    convertible_bonds_dropped_*.csv — 满足剔除条件的标的
    convertible_bonds_kept_*.csv    — 保留的标的
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd
import requests

from anti_throttle import throttle


def _bond_code_key(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(6)


def _effective_price(df: pd.DataFrame) -> pd.Series:
    trade = pd.to_numeric(df["trade"], errors="coerce")
    settle = pd.to_numeric(df["settlement"], errors="coerce")
    return trade.where(trade > 0, settle)


def _fmt_cease_remain_days(delta_days: float) -> str:
    """距停止交易日的间隔天数 -> 「1年70天」「10天」「已到期」。"""
    if delta_days is None or (isinstance(delta_days, float) and pd.isna(delta_days)):
        return ""
    d = int(delta_days)
    if d < 0:
        return "已到期"
    y, r = divmod(d, 365)
    if y > 0:
        return f"{y}年{r}天"
    return f"{d}天"


def _series_remain_until_cease(cease_col: pd.Series, today: pd.Timestamp) -> pd.Series:
    cease = pd.to_datetime(cease_col, errors="coerce").dt.normalize()
    t0 = pd.Timestamp(today).normalize()
    delta = (cease - t0).dt.days
    return delta.map(
        lambda x: _fmt_cease_remain_days(x) if pd.notna(x) else ""
    )


def fetch_eastmoney_cb_cease_date() -> pd.DataFrame:
    """
    东方财富可转债列表 RPT_BOND_CB_LIST，取 SECURITY_CODE + CEASE_DATE。
    与 AkShare bond_zh_cov 同源接口，仅拉取停止交易日字段以减轻体积。
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    base = {
        "sortColumns": "PUBLIC_START_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "reportName": "RPT_BOND_CB_LIST",
        "columns": "SECURITY_CODE,CEASE_DATE",
        "source": "WEB",
        "client": "WEB",
    }
    rows: list[dict] = []
    page = 1
    while True:
        params = {**base, "pageNumber": str(page)}
        r = requests.get(url, params=params, timeout=45)
        r.raise_for_status()
        payload = r.json()
        result = payload.get("result") or {}
        data = result.get("data") or []
        rows.extend(data)
        pages = int(result.get("pages") or 1)
        if page >= pages:
            break
        page += 1

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.rename(columns={"SECURITY_CODE": "债券代码", "CEASE_DATE": "停止交易日"})
    return df


def fetch_cov_spot_with_maturity() -> pd.DataFrame:
    """拉取现货并与东财 CEASE_DATE 合并为「停止交易日」列；现价由行情计算。"""
    spot = ak.bond_zh_hs_cov_spot()
    info = fetch_eastmoney_cb_cease_date()

    spot = spot.copy()
    spot["_code6"] = _bond_code_key(spot["code"])

    info = info.copy()
    info["_code6"] = _bond_code_key(info["债券代码"])

    mat = info[["_code6", "停止交易日"]].drop_duplicates(subset=["_code6"], keep="first")
    merged = spot.merge(mat, on="_code6", how="left")

    merged["现价"] = _effective_price(merged)

    return merged


def split_kept_excluded(
    df: pd.DataFrame,
    *,
    min_price: float,
    max_years: float,
    max_price: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    剔除条件（满足任一则进 excluded）：
    - 「停止交易日」可解析且 0 < 距该日年数 < max_years 且 现价 > min_price（年数仅用于规则，不落表）；
    - 或 volume 数值为 0；
    - 或 现价 > max_price。
    返回 (保留表, 剔除表)，剔除表即满足上述条件的行。
    """
    vol = pd.to_numeric(df["volume"], errors="coerce")
    m_vol = vol == 0

    cease = pd.to_datetime(df["停止交易日"], errors="coerce")
    today = pd.Timestamp.now().normalize()
    years_left = (cease - today).dt.days / 365.25

    m_tenor_price = (
        cease.notna()
        & years_left.notna()
        & (years_left > 0)
        & (years_left < max_years)
        & (df["现价"] > min_price)
    )
    m_high = df["现价"] > max_price
    m = m_tenor_price | m_vol | m_high

    excluded = df.loc[m].copy()
    kept = df.loc[~m].copy()
    _exp = pd.to_datetime(kept["停止交易日"], errors="coerce")
    kept = kept.assign(_e=_exp).sort_values("_e", ascending=True, na_position="last").drop(
        columns=["_e"]
    )
    _ex = pd.to_datetime(excluded["停止交易日"], errors="coerce")
    excluded = excluded.assign(_e=_ex).sort_values("_e", ascending=True, na_position="last").drop(
        columns=["_e"]
    )

    kept["剩余期限"] = _series_remain_until_cease(kept["停止交易日"], today)
    excluded["剩余期限"] = _series_remain_until_cease(excluded["停止交易日"], today)

    cols = [
        "symbol",
        "code",
        "name",
        "现价",
        "trade",
        "settlement",
        "changepercent",
        "停止交易日",
        "剩余期限",
        "volume",
        "amount",
        "ticktime",
    ]
    cols = [c for c in cols if c in kept.columns]
    kept = kept[cols]
    excluded = excluded[cols]
    return kept, excluded


def main() -> None:
    parser = argparse.ArgumentParser(
        description="沪深可转债：剔除短久期+区间价、volume=0、或超高价标的"
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=90.0,
        help="与停止交易日联动：距该日不足 max-years 且现价高于此值则剔除（默认 90）",
    )
    parser.add_argument(
        "--max-price",
        type=float,
        default=118.0,
        help="现价严格高于此值则剔除，与年限无关（默认 118）",
    )
    parser.add_argument(
        "--max-years",
        type=float,
        default=1.0,
        help="与现价联动：距到期年数严格小于此值且价高时会被剔除（默认 1）",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="不写入 output/ CSV",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=20,
        help="控制台只打印「保留」列表前 N 行（默认 20）；0 表示打印全部",
    )
    args = parser.parse_args()

    throttle.patch_akshare()

    raw = fetch_cov_spot_with_maturity()
    kept, excluded = split_kept_excluded(
        raw,
        min_price=args.min_price,
        max_years=args.max_years,
        max_price=args.max_price,
    )

    has_cease = raw["停止交易日"].notna()
    with_cease = int(has_cease.sum())
    print(
        f"现货 {len(raw)} 只，其中 {with_cease} 只合并到东财停止交易日(CEASE_DATE)。"
        f"剔除条件：① 距停止交易日 0～{args.max_years} 年内且现价 > {args.min_price}；"
        f"② volume=0；③ 现价 > {args.max_price}。"
    )
    print(f"剔除 {len(excluded)} 只，保留 {len(kept)} 只\n")

    if kept.empty:
        print("(保留结果为空)")
        return

    n = args.preview
    if n <= 0:
        to_show = kept
        note = ""
    else:
        to_show = kept.head(n)
        if len(kept) > len(to_show):
            tail = (
                "完整列表见已保存 CSV。"
                if not args.no_save
                else "请使用 --preview 0 查看全部。"
            )
            note = f"\n（控制台仅显示前 {len(to_show)} 行，共 {len(kept)} 行；{tail}）\n"
        else:
            note = ""
    if note:
        print(note)
    with pd.option_context("display.max_rows", None, "display.width", 200):
        print(to_show.to_string(index=False))

    if not args.no_save:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out_dir = os.path.join(root, "output")
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        # 文件名与内容一致：excluded = 满足剔除条件的行；kept = 保留
        path_excluded = os.path.join(out_dir, f"convertible_bonds_dropped_{ts}.csv")
        path_kept = os.path.join(out_dir, f"convertible_bonds_kept_{ts}.csv")
        excluded.to_csv(path_excluded, index=False, encoding="utf-8-sig")
        kept.to_csv(path_kept, index=False, encoding="utf-8-sig")
        print(f"\n剔除（满足条件）: {path_excluded}")
        print(f"保留: {path_kept}")


if __name__ == "__main__":
    main()
