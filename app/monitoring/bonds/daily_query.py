"""
与 app/query_convertible_bonds.py 一致的筛选与落盘；收盘前每日快照；
全市场新代码轮询；保留池（kept）跨日新进通知。
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from anti_throttle import throttle

from ..equity.market_clock import get_market_phase
from .quotes_akshare import fetch_bond_zh_hs_cov_spot
from ..infrastructure.notifications import get_secret, get_webhook, send_feishu_post
from . import config as bonds_config


def _app_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def _ensure_app_on_path() -> None:
    ad = _app_dir()
    if str(ad) not in sys.path:
        sys.path.insert(0, str(ad))


def universe_state_path() -> Path:
    out = _app_dir() / "output"
    out.mkdir(parents=True, exist_ok=True)
    return out / "cb_universe_state.json"


def load_universe_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_universe_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _code6_series(df: pd.DataFrame) -> pd.Series:
    return (
        df["code"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )


def _kept_codes_sorted(kept: pd.DataFrame) -> list[str]:
    if kept is None or kept.empty or "code" not in kept.columns:
        return []
    return sorted(_code6_series(kept).unique().tolist())


def _code_name_map_kept(kept: pd.DataFrame) -> dict[str, str]:
    if kept.empty or "code" not in kept.columns:
        return {}
    m: dict[str, str] = {}
    for _, row in kept.iterrows():
        c = str(row["code"]).strip().replace(".0", "").zfill(6)
        m[c] = str(row.get("name", "") or "")
    return m


def _maybe_notify_kept_pool_newcomers(
    *,
    kept: pd.DataFrame,
    kept_codes: list[str],
    state: dict[str, Any],
    dry_run: bool,
) -> list[str]:
    """
    相对 state 中上一日保留池 last_kept_snapshot_codes，找出今日新进入保留池的代码；
    首次无基线时不通知。返回本次新进代码列表。
    """
    if not bonds_config.CB_KEPT_NEW_NOTIFY_ENABLED:
        return []

    prev = list(state.get("last_kept_snapshot_codes") or [])
    prev_set = set(prev)
    if not prev_set:
        print(
            f"[cb_kept] 首次记录保留池基线 {len(kept_codes)} 只，"
            "不推送「新进保留池」通知（下一交易日起比对增量）"
        )
        return []

    new_codes = sorted(set(kept_codes) - prev_set)
    if not new_codes:
        return []

    names = _code_name_map_kept(kept)
    lines = [
        f"相对上一日保留池新增 {len(new_codes)} 只（query_convertible_bonds 筛选后 kept）：",
    ]
    for c in new_codes[:80]:
        lines.append(f"  · {c} {names.get(c, '')}".rstrip())
    if len(new_codes) > 80:
        lines.append(f"  … 另有 {len(new_codes) - 80} 只未列出")

    webhook = get_webhook() or None
    secret = get_secret()
    title = f"可转债保留池新进（{len(new_codes)} 只）"
    if dry_run or not webhook:
        print(f"[cb_kept] [DRY] {title}\n" + "\n".join(lines))
    else:
        send_feishu_post(webhook, title, lines, secret=secret)
        print(f"[cb_kept] 已飞书通知: {title}")

    state["last_kept_new_alert"] = datetime.now().isoformat(timespec="seconds")
    return new_codes


def _import_query_module():
    _ensure_app_on_path()
    import query_convertible_bonds as q  # noqa: PLC0415

    return q


def run_pipeline(
    *,
    min_price: float | None = None,
    max_years: float | None = None,
    max_price: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """返回 (raw, kept, excluded, summary_line)。"""
    min_price = float(
        min_price if min_price is not None else bonds_config.CB_QUERY_DEFAULTS["min_price"]
    )
    max_years = float(
        max_years if max_years is not None else bonds_config.CB_QUERY_DEFAULTS["max_years"]
    )
    max_price = float(
        max_price if max_price is not None else bonds_config.CB_QUERY_DEFAULTS["max_price"]
    )

    q = _import_query_module()
    throttle.patch_akshare()
    raw = q.fetch_cov_spot_with_maturity()
    kept, excluded = q.split_kept_excluded(
        raw,
        min_price=min_price,
        max_years=max_years,
        max_price=max_price,
    )
    has_cease = raw["停止交易日"].notna()
    with_cease = int(has_cease.sum())
    summary = (
        f"现货 {len(raw)} 只，其中 {with_cease} 只合并到东财停止交易日(CEASE_DATE)。"
        f"剔除条件：① 距停止交易日 0～{max_years} 年内且现价 > {min_price}；"
        f"② volume=0；③ 现价 > {max_price}。"
        f"剔除 {len(excluded)} 只，保留 {len(kept)} 只。"
    )
    return raw, kept, excluded, summary


def save_daily_csvs(
    kept: pd.DataFrame,
    excluded: pd.DataFrame,
    *,
    day: str | None = None,
) -> tuple[Path, Path]:
    """按交易日写入 output/cb_daily/，文件名含日期。"""
    out_dir = _app_dir() / "output" / "cb_daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    d = day or datetime.now().strftime("%Y%m%d")
    p_kept = out_dir / f"convertible_bonds_kept_{d}.csv"
    p_excluded = out_dir / f"convertible_bonds_dropped_{d}.csv"
    kept.to_csv(p_kept, index=False, encoding="utf-8-sig")
    excluded.to_csv(p_excluded, index=False, encoding="utf-8-sig")
    return p_kept, p_excluded


def _is_cn_weekday() -> bool:
    return datetime.now().weekday() < 5


def should_run_close_window_snapshot(*, force: bool) -> bool:
    if force:
        return True
    if not _is_cn_weekday():
        return False
    return get_market_phase() == "close_auction"


def run_daily_snapshot(
    *,
    force: bool,
    persist: bool = True,
    dry_run: bool = False,
    state_path: Path | None = None,
) -> dict[str, Any] | None:
    """
    执行与 query_convertible_bonds 相同逻辑并落盘；更新 known_codes 与 last_kept_snapshot_codes。
    若非 force 且不在尾盘竞价窗口或今日已写过，则返回 None。
    persist=False 时不写 CSV/状态（仅控制台预览，供 CLI 调试）。
    dry_run=True 时不发保留池新进飞书（仍可在 persist 时更新状态与 CSV）。
    """
    st_path = state_path or universe_state_path()
    state = load_universe_state(st_path)
    today = datetime.now().strftime("%Y-%m-%d")

    if not force:
        if not should_run_close_window_snapshot(force=False):
            return None
        if state.get("last_daily_snapshot_date") == today:
            return None

    raw, kept, excluded, summary = run_pipeline()
    kept_codes = _kept_codes_sorted(kept)
    day_compact = datetime.now().strftime("%Y%m%d")
    if persist:
        p_kept, p_excl = save_daily_csvs(kept, excluded, day=day_compact)
    else:
        p_kept = p_excl = None

    codes = sorted(_code6_series(raw).unique().tolist())
    prev = set(state.get("known_codes") or [])
    merged = sorted(prev | set(codes))
    state["known_codes"] = merged
    state["last_daily_snapshot_date"] = today
    state["last_daily_summary"] = summary
    state["last_daily_paths"] = (
        {"kept": str(p_kept), "excluded": str(p_excl)} if persist and p_kept else {}
    )
    if persist:
        if bonds_config.CB_KEPT_NEW_NOTIFY_ENABLED:
            _maybe_notify_kept_pool_newcomers(
                kept=kept, kept_codes=kept_codes, state=state, dry_run=dry_run
            )
        state["last_kept_snapshot_codes"] = kept_codes
        save_universe_state(st_path, state)

    print(f"\n[cb_daily] {summary}")
    if persist and p_kept is not None:
        print(f"[cb_daily] 保留 CSV: {p_kept}")
        print(f"[cb_daily] 剔除 CSV: {p_excl}")
    elif not persist:
        print("[cb_daily] persist=False，未写入 CSV/状态")

    return {
        "summary": summary,
        "kept_path": str(p_kept) if p_kept else "",
        "excluded_path": str(p_excl) if p_excl else "",
        "raw_count": len(raw),
        "state": state,
    }


def fetch_spot_codes_only() -> set[str]:
    df = fetch_bond_zh_hs_cov_spot()
    if df.empty:
        return set()
    return set(_code6_series(df).tolist())


def poll_new_convertible_bonds(
    *,
    dry_run: bool,
    state_path: Path | None = None,
) -> list[str]:
    """
    轻量轮询：仅对比现货代码集与 state 中 known_codes。
    首次基线：写入 known_codes，不通知。
    返回本次新增的代码列表（可能为空）。
    """
    st_path = state_path or universe_state_path()
    state = load_universe_state(st_path)
    current = fetch_spot_codes_only()
    if not current:
        return []

    known = set(state.get("known_codes") or [])
    if not known:
        state["known_codes"] = sorted(current)
        state["baseline_note"] = "首次建立转债代码基线，未推送新债通知"
        save_universe_state(st_path, state)
        print(f"[cb_new] 基线已建立，共 {len(current)} 只转债代码")
        return []

    new_codes = sorted(current - known)
    if not new_codes:
        return []

    # 名称映射（尽力而为）
    df = fetch_bond_zh_hs_cov_spot()
    code_to_name: dict[str, str] = {}
    if not df.empty:
        for _, row in df.iterrows():
            c = str(row["code"]).strip().replace(".0", "").zfill(6)
            code_to_name[c] = str(row.get("name", ""))

    lines = [f"共 {len(new_codes)} 只新代码（相对上次已知集合）:"]
    for c in new_codes[:50]:
        nm = code_to_name.get(c, "")
        lines.append(f"  · {c} {nm}".rstrip())
    if len(new_codes) > 50:
        lines.append(f"  … 另有 {len(new_codes) - 50} 只未列出")

    known.update(new_codes)
    state["known_codes"] = sorted(known)
    state["last_new_bond_alert"] = datetime.now().isoformat(timespec="seconds")
    save_universe_state(st_path, state)

    webhook = get_webhook() or None
    secret = get_secret()
    title = f"新可转债代码（{len(new_codes)} 只）"
    if dry_run or not webhook:
        print(f"[cb_new] [DRY] {title}\n" + "\n".join(lines))
    else:
        send_feishu_post(webhook, title, lines, secret=secret)
        print(f"[cb_new] 已飞书通知: {title}")

    return new_codes


def notify_daily_snapshot_feishu(result: dict[str, Any], *, dry_run: bool) -> None:
    webhook = get_webhook() or None
    secret = get_secret()
    summary = result.get("summary", "")
    paths = (result.get("state") or {}).get("last_daily_paths") or {}
    lines = [
        summary,
        f"保留: {paths.get('kept', '')}",
        f"剔除: {paths.get('excluded', '')}",
    ]
    title = "可转债日终快照（query_convertible_bonds）"
    if dry_run or not webhook:
        print(f"[cb_daily] [DRY] {title} | " + " | ".join(lines))
        return
    send_feishu_post(webhook, title, lines, secret=secret)
