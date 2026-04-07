"""
可转债监控应用服务：规则求值、状态持久化、通知。
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..infrastructure.notifications import get_secret, get_webhook, send_feishu_post, send_feishu_text
from .config import CB_MONITOR_RULES
from .quotes_akshare import fetch_spot_by_code


def _code6(s: str) -> str:
    return str(s).strip().replace(".0", "").zfill(6)


def _rule_triggered(row: pd.Series, rule: dict) -> bool:
    price = float(row["现价"])
    if pd.isna(price):
        return False
    kind = rule["kind"]
    val = float(rule["value"])
    if kind == "price_lt":
        return price < val
    if kind == "price_gt":
        return price > val
    raise ValueError(f"未知 kind: {kind}")


def _rule_message(rule: dict, row: pd.Series) -> str:
    name = row.get("name", "")
    code = rule["code"]
    price = row["现价"]
    note = rule.get("note") or ""
    head = f"【可转债监控】{note} ({code}" + (f" {name}" if name else "") + ")"
    if rule["kind"] == "price_lt":
        cond = f"现价 {price:.3f} < {rule['value']}"
    else:
        cond = f"现价 {price:.3f} > {rule['value']}"
    memo = rule.get("memo") or ""
    parts = [head, cond]
    if memo:
        parts.append(f"备注: {memo}")
    parts.append(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(parts)


def _fmt_change(price: float, prev_close: float) -> str:
    diff = price - prev_close
    pct = diff / prev_close * 100 if prev_close else 0
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.3f} ({sign}{pct:.2f}%)"


def _handle_notify_open(
    rid: str,
    rule: dict,
    row: pd.Series,
    state: dict[str, Any],
    webhook: str | None,
    secret: str | None,
    dry_run: bool,
) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    prev = state.get(rid, {})
    if prev.get("notified_date") == today:
        return

    open_price = pd.to_numeric(row.get("open"), errors="coerce")
    prev_close = pd.to_numeric(row.get("settlement"), errors="coerce")
    if pd.isna(open_price) or open_price <= 0:
        return

    note = rule.get("note") or rule["code"]
    change = _fmt_change(open_price, prev_close) if pd.notna(prev_close) else ""
    print(f"  [{rid}] 开盘价={open_price:.3f}  {change}")

    title = f"开盘价 {note}({rule['code']})"
    lines = [f"开盘价: {open_price:.3f}"]
    if pd.notna(prev_close):
        lines.append(f"昨收: {prev_close:.3f}")
        lines.append(f"涨跌: {change}")
    memo = rule.get("memo")
    if memo:
        lines.append(f"备注: {memo}")

    if dry_run or not webhook:
        print(f"    [DRY] 飞书: {title} | {' | '.join(lines)}")
    else:
        send_feishu_post(webhook, title, lines, secret=secret)
        print(f"    已发送飞书: {title}")

    state[rid] = {"notified_date": today, "open_price": float(open_price)}


def project_app_dir() -> Path:
    """app/ 目录（含 output/）。"""
    return Path(__file__).resolve().parents[2]


def state_path() -> Path:
    out = project_app_dir() / "output"
    out.mkdir(parents=True, exist_ok=True)
    return out / "cb_monitor_state.json"


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def run_cb_round(
    *,
    rules: list[dict[str, Any]],
    state: dict[str, Any],
    webhook: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    secret = get_secret()
    df = fetch_spot_by_code()
    by_code = df.set_index("_code6", drop=False)

    for rule in rules:
        rid = rule["id"]
        code = _code6(rule["code"])
        if code not in by_code.index:
            print(f"  [{rid}] 未在行情中查到代码 {code}，跳过")
            continue
        row = by_code.loc[code]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]

        if rule["kind"] == "notify_open":
            _handle_notify_open(rid, rule, row, state, webhook, secret, dry_run)
            continue

        ok = _rule_triggered(row, rule)
        prev = state.get(rid, {})
        was_on = bool(prev.get("triggered"))

        if ok:
            print(f"  [{rid}] 满足条件 现价={row['现价']:.3f}")
            if not was_on:
                msg = _rule_message(rule, row)
                if dry_run or not webhook:
                    print(f"    [DRY] 飞书: {msg.replace(chr(10), ' | ')}")
                else:
                    send_feishu_text(webhook, msg, secret=secret)
                    print("    已发送飞书")
            state[rid] = {"triggered": True, "last_price": float(row["现价"]), "ts": time.time()}
        else:
            if was_on:
                print(f"  [{rid}] 已恢复（不再满足），下次满足将再次通知")
            state[rid] = {
                "triggered": False,
                "last_price": float(row["现价"]) if pd.notna(row["现价"]) else None,
            }

    return state


def run_cb_forever(
    *,
    interval: int,
    once: bool,
    dry_run: bool,
    rules: list[dict[str, Any]] | None = None,
) -> None:
    st_path = state_path()
    webhook = get_webhook() or None
    rules = rules if rules is not None else CB_MONITOR_RULES

    if not dry_run and not once and not webhook:
        print("警告: 未设置 FEISHU_WEBHOOK_URL，可转债监控将以 dry-run 模式运行。")
        dry_run = True

    print(f"可转债监控 {len(rules)} 条；间隔 {interval}s；状态 {st_path}")

    while True:
        print(f"\n=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 可转债行情 ===")
        state = load_state(st_path)
        try:
            state = run_cb_round(
                rules=rules,
                state=state,
                webhook=webhook,
                dry_run=dry_run or not webhook,
            )
            save_state(st_path, state)
        except Exception as e:
            print(f"本轮失败: {e}")
            if webhook and not dry_run:
                try:
                    send_feishu_text(
                        webhook,
                        f"【可转债监控】本轮异常: {e}\n{datetime.now()}",
                        secret=get_secret(),
                    )
                except Exception as e2:
                    print(f"飞书报错失败: {e2}")

        if once:
            break
        print(f"睡眠 {interval} 秒 …")
        time.sleep(interval)
