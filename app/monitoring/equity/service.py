"""
场内盯盘应用服务：编排行情拉取、时段、目标价与通知。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from ..infrastructure.notifications import get_secret, send_feishu_post
from .config import WATCHLIST
from .formatters import fmt_change, fmt_price_line
from .market_clock import ACTIVE_PHASES, PHASE_CN, get_market_phase
from .models import PriceTarget, SymbolState
from .quotes_tencent import fetch_realtime_quotes

logger = logging.getLogger(__name__)


def _pre_close_change_should_fire(phase: str, now_hm: str, at_cfg: str) -> bool:
    """at_cfg 为 HH:MM。>=15:00 的配置在尾盘竞价段首帧触发（A 股无 15:50 交易时点）。"""
    at = at_cfg.strip()
    if not at or len(at) < 4:
        return False
    if at >= "15:00":
        return phase == "close_auction"
    return phase in ("afternoon", "close_auction") and now_hm >= at


def load_watchlist() -> tuple[
    list[str],
    list[PriceTarget],
    dict[str, str],
    dict[str, str],
    dict[str, float],
    dict[str, str],
]:
    symbols: list[str] = []
    targets: list[PriceTarget] = []
    alias_map: dict[str, str] = {}
    memo_map: dict[str, str] = {}
    open_drop_alerts: dict[str, float] = {}
    pre_close_change_pct_at: dict[str, str] = {}
    for row in WATCHLIST:
        if row.get("poll", True) is False:
            continue
        sym = row["code"]
        alias = row["alias"]
        memo = str(row.get("memo") or "")
        symbols.append(sym)
        alias_map[sym] = alias
        if memo:
            memo_map[sym] = memo
        drop_pct = row.get("open_drop_alert_pct")
        if drop_pct is not None:
            open_drop_alerts[sym] = float(drop_pct)
        raw_pc = row.get("pre_close_change_pct_notify_at")
        if raw_pc is not None and str(raw_pc).strip():
            pre_close_change_pct_at[sym] = str(raw_pc).strip()
        raw_notes = row.get("investment_notes")
        notes = list(raw_notes) if raw_notes else []
        for pt in row.get("price_targets") or []:
            raw_rate = pt.get("rate")
            rate = float(raw_rate) if raw_rate is not None else None
            raw_price = pt.get("price")
            t_price = float(raw_price) if raw_price is not None else 0.0
            if rate is None and raw_price is None:
                continue
            targets.append(
                PriceTarget(
                    symbol=sym,
                    target_price=t_price,
                    direction=str(pt["direction"]),
                    memo=memo,
                    notes=notes,
                    rate=rate,
                )
            )
    return symbols, targets, alias_map, memo_map, open_drop_alerts, pre_close_change_pct_at


def parse_targets_cli(targets_str: str) -> list[PriceTarget]:
    results: list[PriceTarget] = []
    for item in targets_str.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) < 3:
            logger.warning("目标价格式错误（需 symbol:price:方向）: %s", item)
            continue
        sym = parts[0].strip()
        try:
            price = float(parts[1].strip())
        except ValueError:
            logger.warning("价格解析失败: %s", item)
            continue
        direction = parts[2].strip()
        if direction not in ("买入", "卖出"):
            logger.warning("方向需为 '买入' 或 '卖出': %s", item)
            continue
        results.append(PriceTarget(symbol=sym, target_price=price, direction=direction))
    return results


def check_target(target: PriceTarget, state: SymbolState) -> str | None:
    if target.triggered:
        return None
    price = state.last_price
    if price <= 0:
        return None
    hit = False
    chg = state.change_pct
    if target.rate is not None:
        if state.prev_close <= 0:
            return None
        r = float(target.rate)
        if target.direction == "卖出":
            hit = chg >= r
        elif target.direction == "买入":
            hit = chg <= -r
    elif target.direction == "买入" and price <= target.target_price:
        hit = True
    elif target.direction == "卖出" and price >= target.target_price:
        hit = True
    if not hit:
        return None
    target.triggered = True
    label = state.alias or state.name
    action = "买入" if target.direction == "买入" else "卖出"
    verb = "跌至" if action == "买入" else "涨至"
    if target.rate is not None:
        r = float(target.rate)
        if action == "卖出":
            cond = f"涨幅 ≥ {r:.2f}%"
        else:
            cond = f"跌幅 ≥ {r:.2f}%"
        lines = [
            f"【{action}提醒】{label}({state.symbol}) "
            f"现价 {price:.3f}，较昨收涨跌幅 {chg:+.2f}%，已满足{cond}。",
            f"  昨收={state.prev_close:.3f}  {fmt_change(price, state.prev_close)}",
        ]
    else:
        lines = [
            f"【{action}提醒】{label}({state.symbol}) "
            f"现价 {price:.3f} 已{verb}目标价 {target.target_price:.3f}！",
            f"  昨收={state.prev_close:.3f}  {fmt_change(price, state.prev_close)}",
        ]
    if target.memo:
        lines.append(f"  操作: {target.memo}")
    if target.notes:
        lines.append("  投资逻辑:")
        for note in target.notes:
            lines.append(f"    · {note}")
    lines.append("  （程序提示，不构成投资建议）")
    return "\n".join(lines)


def monitor(
    symbols: list[str],
    targets: list[PriceTarget],
    interval: int,
    webhook_url: str,
    *,
    alias_map: dict[str, str] | None = None,
    memo_map: dict[str, str] | None = None,
    open_drop_alerts: dict[str, float] | None = None,
    pre_close_change_pct_at: dict[str, str] | None = None,
) -> None:
    alias_map = alias_map or {}
    memo_map = memo_map or {}
    drops = open_drop_alerts if open_drop_alerts is not None else {}
    pre_close_at = pre_close_change_pct_at if pre_close_change_pct_at is not None else {}
    all_symbols = list(dict.fromkeys(symbols + [t.symbol for t in targets]))
    if not all_symbols:
        logger.error("未指定任何监控标的")
        return

    logger.info("盯盘标的: %s", ", ".join(alias_map.get(s, s) for s in all_symbols))
    if targets:
        for t in targets:
            label = alias_map.get(t.symbol, t.symbol)
            extra = f"  ({t.memo})" if t.memo else ""
            if t.rate is not None:
                logger.info("  涨跌幅: %s %s 阈值 %.2f%%%s", label, t.direction, t.rate, extra)
            else:
                logger.info("  目标价: %s %s @ %.3f%s", label, t.direction, t.target_price, extra)
            for note in t.notes or []:
                logger.info("          └ %s", note)
    logger.info("轮询间隔: %ds | 飞书通知: %s", interval, "已启用" if webhook_url else "未配置")

    states: dict[str, SymbolState] = {}
    for s in all_symbols:
        states[s] = SymbolState(symbol=s, alias=alias_map.get(s, ""), memo=memo_map.get(s, ""))

    if webhook_url:
        notify_lines: list[str] = []
        for s in all_symbols:
            label = alias_map.get(s, s)
            sym_targets = [t for t in targets if t.symbol == s]
            if sym_targets:
                for t in sym_targets:
                    if t.rate is not None:
                        line = f"{label}  {t.direction} 涨跌幅阈值 {t.rate:.2f}%"
                    else:
                        line = f"{label}  {t.direction} @ {t.target_price:.3f}"
                    if t.memo:
                        line += f"  ({t.memo})"
                    notify_lines.append(line)
            else:
                note = memo_map.get(s, "仅盯盘")
                notify_lines.append(f"{label}  {note}")
        send_feishu_post(
            webhook_url,
            f"盯盘启动 ({len(all_symbols)}只)",
            notify_lines + [f"轮询间隔: {interval}s"],
            secret=get_secret(),
        )

    last_phase = ""
    poll_round = 0

    while True:
        try:
            phase = get_market_phase()

            if phase != last_phase:
                logger.info("市场阶段: %s", PHASE_CN.get(phase, phase))
                last_phase = phase

            if phase == "closed":
                logger.info("已收盘，监控结束")
                _print_summary(states, targets, webhook_url, alias_map)
                break

            if phase in ("pre", "lunch"):
                time.sleep(10)
                continue

            if phase not in ACTIVE_PHASES:
                time.sleep(interval)
                continue

            poll_round += 1
            quotes = fetch_realtime_quotes(all_symbols)
            if not quotes:
                time.sleep(interval)
                continue

            now_str = datetime.now().strftime("%H:%M:%S")
            now_hm = datetime.now().strftime("%H:%M")
            triggered_alerts: list[str] = []

            for sym in all_symbols:
                q = quotes.get(sym)
                if not q:
                    continue
                st = states[sym]
                st.name = q["name"]
                if not st.alias:
                    st.alias = q["name"]
                st.prev_close = q["prev_close"]
                st.last_price = q["price"]
                st.high = q["high"]
                st.low = q["low"]
                st.change_pct = q["change_pct"]
                label = st.alias

                if phase == "auction":
                    if q["price"] > 0 and q["price"] != st.auction_price:
                        st.auction_price = q["price"]
                        logger.info(
                            "[竞价] %s(%s) 竞价价=%.3f  %s",
                            label,
                            sym,
                            st.auction_price,
                            fmt_change(st.auction_price, st.prev_close),
                        )

                if q["open"] > 0 and st.open_price == 0:
                    st.open_price = q["open"]
                    # 仅配置了 open_drop_alert_pct 的标的推送「开盘价」提醒（飞书/高亮日志）
                    if sym in drops and not st.open_notified:
                        st.open_notified = True
                        open_msg = (
                            f"【开盘价】{label}({sym}) "
                            f"开盘价={st.open_price:.3f}  "
                            f"{fmt_change(st.open_price, st.prev_close)}"
                        )
                        logger.info(open_msg)
                        print(f"\n{'━' * 60}")
                        print(f"  {open_msg}")
                        print(f"{'━' * 60}")
                        if webhook_url:
                            send_feishu_post(
                                webhook_url,
                                f"开盘价 {label}({sym})",
                                [
                                    f"开盘价: {st.open_price:.3f}",
                                    f"昨收: {st.prev_close:.3f}",
                                    f"涨跌: {fmt_change(st.open_price, st.prev_close)}",
                                ],
                                secret=get_secret(),
                            )

                        drop_threshold = drops.get(sym)
                        if drop_threshold is not None and st.prev_close > 0:
                            open_chg_pct = (st.open_price - st.prev_close) / st.prev_close * 100
                            if open_chg_pct <= drop_threshold:
                                drop_msg = (
                                    f"【大跌增持提醒】{label}({sym}) "
                                    f"开盘跌 {open_chg_pct:.2f}%，达到增持阈值({drop_threshold}%)"
                                )
                                logger.info(drop_msg)
                                memo = memo_map.get(sym, "")
                                if webhook_url:
                                    lines_drop = [
                                        f"开盘价: {st.open_price:.3f}",
                                        f"昨收: {st.prev_close:.3f}",
                                        f"跌幅: {open_chg_pct:.2f}%（阈值 {drop_threshold}%）",
                                    ]
                                    if memo:
                                        lines_drop.append(f"操作: {memo}")
                                    send_feishu_post(
                                        webhook_url,
                                        f"大跌增持提醒 {label}({sym})",
                                        lines_drop,
                                        secret=get_secret(),
                                    )

                for t in targets:
                    if t.symbol != sym:
                        continue
                    alert = check_target(t, st)
                    if alert:
                        triggered_alerts.append(alert)

            for sym_pc, at_cfg in pre_close_at.items():
                st_pc = states.get(sym_pc)
                if not st_pc or st_pc.pre_close_chg_notified or st_pc.last_price <= 0:
                    continue
                if not _pre_close_change_should_fire(phase, now_hm, at_cfg):
                    continue
                st_pc.pre_close_chg_notified = True
                label_pc = st_pc.alias or st_pc.name or sym_pc
                chg = st_pc.change_pct
                pc_msg = (
                    f"【收盘前涨跌】{label_pc}({sym_pc}) "
                    f"现价 {st_pc.last_price:.3f}，较昨收 {chg:+.2f}%。"
                    f"\n  昨收={st_pc.prev_close:.3f}  "
                    f"{fmt_change(st_pc.last_price, st_pc.prev_close)}"
                )
                if at_cfg >= "15:00":
                    pc_msg += f"\n  （配置时点 {at_cfg}，已于尾盘竞价段推送）"
                logger.info(pc_msg.split("\n")[0])
                print(f"\n{'━' * 60}")
                print(f"  {pc_msg}")
                print(f"{'━' * 60}")
                if webhook_url:
                    send_feishu_post(
                        webhook_url,
                        f"收盘前涨跌 {label_pc}({sym_pc})",
                        [
                            f"现价: {st_pc.last_price:.3f}",
                            f"较昨收: {chg:+.2f}%",
                            f"昨收: {st_pc.prev_close:.3f}",
                            fmt_change(st_pc.last_price, st_pc.prev_close),
                        ],
                        secret=get_secret(),
                    )

            if poll_round % max(1, 30 // interval) == 0 or phase == "auction":
                print(f"\n[{now_str}] {PHASE_CN.get(phase, phase)}")
                for sym in all_symbols:
                    st = states[sym]
                    if st.last_price > 0:
                        print(fmt_price_line(st))

            for alert in triggered_alerts:
                print(f"\n{'!' * 60}")
                print(alert)
                print(f"{'!' * 60}")
                logger.info(alert.split("\n")[0])
                if webhook_url:
                    send_feishu_post(
                        webhook_url,
                        "目标价触发",
                        alert.split("\n"),
                        secret=get_secret(),
                    )

            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("用户中断")
            _print_summary(states, targets, webhook_url, alias_map)
            break
        except Exception as e:
            logger.error("轮询异常: %s", e)
            time.sleep(interval)


def _print_summary(
    states: dict[str, SymbolState],
    targets: list[PriceTarget],
    webhook_url: str,
    alias_map: dict[str, str] | None = None,
) -> None:
    alias_map = alias_map or {}
    lines: list[str] = []
    print(f"\n{'═' * 60}")
    print("  盯盘小结")
    print(f"{'═' * 60}")
    for sym, st in states.items():
        if st.last_price <= 0:
            continue
        label = st.alias or st.name
        line = (
            f"{label}({sym})  "
            f"开={st.open_price:.3f}  现={st.last_price:.3f}  "
            f"高={st.high:.3f}  低={st.low:.3f}  "
            f"{fmt_change(st.last_price, st.prev_close)}"
        )
        print(f"  {line}")
        lines.append(line)
    if targets:
        print()
        for t in targets:
            label = alias_map.get(t.symbol, t.symbol)
            status = "✓ 已触发" if t.triggered else "✗ 未触发"
            if t.rate is not None:
                tl = f"  {label} {t.direction} 涨跌幅阈值 {t.rate:.2f}%  {status}"
            else:
                tl = f"  {label} {t.direction} @ {t.target_price:.3f}  {status}"
            if t.memo:
                tl += f"  ({t.memo})"
            print(tl)
            lines.append(tl)
            if t.notes and t.triggered:
                for note in t.notes:
                    nl = f"    · {note}"
                    print(nl)
                    lines.append(nl)
    print(f"{'═' * 60}")

    if webhook_url and lines:
        send_feishu_post(webhook_url, "盯盘小结", lines, secret=get_secret())
