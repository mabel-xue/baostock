"""
盯盘脚本 —— 股票/场内基金实时价格监控

功能：
1. 实时监控股票或场内基金（ETF）价格，触及目标价时飞书通知并给出操作提醒；
2. 支持集合竞价阶段（9:15-9:25）跟踪竞价价格，开盘价一旦确定立即推送飞书通知；
3. 可同时监控多只标的，每只可设不同目标价（买入/卖出方向）；
4. 自动识别交易时段，盘前/午休静默等待，收盘后自动退出。

数据源：腾讯财经实时行情接口 qt.gtimg.cn，无需鉴权，秒级延迟。

配置：直接编辑下方 WATCHLIST 即可，无需命令行参数。

用法:
    # 按 WATCHLIST 配置运行（推荐）
    python app/monitor_price.py

    # 命令行覆盖（临时使用）
    python app/monitor_price.py --targets "sh510300:3.50:买入"
    python app/monitor_price.py --symbols sh510300,sz159941
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime

import requests as http_requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

FEISHU_WEBHOOK_URL = os.environ.get(
    "FEISHU_WEBHOOK_URL",
    "https://open.feishu.cn/open-apis/bot/v2/hook/44073acd-feb1-4da9-828d-d3d3a77e9a53",
)

QUOTE_URL = "https://qt.gtimg.cn/q="


# ══════════════════════════════════════════════════════════════════════════════
# 盯盘配置表 WATCHLIST
#
# 每条元组: (代码, 简称, 目标价列表, 备注)
#   代码:     腾讯格式，如 sh510300 / sz159905
#   简称:     自定义名称，便于通知阅读
#   目标价:   [(价格, "买入"/"卖出"), ...]  跌到买入价触发买入提醒，涨到卖出价触发卖出提醒
#             空列表 [] 表示仅盯盘（集合竞价 + 开盘价推送），不设目标价提醒
#   备注:     操作备忘，会随通知一起推送（如仓位计划、策略说明）
# ══════════════════════════════════════════════════════════════════════════════

WATCHLIST: list[tuple[str, str, list[tuple[float, str]], str]] = [
    # ── 低位关注 ──
    ("sh600377", "宁沪高速",    [(11.50, "买入")],                  "跌到11.5加仓，看股息率逢低分批"),
    ("sz159632", "纳斯达克ETF", [(1.600, "买入")],                  "1.6分批建仓，等暴跌机会"),

    # ── 减仓/止盈 ──
    ("sz002100", "天康生物",    [(7.64, "卖出")],    ">=7.64减仓1000股，大涨减1000股"),

    # ── 仅盯盘（集合竞价+开盘价推送，不设目标价） ──
    # ("sh510300", "沪深300ETF",  [],                                 "大盘风向标"),
    # ("sh510900", "H股ETF",     [],                                 "恒生国企"),
]

# ══════════════════════════════════════════════════════════════════════════════
# 投资逻辑备忘 INVESTMENT_NOTES
#
# 键为标的代码（与 WATCHLIST 对齐），值为逻辑说明列表。
# 触发目标价提醒时，相关逻辑会附在飞书通知中，帮助回忆决策依据。
# 也可记录「不再关注」的原因，便于日后复盘。
# ══════════════════════════════════════════════════════════════════════════════

INVESTMENT_NOTES: dict[str, list[str]] = {
    "sz159905": [
        "深证红利指数，高股息策略，历史 PE<15 为低估区间",
        "长期定投品种，红利再投资复利效应",
    ],
    "sh512690": [
        "白酒板块周期性强，消费复苏逻辑",
        "PE 20-25 为合理区间，低于 20 可加仓",
    ],
    "sh516670": [
        "猪肉不再关注（徐老师329课程）",
    ],
    "sz002100": [
        "天康生物，畜牧养殖个股",
        ">=7.64 减仓1000股，大涨>=7.81 再减1000股",
        "猪周期下行阶段逐步减仓（徐老师329课程）",
    ],
    "sh562500": [
        "机器人/AI 产业趋势，中长期看好",
        "波段操作为主，涨幅超 30% 可分批止盈",
    ],
    "sz159611": [
        "电力公用事业，防御性配置",
        "电改+新能源消纳，长期逻辑不变",
    ],
    "sh510300": [
        "沪深300，大盘风向标",
    ],
    "sh510900": [
        "恒生国企，港股估值洼地",
        "关注中美关系、港股流动性变化",
    ],
    "sh600377": [
        "宁沪高速，高速公路龙头，高股息防御品种",
        "看股息率逢低分批加仓，跌到11.5目标价加仓",
    ],
    "sz159632": [
        "纳斯达克ETF华安，跟踪纳斯达克100指数",
        "纳斯达克一定会跌，可能经历暴跌（徐329）",
        "1.6分批建仓，耐心等待暴跌机会",
    ],
    # ── 个股/场外基金（不在 WATCHLIST 盯盘，仅记录投资逻辑备忘） ──
    "000001": [
        "平安银行",
        "反转可能还需2-3年（徐329）",
    ],
    "022930": [
        "易方达中证A500ETF联接Y",
        "中国A500徐老师不投（中国没有500加优质企业）",
    ],
}

# 轮询间隔（秒），盘中每隔此秒数拉取一次行情
POLL_INTERVAL = 5


# ── 数据结构 ──


@dataclass
class PriceTarget:
    symbol: str
    target_price: float
    direction: str  # "买入" or "卖出"
    memo: str = ""
    notes: list[str] = None  # 来自 INVESTMENT_NOTES 的投资逻辑
    triggered: bool = False

    def __post_init__(self):
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


# ── 行情获取 ──


def fetch_realtime_quotes(symbols: list[str]) -> dict[str, dict]:
    """
    批量获取实时行情，返回 {symbol: {字段...}}。
    腾讯接口支持逗号分隔的多代码查询。
    """
    if not symbols:
        return {}
    query = ",".join(symbols)
    try:
        r = http_requests.get(f"{QUOTE_URL}{query}", timeout=5)
        r.encoding = "gbk"
        text = r.text
    except Exception as e:
        logger.warning(f"行情请求失败: {e}")
        return {}

    results = {}
    for line in text.strip().split("\n"):
        line = line.strip()
        if "~" not in line:
            continue
        parts = line.split("~")
        if len(parts) < 49:
            continue
        sym_raw = line.split("=")[0].split("_")[-1] if "=" in line else ""
        if not sym_raw:
            continue
        try:
            current = float(parts[3]) if parts[3] else 0.0
            prev_close = float(parts[4]) if parts[4] else 0.0
            today_open = float(parts[5]) if parts[5] else 0.0
            high = float(parts[33]) if parts[33] else 0.0
            low = float(parts[34]) if parts[34] else 0.0
            change_pct = float(parts[32]) if parts[32] else 0.0
            limit_up = float(parts[47]) if parts[47] else 0.0
            limit_down = float(parts[48]) if parts[48] else 0.0
        except (ValueError, IndexError):
            continue
        results[sym_raw] = {
            "name": parts[1],
            "code": parts[2],
            "price": current,
            "prev_close": prev_close,
            "open": today_open,
            "high": high,
            "low": low,
            "change_pct": change_pct,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "time": parts[30] if len(parts) > 30 else "",
        }
    return results


# ── 飞书通知 ──


def send_feishu(webhook_url: str, title: str, lines: list[str]):
    if not webhook_url:
        return
    content_elements = [[{"tag": "text", "text": line}] for line in lines]
    msg = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": content_elements,
                }
            }
        },
    }
    try:
        resp = http_requests.post(
            webhook_url,
            data=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=5,
        )
        if resp.status_code != 200:
            logger.warning(f"飞书通知失败: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.warning(f"飞书通知异常: {e}")


# ── 时段判断 ──


def get_market_phase() -> str:
    """
    返回当前市场阶段：
    - "pre"        盘前（< 09:15）
    - "auction"    集合竞价（09:15 ~ 09:25）
    - "pre_open"   竞价后待开盘（09:25 ~ 09:30）
    - "morning"    上午连续竞价（09:30 ~ 11:30）
    - "lunch"      午休（11:30 ~ 13:00）
    - "afternoon"  下午连续竞价（13:00 ~ 14:57）
    - "close_auction"  尾盘集合竞价（14:57 ~ 15:00）
    - "closed"     收盘（>= 15:00）
    """
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

ACTIVE_PHASES = {"auction", "pre_open", "morning", "afternoon", "close_auction"}


# ── 格式化工具 ──


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


# ── 核心监控 ──


def load_watchlist() -> tuple[list[str], list[PriceTarget], dict[str, str], dict[str, str]]:
    """从 WATCHLIST + INVESTMENT_NOTES 配置表加载监控标的和目标价。"""
    symbols: list[str] = []
    targets: list[PriceTarget] = []
    alias_map: dict[str, str] = {}
    memo_map: dict[str, str] = {}
    for sym, alias, price_targets, memo in WATCHLIST:
        symbols.append(sym)
        alias_map[sym] = alias
        if memo:
            memo_map[sym] = memo
        notes = INVESTMENT_NOTES.get(sym, [])
        for price, direction in price_targets:
            targets.append(PriceTarget(
                symbol=sym,
                target_price=price,
                direction=direction,
                memo=memo,
                notes=notes,
            ))
    return symbols, targets, alias_map, memo_map


def parse_targets_cli(targets_str: str) -> list[PriceTarget]:
    """解析命令行目标价字符串，格式: symbol:price:方向,symbol:price:方向,..."""
    results = []
    for item in targets_str.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) < 3:
            logger.warning(f"目标价格式错误（需 symbol:price:方向）: {item}")
            continue
        sym = parts[0].strip()
        try:
            price = float(parts[1].strip())
        except ValueError:
            logger.warning(f"价格解析失败: {item}")
            continue
        direction = parts[2].strip()
        if direction not in ("买入", "卖出"):
            logger.warning(f"方向需为 '买入' 或 '卖出': {item}")
            continue
        results.append(PriceTarget(symbol=sym, target_price=price, direction=direction))
    return results


def check_target(target: PriceTarget, state: SymbolState) -> str | None:
    """检查是否触及目标价，返回提示文本或 None。"""
    if target.triggered:
        return None
    price = state.last_price
    if price <= 0:
        return None
    hit = False
    if target.direction == "买入" and price <= target.target_price:
        hit = True
    elif target.direction == "卖出" and price >= target.target_price:
        hit = True
    if not hit:
        return None
    target.triggered = True
    label = state.alias or state.name
    action = "买入" if target.direction == "买入" else "卖出"
    verb = "跌至" if action == "买入" else "涨至"
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
):
    alias_map = alias_map or {}
    memo_map = memo_map or {}
    all_symbols = list(dict.fromkeys(symbols + [t.symbol for t in targets]))
    if not all_symbols:
        logger.error("未指定任何监控标的")
        return

    logger.info(f"盯盘标的: {', '.join(alias_map.get(s, s) for s in all_symbols)}")
    if targets:
        for t in targets:
            label = alias_map.get(t.symbol, t.symbol)
            extra = f"  ({t.memo})" if t.memo else ""
            logger.info(f"  目标价: {label} {t.direction} @ {t.target_price:.3f}{extra}")
            for note in t.notes:
                logger.info(f"          └ {note}")
    logger.info(f"轮询间隔: {interval}s | 飞书通知: {'已启用' if webhook_url else '未配置'}")

    states: dict[str, SymbolState] = {}
    for s in all_symbols:
        st = SymbolState(symbol=s, alias=alias_map.get(s, ""), memo=memo_map.get(s, ""))
        states[s] = st

    if webhook_url:
        notify_lines = []
        for s in all_symbols:
            label = alias_map.get(s, s)
            sym_targets = [t for t in targets if t.symbol == s]
            if sym_targets:
                for t in sym_targets:
                    line = f"{label}  {t.direction} @ {t.target_price:.3f}"
                    if t.memo:
                        line += f"  ({t.memo})"
                    notify_lines.append(line)
            else:
                note = memo_map.get(s, "仅盯盘")
                notify_lines.append(f"{label}  {note}")
        send_feishu(
            webhook_url,
            f"盯盘启动 ({len(all_symbols)}只)",
            notify_lines + [f"轮询间隔: {interval}s"],
        )

    last_phase = ""
    poll_round = 0

    while True:
        try:
            phase = get_market_phase()

            if phase != last_phase:
                logger.info(f"市场阶段: {PHASE_CN.get(phase, phase)}")
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

                # 集合竞价阶段：跟踪竞价价格
                if phase == "auction":
                    if q["price"] > 0 and q["price"] != st.auction_price:
                        st.auction_price = q["price"]
                        logger.info(
                            f"[竞价] {label}({sym}) "
                            f"竞价价={st.auction_price:.3f}  "
                            f"{fmt_change(st.auction_price, st.prev_close)}"
                        )

                # 开盘价确认推送
                if q["open"] > 0 and st.open_price == 0:
                    st.open_price = q["open"]
                    if not st.open_notified:
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
                            send_feishu(
                                webhook_url,
                                f"开盘价 {label}({sym})",
                                [
                                    f"开盘价: {st.open_price:.3f}",
                                    f"昨收: {st.prev_close:.3f}",
                                    f"涨跌: {fmt_change(st.open_price, st.prev_close)}",
                                ],
                            )

                # 目标价检查
                for t in targets:
                    if t.symbol != sym:
                        continue
                    alert = check_target(t, st)
                    if alert:
                        triggered_alerts.append(alert)

            # 定期打印行情
            if poll_round % max(1, 30 // interval) == 0 or phase == "auction":
                print(f"\n[{now_str}] {PHASE_CN.get(phase, phase)}")
                for sym in all_symbols:
                    st = states[sym]
                    if st.last_price > 0:
                        print(fmt_price_line(st))

            # 处理触发的目标价提醒
            for alert in triggered_alerts:
                print(f"\n{'!' * 60}")
                print(alert)
                print(f"{'!' * 60}")
                logger.info(alert.split("\n")[0])
                if webhook_url:
                    send_feishu(webhook_url, "目标价触发", alert.split("\n"))

            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("用户中断")
            _print_summary(states, targets, webhook_url, alias_map)
            break
        except Exception as e:
            logger.error(f"轮询异常: {e}")
            time.sleep(interval)


def _print_summary(
    states: dict[str, SymbolState],
    targets: list[PriceTarget],
    webhook_url: str,
    alias_map: dict[str, str] | None = None,
):
    """打印并推送盯盘小结"""
    alias_map = alias_map or {}
    lines = []
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
        send_feishu(webhook_url, "盯盘小结", lines)


def main():
    parser = argparse.ArgumentParser(
        description="盯盘脚本 —— 股票/场内基金实时价格监控（默认读取脚本内 WATCHLIST 配置）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 按 WATCHLIST 配置运行（推荐，直接编辑脚本顶部配置表）
  python app/monitor_price.py

  # 命令行临时覆盖
  python app/monitor_price.py --targets "sh510300:3.50:买入,sh510300:4.20:卖出"
  python app/monitor_price.py --symbols sh510300,sz159941
        """,
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="命令行覆盖：监控标的代码，逗号分隔（如 sh510300,sz159941）",
    )
    parser.add_argument(
        "--targets",
        type=str,
        default="",
        help='命令行覆盖：目标价列表，格式 "symbol:价格:买入/卖出,..."',
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help=f"轮询间隔（秒），默认使用配置 POLL_INTERVAL={POLL_INTERVAL}",
    )
    parser.add_argument(
        "--webhook",
        type=str,
        default="",
        help="飞书 webhook URL（不填则使用环境变量或默认值）",
    )
    args = parser.parse_args()

    cli_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    cli_targets = parse_targets_cli(args.targets) if args.targets else []

    if cli_symbols or cli_targets:
        symbols = cli_symbols
        targets = cli_targets
        alias_map: dict[str, str] = {}
        memo_map: dict[str, str] = {}
    else:
        symbols, targets, alias_map, memo_map = load_watchlist()

    if not symbols and not targets:
        print("WATCHLIST 为空且未传入命令行参数，请编辑脚本顶部 WATCHLIST 配置表")
        parser.print_help()
        sys.exit(1)

    interval = args.interval if args.interval > 0 else POLL_INTERVAL
    webhook = args.webhook or FEISHU_WEBHOOK_URL
    monitor(symbols, targets, interval, webhook, alias_map=alias_map, memo_map=memo_map)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
