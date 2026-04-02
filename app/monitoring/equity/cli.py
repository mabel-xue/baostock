"""场内盯盘 CLI 入口。"""

from __future__ import annotations

import argparse
import logging
import sys

from ..infrastructure.notifications import get_webhook
from .config import POLL_INTERVAL
from .service import load_watchlist, monitor, parse_targets_cli

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="盯盘 —— 股票/场内基金实时价格监控（配置见 monitoring.equity.config）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  cd app && python -m monitoring.equity.cli
  cd app && python -m monitoring.equity.cli --targets "sh510300:3.50:买入"
        """,
    )
    parser.add_argument("--symbols", type=str, default="", help="标的代码逗号分隔")
    parser.add_argument("--targets", type=str, default="", help='目标价 "sym:价:买入/卖出,..."')
    parser.add_argument("--interval", type=int, default=0, help=f"轮询秒数，默认 {POLL_INTERVAL}")
    parser.add_argument("--webhook", type=str, default="", help="飞书 Webhook，默认读环境变量")
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
        print("WATCHLIST 为空且未传入命令行参数，请编辑 monitoring/equity/config.py")
        parser.print_help()
        sys.exit(1)

    interval = args.interval if args.interval > 0 else POLL_INTERVAL
    webhook = (args.webhook or get_webhook() or "").strip()
    monitor(symbols, targets, interval, webhook, alias_map=alias_map, memo_map=memo_map)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
