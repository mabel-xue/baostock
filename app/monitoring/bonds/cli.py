"""可转债监控 CLI。"""

from __future__ import annotations

import argparse

from .service import run_cb_forever


def main() -> None:
    parser = argparse.ArgumentParser(description="可转债监控 → 飞书")
    parser.add_argument("--interval", type=int, default=1800, help="轮询间隔秒，默认 1800")
    parser.add_argument("--once", action="store_true", help="只跑一轮")
    parser.add_argument("--dry-run", action="store_true", help="不发飞书")
    args = parser.parse_args()
    run_cb_forever(interval=args.interval, once=args.once, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
