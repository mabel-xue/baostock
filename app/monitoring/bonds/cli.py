"""可转债监控 CLI。"""

from __future__ import annotations

import argparse

from .daily_query import notify_daily_snapshot_feishu, poll_new_convertible_bonds, run_daily_snapshot
from .service import run_cb_forever


def main() -> None:
    parser = argparse.ArgumentParser(
        description="可转债监控 → 飞书；可选收盘前落盘 query_convertible_bonds 结果并监听新债代码"
    )
    parser.add_argument("--interval", type=int, default=1800, help="轮询间隔秒，默认 1800")
    parser.add_argument("--once", action="store_true", help="只跑一轮")
    parser.add_argument("--dry-run", action="store_true", help="不发飞书（新债仍落盘状态以免重复提醒）")
    parser.add_argument(
        "--no-daily-snapshot",
        action="store_true",
        help="禁用尾盘竞价时段的 query 日终快照（默认启用）",
    )
    parser.add_argument(
        "--no-new-bond-poll",
        action="store_true",
        help="禁用每轮转债代码增量检测（默认启用）",
    )
    parser.add_argument(
        "--no-kept-new-notify",
        action="store_true",
        help="禁用日终「保留池新进」飞书通知（仍会更新 last_kept_snapshot_codes）",
    )
    parser.add_argument(
        "--snapshot-now",
        action="store_true",
        help="立即跑一轮与 query_convertible_bonds 相同的逻辑并落盘（需配合 --force 在非尾盘窗口执行）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="忽略尾盘时间窗口与「今日已快照」限制",
    )
    parser.add_argument(
        "--notify-snapshot-feishu",
        action="store_true",
        help="与 --snapshot-now 联用：快照完成后发一条飞书 post（默认快照不发飞书，仅新债发）",
    )
    args = parser.parse_args()

    if args.snapshot_now:
        from . import config as cfg

        if args.no_kept_new_notify:
            cfg.CB_KEPT_NEW_NOTIFY_ENABLED = False
        out = run_daily_snapshot(
            force=args.force,
            persist=not args.dry_run,
            dry_run=args.dry_run,
        )
        if out and args.notify_snapshot_feishu:
            notify_daily_snapshot_feishu(out, dry_run=args.dry_run)
        return

    from . import config as cfg

    if args.no_daily_snapshot:
        cfg.CB_DAILY_SNAPSHOT_ENABLED = False
    if args.no_new_bond_poll:
        cfg.CB_NEW_BOND_POLL_ENABLED = False
    if args.no_kept_new_notify:
        cfg.CB_KEPT_NEW_NOTIFY_ENABLED = False

    run_cb_forever(interval=args.interval, once=args.once, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
