"""
多日程飞书提醒：按 JSON 配置在指定时点发送飞书 webhook。

- 每日重复：at 为 \"HH:MM\"（24 小时制），每个自然日触发一次。
- 单次：at 为 \"YYYY-MM-DD HH:MM\"，到点后触发一次（进程内需保持运行）。

用法:
    python app/schedule_reminder.py --config config/schedule_reminders.json
    python app/schedule_reminder.py --config config/schedule_reminders.json --webhook \"https://...\"
    python app/schedule_reminder.py --config config/schedule_reminders.json --interval 10

环境变量 FEISHU_WEBHOOK_URL 或与 monitor_big_orders 相同的默认 webhook 可用；--webhook 优先。
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from typing import Literal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.monitoring.infrastructure.notifications import get_secret, get_webhook, send_feishu_post

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class ScheduleItem:
    id: str
    kind: Literal["daily", "once"]
    title: str
    lines: list[str]
    # daily: time; once: full datetime
    daily_time: dt_time | None = None
    once_at: datetime | None = None


def _parse_at(at: str) -> tuple[Literal["daily", "once"], dt_time | datetime]:
    at = at.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return "once", datetime.strptime(at, fmt)
        except ValueError:
            continue
    try:
        parts = at.split(":")
        h, m = int(parts[0]), int(parts[1])
        return "daily", dt_time(hour=h, minute=m, second=0 if len(parts) < 3 else int(parts[2]))
    except (ValueError, IndexError) as e:
        raise ValueError(f"无法解析 at={at!r}，请使用 HH:MM 或 YYYY-MM-DD HH:MM") from e


def load_schedules(path: str) -> list[ScheduleItem]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    raw = data.get("schedules", data) if isinstance(data, dict) else data
    if not isinstance(raw, list):
        raise ValueError("配置须为 {\"schedules\": [...]} 或顶层数组")
    out: list[ScheduleItem] = []
    for i, row in enumerate(raw):
        if not isinstance(row, dict):
            raise ValueError(f"schedules[{i}] 须为对象")
        at = row.get("at")
        title = row.get("title", "日程提醒")
        lines = row.get("lines") or [row.get("message", "")]
        lines = [str(x) for x in lines if str(x).strip()]
        if not lines:
            lines = ["（无正文）"]
        sid = str(row.get("id", f"schedule_{i}"))
        kind, parsed = _parse_at(str(at))
        if kind == "daily":
            assert isinstance(parsed, dt_time)
            out.append(
                ScheduleItem(
                    id=sid,
                    kind="daily",
                    title=title,
                    lines=lines,
                    daily_time=parsed,
                    once_at=None,
                )
            )
        else:
            assert isinstance(parsed, datetime)
            out.append(
                ScheduleItem(
                    id=sid,
                    kind="once",
                    title=title,
                    lines=lines,
                    daily_time=None,
                    once_at=parsed,
                )
            )
    return out


def run_loop(config_path: str, webhook_url: str, interval_sec: int) -> None:
    items = load_schedules(config_path)
    if not items:
        logger.warning("配置中没有任何日程，退出")
        return
    daily_fired_on: dict[str, date] = {}
    once_fired: set[str] = set()

    logger.info("已加载 %d 条日程，轮询间隔 %ds", len(items), interval_sec)
    if webhook_url:
        logger.info("飞书通知已启用")
    else:
        logger.info("未配置 webhook，将只打日志不推送")

    while True:
        now = datetime.now()
        today = now.date()

        for s in items:
            if s.kind == "daily" and s.daily_time is not None:
                if daily_fired_on.get(s.id) == today:
                    continue
                target = datetime.combine(today, s.daily_time)
                if now >= target:
                    logger.info("触发每日日程 %s (%s)", s.id, s.daily_time.strftime("%H:%M"))
                    if webhook_url:
                        send_feishu_post(webhook_url, s.title, s.lines, secret=get_secret())
                    daily_fired_on[s.id] = today

            elif s.kind == "once" and s.once_at is not None:
                if s.id in once_fired:
                    continue
                if now >= s.once_at:
                    logger.info("触发单次日程 %s (%s)", s.id, s.once_at.isoformat(sep=" "))
                    if webhook_url:
                        send_feishu_post(webhook_url, s.title, s.lines, secret=get_secret())
                    once_fired.add(s.id)

        if all_once_done(items, once_fired) and not any_daily(items):
            logger.info("所有单次日程已发送且无每日日程，退出")
            break

        time.sleep(interval_sec)


def any_daily(items: list[ScheduleItem]) -> bool:
    return any(s.kind == "daily" for s in items)


def all_once_done(items: list[ScheduleItem], once_fired: set[str]) -> bool:
    once_ids = [s.id for s in items if s.kind == "once"]
    return bool(once_ids) and all(i in once_fired for i in once_ids)


def main() -> None:
    parser = argparse.ArgumentParser(description="多日程飞书提醒")
    parser.add_argument(
        "--config",
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "config",
            "schedule_reminders.json",
        ),
        help="JSON 配置文件路径",
    )
    parser.add_argument("--webhook", type=str, default="", help="飞书 webhook URL，空则用环境变量或默认")
    parser.add_argument("--interval", type=int, default=15, help="检查间隔（秒）")
    args = parser.parse_args()

    if not os.path.isfile(args.config):
        logger.error("配置文件不存在: %s", args.config)
        sys.exit(1)

    webhook = (args.webhook or get_webhook() or "").strip()

    try:
        run_loop(args.config, webhook, max(5, args.interval))
    except KeyboardInterrupt:
        print("\n用户中断")


if __name__ == "__main__":
    main()
