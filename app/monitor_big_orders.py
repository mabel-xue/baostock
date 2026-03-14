"""
盘中大单实时监控

通过定时轮询 akshare 的 stock_zh_a_tick_tx_js 接口，增量检测新出现的大单，
终端实时打印并通过飞书 webhook 推送通知。

用法:
    python app/monitor_big_orders.py --symbol sh601166 --threshold 1000
    python app/monitor_big_orders.py --symbol sh601166 --threshold 1000 --interval 15
    python app/monitor_big_orders.py --symbol sh601166 --threshold 1000 --webhook "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import time
import json
import logging
from datetime import datetime

import pandas as pd
import requests

from app.query_tick_analysis import fetch_tick_data

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

TRADING_PERIODS = [
    ("09:15", "11:35"),
    ("12:55", "15:05"),
]


def is_trading_time() -> bool:
    now = datetime.now().strftime("%H:%M")
    return any(start <= now <= end for start, end in TRADING_PERIODS)


def send_feishu(webhook_url: str, title: str, lines: list[str]):
    """通过飞书 webhook 发送富文本消息，多笔大单合并为一条"""
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
        resp = requests.post(
            webhook_url,
            data=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=5,
        )
        if resp.status_code != 200:
            logger.warning(f"飞书通知发送失败: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.warning(f"飞书通知异常: {e}")


def append_csv(filepath: str, df: pd.DataFrame):
    """追加写入 CSV（首次写入表头）"""
    write_header = not os.path.exists(filepath)
    df.to_csv(filepath, mode="a", index=False, header=write_header, encoding="utf-8-sig")


def format_big_order(row) -> str:
    kind_icon = {"买盘": "+", "卖盘": "-", "中性盘": "="}
    icon = kind_icon.get(row["性质"], "?")
    return (
        f"  [{icon}] {row['成交时间']}  "
        f"价格={row['成交价格']:.2f}  "
        f"量={int(row['成交量'])}手  "
        f"额={row['成交金额'] / 10000:.1f}万  "
        f"{row['性质']}"
    )


def monitor(symbol: str, threshold: int, interval: int, webhook_url: str):
    logger.info(f"开始监控 {symbol}，大单阈值 >= {threshold} 手，轮询间隔 {interval}s")
    if webhook_url:
        logger.info(f"飞书通知已启用")
    else:
        logger.info("飞书通知未配置，仅终端输出")

    os.makedirs("output", exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    csv_path = f"output/{symbol}_monitor_{threshold}_{today}.csv"

    last_count = 0
    total_big_buy = 0
    total_big_sell = 0
    poll_round = 0

    # 首次全量拉取
    logger.info("首次拉取全量数据...")
    try:
        df = fetch_tick_data(symbol)
        last_count = len(df)
        existing_big = df[df["成交量"] >= threshold]
        logger.info(
            f"当前共 {last_count} 条记录，其中已有 {len(existing_big)} 笔 >= {threshold} 手的大单"
        )
        if not existing_big.empty:
            append_csv(csv_path, existing_big)
            for _, row in existing_big.iterrows():
                if row["性质"] == "买盘":
                    total_big_buy += row["成交金额"]
                elif row["性质"] == "卖盘":
                    total_big_sell += row["成交金额"]
    except Exception as e:
        logger.error(f"首次拉取失败: {e}")
        last_count = 0

    # 轮询循环
    while True:
        try:
            time.sleep(interval)

            if not is_trading_time():
                now_str = datetime.now().strftime("%H:%M")
                # 收盘后退出
                if now_str > "15:05":
                    logger.info("已过收盘时间，监控结束")
                    break
                logger.debug(f"非交易时间 ({now_str})，等待中...")
                continue

            poll_round += 1
            df = fetch_tick_data(symbol)
            current_count = len(df)

            if current_count <= last_count:
                logger.debug(f"[轮询#{poll_round}] 无新数据 ({current_count}条)")
                last_count = current_count
                continue

            # 增量部分
            new_data = df.iloc[last_count:]
            new_big = new_data[new_data["成交量"] >= threshold].copy()
            last_count = current_count

            if new_big.empty:
                logger.debug(f"[轮询#{poll_round}] +{len(new_data)}条，无大单")
                continue

            # 发现大单
            new_big["成交金额_万"] = (new_big["成交金额"] / 10000).round(1)
            logger.info(f"[轮询#{poll_round}] 发现 {len(new_big)} 笔大单!")

            # 终端打印
            print(f"\n{'─' * 60}")
            print(f"  {datetime.now().strftime('%H:%M:%S')} 发现 {len(new_big)} 笔大单（>= {threshold}手）")
            print(f"{'─' * 60}")
            for _, row in new_big.iterrows():
                print(format_big_order(row))

            # 累计统计
            for _, row in new_big.iterrows():
                if row["性质"] == "买盘":
                    total_big_buy += row["成交金额"]
                elif row["性质"] == "卖盘":
                    total_big_sell += row["成交金额"]
            net = (total_big_buy - total_big_sell) / 10000
            print(f"  累计大单: 买入={total_big_buy / 10000:.0f}万 卖出={total_big_sell / 10000:.0f}万 净流入={net:.0f}万")

            # CSV 日志
            append_csv(csv_path, new_big)

            # 飞书通知
            if webhook_url:
                lines = [format_big_order(row) for _, row in new_big.iterrows()]
                lines.append(
                    f"\n累计大单净流入: {net:.0f}万"
                )
                send_feishu(
                    webhook_url,
                    f"{symbol} 发现{len(new_big)}笔大单(>={threshold}手)",
                    lines,
                )

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"轮询异常: {e}")
            time.sleep(interval)

    # 结束汇总
    print(f"\n{'=' * 60}")
    print(f"监控结束 — {symbol} 大单汇总（阈值 >= {threshold}手）")
    net = (total_big_buy - total_big_sell) / 10000
    print(f"  大单买入: {total_big_buy / 10000:.0f}万")
    print(f"  大单卖出: {total_big_sell / 10000:.0f}万")
    print(f"  净流入:   {net:.0f}万")
    if os.path.exists(csv_path):
        print(f"  日志文件: {csv_path}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="盘中大单实时监控")
    parser.add_argument("--symbol", type=str, default="sh601166", help="股票代码，如 sh601166")
    parser.add_argument("--threshold", type=int, default=1000, help="大单阈值（手）")
    parser.add_argument("--interval", type=int, default=30, help="轮询间隔（秒），建议>=15")
    parser.add_argument("--webhook", type=str, default="", help="飞书 webhook URL")
    args = parser.parse_args()

    webhook = args.webhook or FEISHU_WEBHOOK_URL
    monitor(args.symbol, args.threshold, args.interval, webhook)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
