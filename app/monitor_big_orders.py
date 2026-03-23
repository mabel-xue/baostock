"""
盘中大单实时监控

直接请求腾讯财经逐笔成交分页接口，增量拉取新数据，秒级延迟。

用法:
    python app/monitor_big_orders.py --symbol sh601166 --threshold 1000
    python app/monitor_big_orders.py --symbol sh601166 --threshold 1000 --interval 3
    python app/monitor_big_orders.py --symbol sh601166 --threshold 1000 --webhook "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import time
import logging
from datetime import datetime

import pandas as pd

from app.feishu_notify import FEISHU_WEBHOOK_URL, send_feishu
import requests as http_requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

TRADING_PERIODS = [
    ("09:15", "11:35"),
    ("12:55", "15:05"),
]

TX_TICK_URL = "https://stock.gtimg.cn/data/index.php"
TX_PAGE_SIZE = 70  # 腾讯接口每页固定70条

DIRECTION_MAP = {"B": "买盘", "S": "卖盘", "N": "中性盘"}


def is_trading_time() -> bool:
    now = datetime.now().strftime("%H:%M")
    return any(start <= now <= end for start, end in TRADING_PERIODS)


def fetch_tick_page(symbol: str, page: int) -> list[dict]:
    """
    请求腾讯逐笔成交单页数据（~0.2s），返回解析后的记录列表。
    page=0 为最早数据，页码越大数据越新。
    """
    params = {"appn": "detail", "action": "data", "c": symbol, "p": str(page)}
    r = http_requests.get(TX_TICK_URL, params=params, timeout=5)
    text = r.text
    if '"' not in text:
        return []
    data_str = text.split('"')[1].strip()
    if not data_str:
        return []

    records = []
    for item in data_str.split("|"):
        parts = item.split("/")
        if len(parts) < 7:
            continue
        records.append({
            "序号": int(parts[0]),
            "成交时间": parts[1],
            "成交价格": float(parts[2]),
            "价格变动": float(parts[3]),
            "成交量": int(parts[4]),
            "成交金额": float(parts[5]),
            "性质": DIRECTION_MAP.get(parts[6], parts[6]),
        })
    return records


def fetch_tick_incremental(symbol: str, last_seq: int) -> list[dict]:
    """
    增量拉取：从 last_seq 之后开始，只请求包含新数据的页。
    返回所有 序号 > last_seq 的记录。
    """
    start_page = max(0, last_seq // TX_PAGE_SIZE)
    all_new = []
    page = start_page
    while True:
        records = fetch_tick_page(symbol, page)
        if not records:
            break
        for rec in records:
            if rec["序号"] > last_seq:
                all_new.append(rec)
        if len(records) < TX_PAGE_SIZE:
            break
        page += 1
    return all_new


def fetch_all_pages(symbol: str) -> list[dict]:
    """首次全量拉取所有页"""
    all_records = []
    page = 0
    while True:
        records = fetch_tick_page(symbol, page)
        if not records:
            break
        all_records.extend(records)
        if len(records) < TX_PAGE_SIZE:
            break
        page += 1
    return all_records


# ── CSV / 格式化 ──

def append_csv(filepath: str, df: pd.DataFrame):
    write_header = not os.path.exists(filepath)
    df.to_csv(filepath, mode="a", index=False, header=write_header, encoding="utf-8-sig")


def format_big_order(rec: dict) -> str:
    kind_icon = {"买盘": "+", "卖盘": "-", "中性盘": "="}
    icon = kind_icon.get(rec["性质"], "?")
    return (
        f"  [{icon}] {rec['成交时间']}  "
        f"价格={rec['成交价格']:.2f}  "
        f"量={rec['成交量']}手  "
        f"额={rec['成交金额'] / 10000:.1f}万  "
        f"{rec['性质']}"
    )


# ── 收盘日报 ──

def generate_daily_report(symbol: str, threshold: int, total_big_buy: float, total_big_sell: float, webhook_url: str) -> str:
    """收盘后拉取全量数据，生成当日大单报告 CSV 并发送飞书日报"""
    today = datetime.now().strftime("%Y%m%d")
    os.makedirs("output", exist_ok=True)
    daily_csv = f"output/{symbol}_daily_big_orders_{threshold}_{today}.csv"

    logger.info("收盘，正在生成当日大单报告...")
    try:
        all_records = fetch_all_pages(symbol)
        if not all_records:
            logger.warning("未获取到数据，跳过日报生成")
            return ""

        df = pd.DataFrame(all_records)
        big = df[df["成交量"] >= threshold].copy()
        big["成交金额_万"] = (big["成交金额"] / 10000).round(1)
        big.to_csv(daily_csv, index=False, encoding="utf-8-sig")

        buy_count = len(big[big["性质"] == "买盘"])
        sell_count = len(big[big["性质"] == "卖盘"])
        net = (total_big_buy - total_big_sell) / 10000

        print(f"\n{'=' * 60}")
        print(f"【日报】{symbol} {today} 大单汇总（阈值 >= {threshold}手）")
        print(f"  大单笔数: {len(big)} (买{buy_count} 卖{sell_count})")
        print(f"  大单买入: {total_big_buy / 10000:.0f}万")
        print(f"  大单卖出: {total_big_sell / 10000:.0f}万")
        print(f"  净流入:   {net:.0f}万")
        print(f"  报告文件: {daily_csv}")
        print(f"{'=' * 60}")

        if webhook_url:
            send_feishu(
                webhook_url,
                f"{symbol} {today} 大单日报",
                [
                    f"大单笔数: {len(big)} (买{buy_count} 卖{sell_count})",
                    f"大单买入: {total_big_buy / 10000:.0f}万",
                    f"大单卖出: {total_big_sell / 10000:.0f}万",
                    f"净流入: {net:.0f}万",
                ],
            )

        return daily_csv
    except Exception as e:
        logger.error(f"生成日报失败: {e}")
        return ""


# ── 主监控逻辑 ──

def monitor(symbol: str, threshold: int, interval: int, webhook_url: str):
    logger.info(f"开始监控 {symbol}，大单阈值 >= {threshold} 手，轮询间隔 {interval}s")
    logger.info("使用腾讯分页接口，增量拉取（秒级延迟）")
    if webhook_url:
        logger.info("飞书通知已启用")
    else:
        logger.info("飞书通知未配置，仅终端输出")

    os.makedirs("output", exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    csv_path = f"output/{symbol}_monitor_{threshold}_{today}.csv"

    last_seq = -1
    total_big_buy = 0.0
    total_big_sell = 0.0
    poll_round = 0

    # 首次全量拉取
    logger.info("首次拉取全量数据...")
    try:
        t0 = time.time()
        all_records = fetch_all_pages(symbol)
        elapsed = time.time() - t0
        if all_records:
            last_seq = all_records[-1]["序号"]
        big = [r for r in all_records if r["成交量"] >= threshold]
        logger.info(
            f"全量 {len(all_records)} 条 (耗时{elapsed:.1f}s)，已有 {len(big)} 笔大单，last_seq={last_seq}"
        )
        if big:
            df_big = pd.DataFrame(big)
            df_big["成交金额_万"] = (df_big["成交金额"] / 10000).round(1)
            append_csv(csv_path, df_big)
            for r in big:
                if r["性质"] == "买盘":
                    total_big_buy += r["成交金额"]
                elif r["性质"] == "卖盘":
                    total_big_sell += r["成交金额"]
    except Exception as e:
        logger.error(f"首次拉取失败: {e}")

    # 轮询循环
    while True:
        try:
            time.sleep(interval)

            if not is_trading_time():
                now_str = datetime.now().strftime("%H:%M")
                if now_str > "15:05":
                    logger.info("已过收盘时间，监控结束")
                    break
                continue

            poll_round += 1
            t0 = time.time()
            new_records = fetch_tick_incremental(symbol, last_seq)
            elapsed = time.time() - t0

            if not new_records:
                logger.debug(f"[#{poll_round}] 无新数据 ({elapsed:.2f}s)")
                continue

            last_seq = new_records[-1]["序号"]
            big = [r for r in new_records if r["成交量"] >= threshold]

            if not big:
                logger.debug(f"[#{poll_round}] +{len(new_records)}条，无大单 ({elapsed:.2f}s)")
                continue

            # 发现大单
            logger.info(f"[#{poll_round}] +{len(new_records)}条，发现 {len(big)} 笔大单! ({elapsed:.2f}s)")

            print(f"\n{'─' * 60}")
            print(f"  {datetime.now().strftime('%H:%M:%S')} 发现 {len(big)} 笔大单（>= {threshold}手）")
            print(f"{'─' * 60}")
            for rec in big:
                print(format_big_order(rec))

            for rec in big:
                if rec["性质"] == "买盘":
                    total_big_buy += rec["成交金额"]
                elif rec["性质"] == "卖盘":
                    total_big_sell += rec["成交金额"]
            net = (total_big_buy - total_big_sell) / 10000
            print(f"  累计大单: 买入={total_big_buy / 10000:.0f}万 卖出={total_big_sell / 10000:.0f}万 净流入={net:.0f}万")

            # CSV
            df_big = pd.DataFrame(big)
            df_big["成交金额_万"] = (df_big["成交金额"] / 10000).round(1)
            append_csv(csv_path, df_big)

            # 飞书
            if webhook_url:
                buy_count = sum(1 for r in big if r["性质"] == "买盘")
                sell_count = sum(1 for r in big if r["性质"] == "卖盘")
                buy_sell_info = f"买{buy_count}卖{sell_count}" if buy_count or sell_count else ""
                lines = [format_big_order(rec) for rec in big]
                lines.append(f"\n累计大单净流入: {net:.0f}万")
                send_feishu(
                    webhook_url,
                    f"{symbol} {len(big)}笔大单(>={threshold}手) {buy_sell_info} 净流入{net:.0f}万",
                    lines,
                )

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"轮询异常: {e}")
            time.sleep(interval)

    # 收盘后生成当日大单报告
    daily_csv = generate_daily_report(symbol, threshold, total_big_buy, total_big_sell, webhook_url)


def main():
    parser = argparse.ArgumentParser(description="盘中大单实时监控")
    parser.add_argument("--symbol", type=str, default="sh601166", help="股票代码，如 sh601166")
    parser.add_argument("--threshold", type=int, default=1000, help="大单阈值（手）")
    parser.add_argument("--interval", type=int, default=5, help="轮询间隔（秒），默认5秒")
    parser.add_argument("--webhook", type=str, default="", help="飞书 webhook URL")
    args = parser.parse_args()

    webhook = args.webhook or FEISHU_WEBHOOK_URL
    monitor(args.symbol, args.threshold, args.interval, webhook)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
