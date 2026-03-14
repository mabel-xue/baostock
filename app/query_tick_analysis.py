"""
个股盘中逐笔成交分析 —— 大单阈值计算与资金流向统计

数据来源: 腾讯财经 (stock_zh_a_tick_tx_js)，仅支持最近交易日
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime


def fetch_tick_data(symbol: str) -> pd.DataFrame:
    """
    获取个股最近交易日逐笔成交数据

    Args:
        symbol: 股票代码，格式 sh600519 / sz000001
    """
    df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
    df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
    df['成交金额'] = pd.to_numeric(df['成交金额'], errors='coerce')
    df['成交价格'] = pd.to_numeric(df['成交价格'], errors='coerce')
    return df


def analyze_big_order_threshold(df: pd.DataFrame) -> dict:
    """
    根据成交量分布，计算大单阈值建议

    Returns:
        dict: 包含分位数统计、各阈值下的大单占比等
    """
    total_count = len(df)
    total_vol = df['成交量'].sum()
    total_amt = df['成交金额'].sum()
    price = df['成交价格'].iloc[-1]

    # 分位数
    percentiles = {}
    for p in [50, 75, 80, 85, 90, 95, 97, 99]:
        percentiles[f'P{p}'] = {
            '成交量_手': int(df['成交量'].quantile(p / 100)),
            '成交金额_元': int(df['成交金额'].quantile(p / 100)),
        }

    # 自动推算候选阈值（基于 P90/P95/P99 和常见整数档位）
    p95_vol = int(df['成交量'].quantile(0.95))
    candidates = sorted(set([
        round_to_nice(p95_vol * 0.8),
        round_to_nice(p95_vol),
        round_to_nice(p95_vol * 2),
        round_to_nice(p95_vol * 4),
        round_to_nice(p95_vol * 8),
    ]))

    threshold_stats = []
    for t in candidates:
        big = df[df['成交量'] >= t]
        if len(big) == 0:
            continue
        threshold_stats.append({
            '阈值_手': t,
            '阈值_万元': round(t * price * 100 / 10000, 1),
            '笔数': len(big),
            '笔数占比%': round(len(big) / total_count * 100, 1),
            '占总成交量%': round(big['成交量'].sum() / total_vol * 100, 1),
            '占总成交金额%': round(big['成交金额'].sum() / total_amt * 100, 1),
        })

    return {
        'price': price,
        'total_count': total_count,
        'total_vol': total_vol,
        'total_amt': total_amt,
        'percentiles': percentiles,
        'threshold_stats': threshold_stats,
    }


def analyze_money_flow(df: pd.DataFrame, big_threshold: int) -> dict:
    """
    按买卖方向统计资金流向

    Args:
        df: 逐笔成交数据
        big_threshold: 大单阈值（手）
    """
    result = {}
    for label, sub in [('全部', df), ('大单', df[df['成交量'] >= big_threshold])]:
        flow = {}
        for kind in ['买盘', '卖盘', '中性盘']:
            k = sub[sub['性质'] == kind]
            flow[kind] = {
                '笔数': len(k),
                '成交量_手': int(k['成交量'].sum()),
                '成交金额_万': round(k['成交金额'].sum() / 10000, 1),
            }
        buy_amt = flow['买盘']['成交金额_万']
        sell_amt = flow['卖盘']['成交金额_万']
        flow['净流入_万'] = round(buy_amt - sell_amt, 1)
        result[label] = flow
    return result


def round_to_nice(n: float) -> int:
    """将数字取整到 '好看' 的整数（如 500, 1000, 2000, 5000）"""
    if n <= 0:
        return 100
    magnitude = 10 ** int(np.log10(max(n, 1)))
    nice_values = [1, 2, 5, 10]
    ratio = n / magnitude
    for v in nice_values:
        if ratio <= v:
            return int(v * magnitude)
    return int(10 * magnitude)


def print_report(symbol: str, df: pd.DataFrame, analysis: dict, flow: dict, big_threshold: int):
    """打印分析报告"""
    price = analysis['price']
    print(f"\n{'=' * 70}")
    print(f"【{symbol}】逐笔成交大单分析")
    print(f"{'=' * 70}")
    print(f"当前价格: {price} 元 | 总笔数: {analysis['total_count']} | "
          f"总成交量: {analysis['total_vol']} 手 | "
          f"总成交金额: {analysis['total_amt'] / 10000:.0f} 万元")

    print(f"\n--- 成交量分位数 ---")
    for k, v in analysis['percentiles'].items():
        vol = v['成交量_手']
        amt_wan = round(vol * price * 100 / 10000, 1)
        print(f"  {k}: {vol} 手 (≈{amt_wan} 万元)")

    print(f"\n--- 大单阈值分析 ---")
    stats_df = pd.DataFrame(analysis['threshold_stats'])
    if not stats_df.empty:
        print(stats_df.to_string(index=False))

    print(f"\n--- 资金流向（大单阈值 = {big_threshold} 手，≈{round(big_threshold * price * 100 / 10000, 1)} 万元）---")
    for label in ['全部', '大单']:
        f = flow[label]
        print(f"\n  [{label}交易]")
        for kind in ['买盘', '卖盘', '中性盘']:
            info = f[kind]
            print(f"    {kind}: {info['笔数']}笔, {info['成交量_手']}手, {info['成交金额_万']}万元")
        print(f"    净流入: {f['净流入_万']} 万元")

    # TOP15 大单
    big_orders = df[df['成交量'] >= big_threshold].nlargest(15, '成交量')
    if not big_orders.empty:
        print(f"\n--- TOP{len(big_orders)} 大单明细 ---")
        show = big_orders[['成交时间', '成交价格', '成交量', '成交金额', '性质']].copy()
        show['成交金额_万'] = (show['成交金额'] / 10000).round(1)
        print(show.to_string(index=False))


def export_big_orders(df: pd.DataFrame, symbol: str, threshold: int, output_dir: str = "output"):
    """
    导出超过阈值的大单明细到 CSV

    Args:
        df: 逐笔成交数据
        symbol: 股票代码
        threshold: 大单阈值（手）
        output_dir: 输出目录
    """
    big = df[df['成交量'] >= threshold].sort_values('成交时间').copy()
    if big.empty:
        print(f"没有 >= {threshold} 手的成交记录")
        return None

    big['成交金额_万'] = (big['成交金额'] / 10000).round(1)

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/{symbol}_big_orders_{threshold}_{ts}.csv"
    big.to_csv(filename, index=False, encoding='utf-8-sig')

    print(f"\n共 {len(big)} 笔 >= {threshold} 手的大单，已保存到: {filename}")
    return filename


def run(symbol: str, big_threshold: int = None, export_threshold: int = None):
    """
    主入口：分析个股逐笔成交

    Args:
        symbol: 股票代码，如 sh601166
        big_threshold: 大单阈值（手），不指定则自动根据 P95 推算
        export_threshold: 导出大单明细的阈值（手），不指定则不导出
    """
    print(f"正在获取 {symbol} 逐笔成交数据...")
    df = fetch_tick_data(symbol)
    analysis = analyze_big_order_threshold(df)

    if big_threshold is None:
        big_threshold = int(df['成交量'].quantile(0.95))
        big_threshold = round_to_nice(big_threshold)

    flow = analyze_money_flow(df, big_threshold)
    print_report(symbol, df, analysis, flow, big_threshold)

    if export_threshold is not None:
        export_big_orders(df, symbol, export_threshold)

    return df, analysis, flow


def main():
    stocks = [
        ("sh601166", "兴业银行"),
    ]

    for symbol, name in stocks:
        print(f"\n正在分析: {name} ({symbol})")
        run(symbol, export_threshold=1000)

    print(f"\n{'=' * 70}")
    print("分析完成!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
