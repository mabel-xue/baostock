"""
查询我的基金持仓信息
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd
from datetime import datetime

# 我的基金列表
MY_FUNDS = [
    ("160218", "国泰国证房地产行业指数A"),
    ("167301", "方正富邦中证保险A"),
    ("005662", "嘉实金融精选股票A"),
    ("006289", "华夏养老2040三年持有混合(FOF)A"),
    ("673100", "西部利得沪深300指数增强A"),
    ("161127", "易方达标普生物科技人民币A"),
    ("010409", "富国消费精选30股票A"),
    ("014414", "招商中证畜牧养殖ETF联接A"),
    ("002943", "广发多因子混合"),
    ("001170", "宏利复兴混合A"),
    ("018240", "嘉实制造升级股票发起式A"),
    ("004818", "国寿安保目标策略混合发起A"),
    ("000423", "前海开源事件驱动混合A"),
    ("022164", "西部利得央企优选股票A"),
    ("000251", "工银金融地产混合A"),
]

def query_fund_holdings(fund_code: str, year: str = None) -> pd.DataFrame:
    """查询基金持仓"""
    if year is None:
        year = str(datetime.now().year - 1)
    try:
        df = ak.fund_portfolio_hold_em(symbol=fund_code, date=year)
        return df
    except Exception as e:
        print(f"  查询失败: {e}")
        return pd.DataFrame()


def main():
    year = str(datetime.now().year - 1)  # 默认查询上一年数据
    print(f"查询年份: {year}")
    print("=" * 80)

    all_results = {}

    for fund_code, fund_name in MY_FUNDS:
        print(f"\n【{fund_name}】({fund_code})")
        df = query_fund_holdings(fund_code, year)

        if df is None or df.empty:
            print("  暂无持仓数据")
            continue

        all_results[fund_code] = df

        # 获取最新一期（最大季度）
        if "季度" in df.columns:
            latest_quarter = df["季度"].max()
            latest_df = df[df["季度"] == latest_quarter]
            print(f"  最新报告期: {latest_quarter}，共 {len(latest_df)} 只持仓")
        else:
            latest_df = df
            print(f"  共 {len(latest_df)} 条持仓记录")

        # 展示前10持仓
        show_cols = [c for c in ["股票代码", "股票名称", "占净值比例", "持股数", "持仓市值", "季度"] if c in latest_df.columns]
        if show_cols:
            print(latest_df[show_cols].head(10).to_string(index=False))
        else:
            print(latest_df.head(10).to_string(index=False))

    # 汇总：统计各只股票被多少基金持有
    print("\n" + "=" * 80)
    print("【持仓汇总：各股票被持有基金数】")

    stock_counter = {}
    for fund_code, df in all_results.items():
        fund_name = dict(MY_FUNDS).get(fund_code, fund_code)
        # 取最新季度
        if "季度" in df.columns:
            latest_quarter = df["季度"].max()
            df = df[df["季度"] == latest_quarter]

        code_col = "股票代码" if "股票代码" in df.columns else df.columns[0]
        name_col = "股票名称" if "股票名称" in df.columns else None

        for _, row in df.iterrows():
            code = row[code_col]
            name = row[name_col] if name_col else code
            if code not in stock_counter:
                stock_counter[code] = {"名称": name, "持有基金数": 0, "持有基金": []}
            stock_counter[code]["持有基金数"] += 1
            stock_counter[code]["持有基金"].append(fund_name)

    if stock_counter:
        summary = pd.DataFrame([
            {"股票代码": k, "股票名称": v["名称"], "持有基金数": v["持有基金数"], "持有基金": ", ".join(v["持有基金"])}
            for k, v in sorted(stock_counter.items(), key=lambda x: -x[1]["持有基金数"])
        ])
        multi_held = summary[summary["持有基金数"] > 1]
        if not multi_held.empty:
            print(f"\n被多只基金共同持有的股票（{len(multi_held)} 只）：")
            print(multi_held.to_string(index=False))
        else:
            print("没有被多只基金共同持有的股票")

        # 保存完整汇总
        output_path = f"output/my_funds_holdings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        os.makedirs("output", exist_ok=True)

        all_dfs = []
        for fund_code, df in all_results.items():
            fund_name = dict(MY_FUNDS).get(fund_code, fund_code)
            df = df.copy()
            df.insert(0, "基金名称", fund_name)
            df.insert(0, "基金代码", fund_code)
            if "季度" in df.columns:
                latest_quarter = df["季度"].max()
                df = df[df["季度"] == latest_quarter]
            all_dfs.append(df)

        if all_dfs:
            result_df = pd.concat(all_dfs, ignore_index=True)
            result_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"\n完整数据已保存到: {output_path}")


if __name__ == "__main__":
    main()
