"""
查询我的基金持仓信息，并对基金间持仓相似度做量化分析。

用法：
  在项目根目录执行（需已安装 akshare、pandas）：
    python app/query_my_funds.py
  或进入 app 目录后：
    python query_my_funds.py
  默认拉取「上一年」公开持仓（与 AkShare 接口一致），结果打印在终端；
  同时在项目下 output/ 生成带时间戳的 CSV：
    my_funds_holdings_*.csv   — 各基金最新一期持仓明细合并表
    my_funds_similarity_*.csv — 基金两两相似度（Jaccard、重叠系数、加权余弦）
  维护列表：MY_FUNDS、MY_ETFS（场内 ETF，每项为 代码/简称/跟踪指数）；
  ALL_TRACKED 仅取前两列参与持仓与相似度。跟踪指数来自天天基金 tsdata 页，
  可用 query_etf_tracking_index_em(代码) 在线刷新核对。

相似度算法（基于最新报告期股票持仓集合与占净值比例）：
- Jaccard：|A∩B| / |A∪B|，同时看重合与总分散度，适合比较「持仓集合有多像」。
- 重叠系数（Szymkiewicz–Simpson）：|A∩B| / min(|A|,|B|)，强调较小持仓集合被覆盖的比例。
- 加权余弦：在 A∪B 上把缺失权重视为 0，对占净值比例向量做余弦相似度，重仓重合会拉高得分。
"""

import sys
import os
import math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import akshare as ak
import pandas as pd
from datetime import datetime

# 我的基金列表（原持仓 + 自选截图新增；002943 仅保留原条目）
MY_FUNDS = [
    # 待清仓
    ("005662", "嘉实金融精选股票A"),
    ("006289", "华夏养老2040三年持有混合(FOF)A"),
    ("161029", "富国中证银行指数(LOF)A"),
    ("519736", "交银新成长混合"),
    ("001594", "天弘中证银行ETF联接A"),
    ("501203", "易方达创新未来混合(LOF)"),
    ("161127", "易方达标普生物科技人民币A"),
    ("008283", "易方达金融行业股票发起式A"),
    ("161121", "易方达中证银行ETF联接(LOF)A"),
    ("022925", "易方达中证红利ETF联接发起式Y"),
    # 转仓
    ("673100", "西部利得沪深300指数增强A"),
    ("110003", "易方达上证50增强A"),
    ("007345", "富国科技创新灵活配置混合"),
    # 低位持仓
    ("160218", "国泰国证房地产行业指数A"),
    ("014414", "招商中证畜牧养殖ETF联接A"),
    ("000251", "工银金融地产混合A"),
    ("010409", "富国消费精选30股票A"),
    # Y基金
    ("022913", "易方达中证500ETF联接发起式Y"),
    ("022914", "易方达沪深300精选增强Y"),
    ("022930", "易方达中证A500ETF联接Y"),
    ("017331", "易方达汇康稳健养老目标一年持有混合(FOF)Y"),
    # 长期持仓
    ("167301", "方正富邦中证保险A"),
    # 主动战略持仓
    ("002943", "广发多因子混合"),
    ("001170", "宏利复兴混合A"),
    ("004818", "国寿安保目标策略混合发起A"),
    ("000423", "前海开源事件驱动混合A"),
    ("022164", "西部利得央企优选股票A"),
    # 观察
    ("018240", "嘉实制造升级股票发起式A"),
]

# 场内 ETF：（基金代码, 简称, 跟踪指数名称），第三列由天天基金 tsdata 页解析填入
MY_ETFS = [
    # 清仓
    ("159822", "新经济ETF银华", "标普中国新经济行业指数"),
    # 低位持仓
    ("159905", "红利ETF工银", "深证红利指数"),
    ("512690", "酒ETF", "中证酒指数"),
    ("516670", "畜牧养殖ETF招商", "中证畜牧养殖指数"),
    ("513330", "恒生互联网ETF", "恒生互联网科技业指数"),
    # 海外
    ("159632", "纳斯达克ETF华安", "纳斯达克100指数"),
    ("513880", "日经225ETF华安", "东京日经225指数"),
    # 长期持仓
    ("510900", "恒生中国企业ETF", "恒生中国企业指数"),
    ("512880", "证券ETF国泰", "中证全指证券公司指数"),
    ("588000", "科创50ETF", "上证科创板50成份指数"),
    ("560050", "中国A50ETF汇添富", "MSCI中国A50互联互通人民币指数"),
    # 战略持仓
    ("562500", "机器人ETF", "中证机器人指数"),
    ("159611", "电力ETF广发", "中证全指电力公用事业指数"),
    ("159363", "创业板人工智能ETF", "创业板人工智能指数"),
]

ALL_TRACKED = [*MY_FUNDS, *[(c, n) for c, n, _ in MY_ETFS]]
ETF_TRACKING_BY_CODE = {c: t for c, _, t in MY_ETFS}


def query_etf_tracking_index_em(fund_code: str) -> tuple:
    """
    查询场内基金在天天基金「跟踪业绩」页披露的跟踪指数名称及年化跟踪误差。

    :param fund_code: 基金代码，如 \"159905\"
    :return: (跟踪指数名称, 年化跟踪误差字符串)，解析失败返回 (None, None)
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None, None
    url = f"https://fundf10.eastmoney.com/tsdata_{fund_code}.html"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "lxml")
        for tbl in soup.find_all("table"):
            rows = [
                [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
                for tr in tbl.find_all("tr")
            ]
            for i, row in enumerate(rows):
                if row and row[0] == "跟踪指数" and i + 1 < len(rows):
                    nxt = rows[i + 1]
                    if len(nxt) >= 2:
                        return nxt[0], nxt[1]
                    return (nxt[0] if nxt else None), None
        return None, None
    except (requests.RequestException, OSError, ValueError):
        return None, None


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


def latest_holdings_subset(df: pd.DataFrame) -> pd.DataFrame:
    """取最新报告期持仓子表。"""
    if df is None or df.empty:
        return pd.DataFrame()
    if "季度" in df.columns:
        q = df["季度"].max()
        return df[df["季度"] == q].copy()
    return df.copy()


def normalize_stock_code(code) -> str:
    """统一为 6 位股票代码，便于跨基金比对。"""
    if code is None:
        return ""
    try:
        if pd.isna(code):
            return ""
    except (TypeError, ValueError):
        pass
    digits = "".join(ch for ch in str(code).strip() if ch.isdigit())
    if not digits:
        return ""
    return digits[-6:] if len(digits) >= 6 else digits.zfill(6)


def parse_weight_pct(val) -> float:
    """解析占净值比例字段（数值或带 % 的字符串）。"""
    if val is None:
        return 0.0
    try:
        if pd.isna(val):
            return 0.0
    except (TypeError, ValueError):
        pass
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("%", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def extract_stocks_and_weights(latest: pd.DataFrame) -> tuple[set[str], dict[str, float]]:
    """
    从最新一期持仓得到股票集合与代码→占净值比例（同代码多行则累加）。
    若无权重列，则退化为每只股票权重 1.0（等价于仅按是否持有做余弦）。
    """
    if latest.empty:
        return set(), {}
    code_col = "股票代码" if "股票代码" in latest.columns else latest.columns[0]
    wcol = None
    for c in ("占净值比例", "占净值百分比", "持股占净值比"):
        if c in latest.columns:
            wcol = c
            break
    stocks: set[str] = set()
    weights: dict[str, float] = {}
    for _, row in latest.iterrows():
        c = normalize_stock_code(row[code_col])
        if not c:
            continue
        stocks.add(c)
        if wcol is not None:
            weights[c] = weights.get(c, 0.0) + parse_weight_pct(row[wcol])
    if wcol is None and stocks:
        weights = {s: 1.0 for s in stocks}
    return stocks, weights


def jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def overlap_coefficient(a: set[str], b: set[str]) -> float:
    """|A∩B| / min(|A|,|B|)"""
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / min(len(a), len(b))


def cosine_weight_similarity(wa: dict[str, float], wb: dict[str, float]) -> float:
    """在 A∪B 上对齐向量后的余弦相似度。"""
    keys = set(wa) | set(wb)
    if not keys:
        return 0.0
    dot = sum(wa.get(k, 0.0) * wb.get(k, 0.0) for k in keys)
    na = math.sqrt(sum(wa.get(k, 0.0) ** 2 for k in keys))
    nb = math.sqrt(sum(wb.get(k, 0.0) ** 2 for k in keys))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def build_per_fund_holdings(all_results: dict[str, pd.DataFrame]) -> dict[str, tuple[set[str], dict[str, float]]]:
    out: dict[str, tuple[set[str], dict[str, float]]] = {}
    for fund_code, df in all_results.items():
        latest = latest_holdings_subset(df)
        stocks, weights = extract_stocks_and_weights(latest)
        out[fund_code] = (stocks, weights)
    return out


def pairwise_similarity_table(
    per_fund: dict[str, tuple[set[str], dict[str, float]]],
    code_to_name: dict[str, str],
) -> pd.DataFrame:
    codes = [c for c, (s, _) in per_fund.items() if s]
    rows = []
    for i in range(len(codes)):
        for j in range(i + 1, len(codes)):
            ca, cb = codes[i], codes[j]
            sa, sb = per_fund[ca][0], per_fund[cb][0]
            wa, wb = per_fund[ca][1], per_fund[cb][1]
            rows.append(
                {
                    "基金A代码": ca,
                    "基金A": code_to_name.get(ca, ca),
                    "基金B代码": cb,
                    "基金B": code_to_name.get(cb, cb),
                    "共同持股数": len(sa & sb),
                    "A持股数": len(sa),
                    "B持股数": len(sb),
                    "Jaccard": round(jaccard_similarity(sa, sb), 4),
                    "重叠系数": round(overlap_coefficient(sa, sb), 4),
                    "加权余弦": round(cosine_weight_similarity(wa, wb), 4),
                }
            )
    return pd.DataFrame(rows)


def print_and_save_similarity(pair_df: pd.DataFrame, ts: str) -> None:
    if pair_df.empty:
        print("\n【持仓相似度】有效持仓基金不足 2 只，跳过两两对比。")
        return
    os.makedirs("output", exist_ok=True)
    path = f"output/my_funds_similarity_{ts}.csv"
    pair_df.to_csv(path, index=False, encoding="utf-8-sig")
    print("\n" + "=" * 80)
    print("【持仓相似度：两两指标】（完整表已保存）")
    print(f"  {path}")
    top_n = 12
    by_j = pair_df.sort_values("Jaccard", ascending=False).head(top_n)
    print(f"\n按 Jaccard 最高的前 {top_n} 对（重仓重合多、集合也接近时偏高）：")
    cols = ["基金A", "基金B", "共同持股数", "Jaccard", "重叠系数", "加权余弦"]
    print(by_j[cols].to_string(index=False))
    by_cos = pair_df.sort_values("加权余弦", ascending=False).head(top_n)
    print(f"\n按加权余弦最高的前 {top_n} 对（占净值比例向量方向接近）：")
    print(by_cos[cols].to_string(index=False))


def main():
    year = str(datetime.now().year - 1)  # 默认查询上一年数据
    print(f"查询年份: {year}")
    print("=" * 80)

    all_results = {}

    for fund_code, fund_name in ALL_TRACKED:
        print(f"\n【{fund_name}】({fund_code})")
        if fund_code in ETF_TRACKING_BY_CODE:
            print(f"  跟踪指数: {ETF_TRACKING_BY_CODE[fund_code]}")
        df = query_fund_holdings(fund_code, year)

        if df is None or df.empty:
            print("  暂无持仓数据")
            continue

        all_results[fund_code] = df

        latest_df = latest_holdings_subset(df)
        if "季度" in df.columns and not latest_df.empty:
            print(f"  最新报告期: {latest_df['季度'].iloc[0]}，共 {len(latest_df)} 只持仓")
        elif not latest_df.empty:
            print(f"  共 {len(latest_df)} 条持仓记录")
        else:
            print("  暂无持仓记录")

        # 展示前10持仓
        show_cols = [c for c in ["股票代码", "股票名称", "占净值比例", "持股数", "持仓市值", "季度"] if c in latest_df.columns]
        if show_cols:
            print(latest_df[show_cols].head(10).to_string(index=False))
        else:
            print(latest_df.head(10).to_string(index=False))

    # 汇总：统计各只股票被多少基金持有
    print("\n" + "=" * 80)
    print("【持仓汇总：各股票被持有基金数】")

    code_to_name = dict(ALL_TRACKED)
    stock_counter = {}
    for fund_code, df in all_results.items():
        fund_name = code_to_name.get(fund_code, fund_code)
        df = latest_holdings_subset(df)

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

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"output/my_funds_holdings_{ts}.csv"
        os.makedirs("output", exist_ok=True)

        all_dfs = []
        for fund_code, df in all_results.items():
            fund_name = code_to_name.get(fund_code, fund_code)
            sub = latest_holdings_subset(df)
            if sub.empty:
                continue
            sub = sub.copy()
            sub.insert(0, "基金名称", fund_name)
            sub.insert(0, "基金代码", fund_code)
            all_dfs.append(sub)

        if all_dfs:
            result_df = pd.concat(all_dfs, ignore_index=True)
            result_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"\n完整数据已保存到: {output_path}")

        per_fund = build_per_fund_holdings(all_results)
        pair_df = pairwise_similarity_table(per_fund, code_to_name)
        print_and_save_similarity(pair_df, ts)


if __name__ == "__main__":
    main()
