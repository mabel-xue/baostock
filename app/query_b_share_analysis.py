"""
B股分化预测分析 —— 79只B股三梯队分类（2026年—2028年）

分类规则：
  第一梯队（约20–25只）：有A股母公司、高股息、基本面好 → 优先B股转A股，2–3年内完成
  第二梯队（约30–35只）：优质纯B/有H股基础 → B转H或被A股吸并
  第三梯队（约15–20只）：绩差、无母公司、流动性枯竭 → 主动或强制退市

数据源（AkShare / 新浪财经 / 交易所）：
- stock_zh_b_spot()：B股实时行情（新浪，代码、名称、价格、成交量等）
- stock_info_a_code_name()：A股代码名录（用于匹配同公司A股）
- stock_financial_abstract()：个股财务摘要（EPS、每股净资产、ROE等，按需逐只获取）

用法（在项目根目录）：
  python app/query_b_share_analysis.py                 # 完整分析（含财务数据）
  python app/query_b_share_analysis.py --fast           # 快速模式（跳过逐只财务查询）
  python app/query_b_share_analysis.py --tier 1         # 只看第一梯队
  python app/query_b_share_analysis.py --tier 3         # 只看第三梯队
  python app/query_b_share_analysis.py --no-export      # 不导出CSV
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import warnings
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

TIER_META = {
    1: {
        "label": "第一梯队",
        "target": "约20–25只",
        "desc": "有A股母公司、基本面好 → 优先B股转A股，2–3年内完成",
        "prediction": "B转A",
    },
    2: {
        "label": "第二梯队",
        "target": "约30–35只",
        "desc": "优质纯B/有H股基础 → B转H或被A股吸并",
        "prediction": "B转H / 被吸并",
    },
    3: {
        "label": "第三梯队",
        "target": "约15–20只",
        "desc": "绩差、无母公司、流动性枯竭 → 主动或强制退市",
        "prediction": "退市风险",
    },
}


# ═══════════════════════════════════════════════════════════════════
#  数据获取
# ═══════════════════════════════════════════════════════════════════

MAX_RETRIES = 3
RETRY_DELAY = 2


def _retry(fn, label: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            result = fn()
            if result is not None:
                return result
        except Exception as e:
            print(f"  {label}: 第{attempt}次失败 — {str(e)[:100]}")
        if attempt < retries:
            time.sleep(RETRY_DELAY * attempt)
    return None


def fetch_b_shares() -> pd.DataFrame:
    """获取B股实时行情（新浪财经）"""
    print("正在获取B股实时行情 (stock_zh_b_spot) ...")
    df = _retry(ak.stock_zh_b_spot, "B股行情")
    if df is None or df.empty:
        print("  ✗ 未获取到B股数据")
        return pd.DataFrame()
    print(f"  ✓ 共获取 {len(df)} 只B股")
    return df


def fetch_a_code_name() -> pd.DataFrame:
    """获取A股代码名录（轻量接口）"""
    print("正在获取A股名录 (stock_info_a_code_name) ...")
    df = _retry(ak.stock_info_a_code_name, "A股名录")
    if df is None or df.empty:
        print("  ✗ 未获取到A股名录")
        return pd.DataFrame()
    if "code" in df.columns:
        df = df.rename(columns={"code": "代码", "name": "名称"})
    print(f"  ✓ 共获取 {len(df)} 只A股")
    return df


def _parse_financial_df(df: pd.DataFrame) -> dict | None:
    """从 stock_financial_abstract 返回的 DataFrame 中提取核心指标"""
    if df is None or df.empty or len(df.columns) < 3:
        return None

    latest_col = df.columns[2]

    def _get(section: str, metric: str) -> float | None:
        mask = (df.iloc[:, 0] == section) & (df.iloc[:, 1].str.contains(metric, na=False))
        rows = df.loc[mask, latest_col]
        if rows.empty:
            return None
        val = pd.to_numeric(rows.iloc[0], errors="coerce")
        return None if pd.isna(val) else float(val)

    return {
        "eps": _get("常用指标", "基本每股收益"),
        "bvps": _get("常用指标", "每股净资产"),
        "roe": _get("常用指标", "净资产收益率"),
        "net_profit": _get("常用指标", "归母净利润"),
        "debt_ratio": _get("常用指标", "资产负债率"),
        "gross_margin": _get("常用指标", "毛利率"),
    }


def fetch_financial_for_stock(symbol: str) -> dict | None:
    """
    获取单只股票的核心财务指标（新浪 stock_financial_abstract）。
    symbol 格式: sh900901 / sz200002
    深圳B股(200xxx)的财务数据在新浪系统中挂在对应A股代码(000xxx)下，
    因此优先用B股代码查，查不到则自动转A股代码重试。
    """
    candidates = [symbol]
    raw = re.sub(r"^[a-z]{2}", "", symbol.strip())
    if raw.startswith("200"):
        a_code = "000" + raw[3:]
        candidates.append(f"sz{a_code}")

    for sym in candidates:
        try:
            df = ak.stock_financial_abstract(symbol=sym)
        except Exception:
            continue
        result = _parse_financial_df(df)
        if result and any(v is not None for v in result.values()):
            return result
    return None


def enrich_financials(b_df: pd.DataFrame) -> pd.DataFrame:
    """逐只获取财务数据，补充 PE/PB/ROE 等字段"""
    print("\n正在逐只获取财务数据 (stock_financial_abstract) ...")
    total = len(b_df)

    eps_list, bvps_list, roe_list = [], [], []
    profit_list, debt_list, margin_list = [], [], []

    for i, (_, row) in enumerate(b_df.iterrows(), 1):
        symbol = str(row["代码"])
        if i % 10 == 0 or i == total:
            print(f"  进度: {i}/{total}")

        fin = fetch_financial_for_stock(symbol)
        if fin:
            eps_list.append(fin["eps"])
            bvps_list.append(fin["bvps"])
            roe_list.append(fin["roe"])
            profit_list.append(fin["net_profit"])
            debt_list.append(fin["debt_ratio"])
            margin_list.append(fin["gross_margin"])
        else:
            eps_list.append(None)
            bvps_list.append(None)
            roe_list.append(None)
            profit_list.append(None)
            debt_list.append(None)
            margin_list.append(None)
        time.sleep(0.15)

    b_df = b_df.copy()
    b_df["EPS"] = eps_list
    b_df["每股净资产"] = bvps_list
    b_df["ROE(%)"] = roe_list
    b_df["归母净利润"] = profit_list
    b_df["资产负债率(%)"] = debt_list
    b_df["毛利率(%)"] = margin_list

    price = pd.to_numeric(b_df["最新价"], errors="coerce")
    eps = pd.to_numeric(b_df["EPS"], errors="coerce")
    bvps = pd.to_numeric(b_df["每股净资产"], errors="coerce")
    b_df["PE(动)"] = np.where((eps > 0) & price.notna(), (price / eps).round(2), np.nan)
    b_df["PB"] = np.where((bvps > 0) & price.notna(), (price / bvps).round(2), np.nan)

    ok_count = sum(1 for e in eps_list if e is not None)
    print(f"  ✓ 成功获取 {ok_count}/{total} 只财务数据")
    return b_df


# ═══════════════════════════════════════════════════════════════════
#  名称清洗与 A 股匹配
# ═══════════════════════════════════════════════════════════════════


def _strip_b(name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    name = re.sub(r"Ｂ股$|B股$", "", name)
    name = re.sub(r"Ｂ$|B$", "", name)
    return name.strip()


def _strip_a(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"Ａ$|A$", "", name.strip()).strip()


def _strip_st(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"^\*?ST\s*", "", name.strip()).strip()


def _core(name: str) -> str:
    return _strip_st(_strip_b(_strip_a(name)))


def _raw_code(symbol: str) -> str:
    """sh900901 → 900901, sz200002 → 200002"""
    return re.sub(r"^[a-z]{2}", "", symbol.strip())


def match_a_shares(b_df: pd.DataFrame, a_df: pd.DataFrame) -> dict:
    """
    匹配B股与同公司A股。
    策略: ① 深圳 200xxx→000xxx 代码映射  ② 名称精确匹配  ③ 模糊子串匹配
    """
    print("\n正在匹配B股与A股对应关系 ...")

    a_by_code: dict[str, str] = {}
    a_by_core: dict[str, dict] = {}

    if not a_df.empty:
        for _, row in a_df.iterrows():
            code = str(row.get("代码", ""))
            name = str(row.get("名称", ""))
            a_by_code[code] = name
            core = _core(name)
            if core and len(core) >= 2:
                a_by_core[core] = {"code": code, "name": name}

    has_names = bool(a_by_core)
    if not has_names:
        print("  A股名录为空，仅使用深圳代码映射")

    result: dict[str, dict | None] = {}
    matched_count = 0

    for _, row in b_df.iterrows():
        symbol = str(row.get("代码", ""))
        b_name = str(row.get("名称", ""))
        raw = _raw_code(symbol)
        matched = None

        # ① 深圳代码映射 200xxx → 000xxx
        if raw.startswith("200"):
            potential = "000" + raw[3:]
            if potential in a_by_code:
                matched = {"a_code": potential, "a_name": a_by_code[potential], "method": "深圳代码"}
            elif not has_names:
                matched = {"a_code": potential, "a_name": f"({potential})", "method": "深圳代码(推断)"}

        # ② 名称精确匹配
        if matched is None and has_names:
            core_b = _core(b_name)
            if core_b and core_b in a_by_core:
                info = a_by_core[core_b]
                matched = {"a_code": info["code"], "a_name": info["name"], "method": "名称匹配"}

            # ③ 模糊子串匹配 (≥3字符)
            if matched is None and core_b and len(core_b) >= 3:
                best, best_len = None, 0
                for a_core, a_info in a_by_core.items():
                    if len(a_core) < 2:
                        continue
                    if core_b in a_core or a_core in core_b:
                        ml = min(len(core_b), len(a_core))
                        if ml > best_len:
                            best_len = ml
                            best = {"a_code": a_info["code"], "a_name": a_info["name"], "method": "模糊匹配"}
                if best and best_len >= 3:
                    matched = best

        result[symbol] = matched
        if matched:
            matched_count += 1

    print(f"  匹配结果: {matched_count}/{len(b_df)} 只B股有对应A股")
    for sym, m in result.items():
        if m:
            rows = b_df.loc[b_df["代码"] == sym, "名称"]
            bn = rows.iloc[0] if not rows.empty else sym
            print(f"    {bn}({_raw_code(sym)}) → {m['a_name']}({m['a_code']}) [{m['method']}]")
    return result


# ═══════════════════════════════════════════════════════════════════
#  梯队分类评分
# ═══════════════════════════════════════════════════════════════════


def classify(df: pd.DataFrame, has_financials: bool) -> pd.DataFrame:
    """
    多维打分分类：

    基础维度（始终可用）：
      有A股           +40
      ST风险           −20
      流动性(成交额)    高+10 / 低−15

    财务维度（--fast 模式下跳过）：
      PE (0~15 +20, 15~30 +10, >100 −5, 亏损EPS<0 −15)
      PB (0~1 +15, 1~2 +5, 净资产为负 −15)
      ROE (>8 +10, >15 +5额外)
      毛利率 (>30% +5)
      资产负债率 (>80% −10)

    分档:  Tier1: 有A股 且 score≥45  |  Tier2: score≥15  |  Tier3: 其余
    """
    tiers, scores, reasons_all = [], [], []

    for _, row in df.iterrows():
        has_a = bool(row.get("有A股", False))
        name = str(row.get("名称", ""))
        vol_amt = pd.to_numeric(row.get("成交额"), errors="coerce")
        vol_amt = 0.0 if pd.isna(vol_amt) else vol_amt

        score = 0
        parts: list[str] = []

        # ── A 股 ──
        if has_a:
            score += 40
            parts.append("有A股")

        # ── ST ──
        if re.search(r"ST", name):
            score -= 20
            parts.append("ST风险")

        # ── 流动性 (成交额, 单位: 元) ──
        if vol_amt >= 5_000_000:
            score += 10
            parts.append("流动性好")
        elif vol_amt >= 1_000_000:
            score += 5
        elif vol_amt < 200_000:
            score -= 15
            parts.append("流动性枯竭")
        elif vol_amt < 500_000:
            score -= 10
            parts.append("流动性差")

        # ── 财务维度 ──
        if has_financials:
            eps = pd.to_numeric(row.get("EPS"), errors="coerce")
            pe = pd.to_numeric(row.get("PE(动)"), errors="coerce")
            pb = pd.to_numeric(row.get("PB"), errors="coerce")
            roe = pd.to_numeric(row.get("ROE(%)"), errors="coerce")
            gm = pd.to_numeric(row.get("毛利率(%)"), errors="coerce")
            dr = pd.to_numeric(row.get("资产负债率(%)"), errors="coerce")
            bvps = pd.to_numeric(row.get("每股净资产"), errors="coerce")

            # PE
            if pd.notna(eps) and eps < 0:
                score -= 15
                parts.append("亏损")
            elif pd.notna(pe):
                if pe <= 15:
                    score += 20
                    parts.append(f"低PE({pe:.1f})")
                elif pe <= 30:
                    score += 10
                    parts.append(f"合理PE({pe:.1f})")
                elif pe <= 100:
                    score += 5
                else:
                    score -= 5
                    parts.append(f"高PE({pe:.0f})")

            # PB
            if pd.notna(bvps) and bvps <= 0:
                score -= 15
                parts.append("净资产为负")
            elif pd.notna(pb):
                if pb <= 1:
                    score += 15
                    parts.append(f"破净(PB{pb:.2f})")
                elif pb <= 2:
                    score += 5

            # ROE
            if pd.notna(roe):
                if roe > 15:
                    score += 15
                    parts.append(f"高ROE({roe:.1f}%)")
                elif roe > 8:
                    score += 10
                    parts.append(f"ROE({roe:.1f}%)")
                elif roe > 0:
                    score += 3

            # 毛利率
            if pd.notna(gm) and gm > 30:
                score += 5

            # 资产负债率
            if pd.notna(dr) and dr > 80:
                score -= 10
                parts.append(f"高负债({dr:.0f}%)")

        # ── 分档 ──
        if has_a and score >= 45:
            tier = 1
        elif score >= 15:
            tier = 2
        else:
            tier = 3

        tiers.append(tier)
        scores.append(score)
        reasons_all.append("; ".join(parts) if parts else "—")

    df = df.copy()
    df["梯队"] = tiers
    df["综合评分"] = scores
    df["分类依据"] = reasons_all
    return df


# ═══════════════════════════════════════════════════════════════════
#  报告输出
# ═══════════════════════════════════════════════════════════════════


def print_summary(df: pd.DataFrame) -> None:
    total = len(df)
    has_a = int(df["有A股"].sum())
    pure_b = total - has_a

    print(f"\n{'=' * 90}")
    print(f"  B股分化预测分析 —— 三梯队分类（2026年—2028年）")
    print(f"  分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 90}")

    print(f"\n【总体概况】")
    print(f"  B股总数: {total} 只")
    print(f"  有A股对应: {has_a} 只")
    print(f"  纯B股(无A股): {pure_b} 只")

    if "PE(动)" in df.columns:
        pe_valid = df["PE(动)"].dropna()
        if not pe_valid.empty:
            print(f"  PE中位数: {pe_valid.median():.1f} | 平均: {pe_valid.mean():.1f}")
    if "PB" in df.columns:
        pb_valid = df["PB"].dropna()
        broken = (pb_valid < 1).sum()
        if not pb_valid.empty:
            print(f"  破净股数: {broken}/{len(pb_valid)} 只 (PB<1)")

    print(f"\n【梯队分布】")
    for tier in [1, 2, 3]:
        meta = TIER_META[tier]
        count = int((df["梯队"] == tier).sum())
        print(f"  {meta['label']}: {count} 只 (预期{meta['target']})")
        print(f"    → {meta['desc']}")


def print_tier_detail(df: pd.DataFrame, tier: int) -> None:
    tier_df = df[df["梯队"] == tier].copy()
    if tier_df.empty:
        print(f"\n{TIER_META[tier]['label']}: 无股票")
        return

    meta = TIER_META[tier]
    tier_df = tier_df.sort_values("综合评分", ascending=False)

    print(f"\n  {meta['label']} ({len(tier_df)} 只):")
    names = [f"{_raw_code(r['代码'])} {r['名称']}" for _, r in tier_df.iterrows()]
    for i in range(0, len(names), 5):
        print(f"    {'、'.join(names[i:i+5])}")


def export_results(df: pd.DataFrame) -> str:
    os.makedirs("output", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"output/b_share_analysis_{ts}.csv"

    df_out = df.copy()
    df_out["代码6"] = df_out["代码"].apply(_raw_code)
    if "成交额" in df_out.columns:
        df_out["成交额(万)"] = (pd.to_numeric(df_out["成交额"], errors="coerce") / 1e4).round(1)

    export_cols = [
        "梯队", "代码6", "名称",
        "有A股", "对应A股", "A股匹配方式",
        "最新价", "涨跌幅",
        "PE(动)", "PB", "EPS", "每股净资产", "ROE(%)", "毛利率(%)", "资产负债率(%)",
        "成交量", "成交额(万)",
        "综合评分", "分类依据",
    ]
    export_cols = [c for c in export_cols if c in df_out.columns]

    df_sorted = df_out.sort_values(["梯队", "综合评分"], ascending=[True, False])
    df_sorted[export_cols].to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n完整数据已保存: {path}")
    return path


# ═══════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════


def main() -> None:
    parser = argparse.ArgumentParser(
        description="B股分化预测分析 —— 三梯队分类（2026年—2028年）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
梯队说明:
  第一梯队: 有A股母公司、基本面好 → B转A
  第二梯队: 优质纯B / 有H股基础 → B转H / 被吸并
  第三梯队: 绩差、无母公司、流动性枯竭 → 退市

示例:
  python app/query_b_share_analysis.py
  python app/query_b_share_analysis.py --fast
        """,
    )
    parser.add_argument("--fast", action="store_true",
                        help="快速模式：跳过逐只财务查询，仅用行情数据分类")
    parser.add_argument("--no-export", action="store_true",
                        help="不导出CSV")
    args = parser.parse_args()

    # ── 1. 获取B股行情 ──
    b_df = fetch_b_shares()
    if b_df.empty:
        print("无法获取B股数据，退出。")
        return

    # ── 2. 获取A股名录并匹配 ──
    try:
        a_df = fetch_a_code_name()
    except Exception as e:
        print(f"  A股名录获取异常({str(e)[:80]})，将仅用深圳代码映射")
        a_df = pd.DataFrame()

    a_matches = match_a_shares(b_df, a_df)

    b_df["有A股"] = b_df["代码"].map(lambda c: a_matches.get(c) is not None)
    b_df["对应A股"] = b_df["代码"].map(
        lambda c: f"{a_matches[c]['a_name']}({a_matches[c]['a_code']})" if a_matches.get(c) else ""
    )
    b_df["A股匹配方式"] = b_df["代码"].map(
        lambda c: a_matches[c]["method"] if a_matches.get(c) else ""
    )

    # ── 3. 财务数据补充 ──
    has_financials = False
    if not args.fast:
        b_df = enrich_financials(b_df)
        has_financials = True
    else:
        print("\n--fast 模式：跳过财务数据查询")

    # ── 4. 分类 ──
    print("\n正在进行梯队分类 ...")
    b_df = classify(b_df, has_financials)

    # ── 5. 输出 ──
    print_summary(b_df)
    for t in [1, 2, 3]:
        print_tier_detail(b_df, t)

    if not args.no_export:
        path = export_results(b_df)
        print(f"\n  详细数据请查看: {path}")

    print(f"\n{'=' * 90}")
    print("分析完成！提示: 分类结果基于实时行情 + 最新财报的量化评分，仅供参考。")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
