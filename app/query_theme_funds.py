"""
按主题关键词查询全市场基金（指数基金 + 主动基金），筛选排序后输出。

数据源（AkShare / 东方财富天天基金）：
- fund_info_index_em(symbol, indicator)：行业主题指数基金，含跟踪标的与收益率；
- fund_name_em()：全市场基金名录（代码、简称、类型），用于覆盖主动型基金；
- fund_open_fund_rank_em(symbol)：开放式基金排行，含近1年等收益率。

用法（在项目根目录）：
  # 默认查询消费主题
  python app/query_theme_funds.py

  # 自定义关键词
  python app/query_theme_funds.py --keywords 消费,食品,饮料,白酒,家电

  # 只看指数基金
  python app/query_theme_funds.py --mode index

  # 只看主动基金
  python app/query_theme_funds.py --mode active

  # 设置最小规模阈值（默认1亿，低于此值排除）
  python app/query_theme_funds.py --min-scale 2

  # 跳过规模过滤（设为 0）
  python app/query_theme_funds.py --min-scale 0

  # 跳过持仓过滤（不查持仓，但仍做规模过滤）
  python app/query_theme_funds.py --no-filter
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd
import requests

# 预置主题关键词组（可通过 --keywords 覆盖）
THEME_PRESETS: dict[str, list[str]] = {
    # 排除白酒
    "消费": ["消费", "食品", "饮料", "家电", "零售", "日用"],
    "医药": ["医药", "医疗", "生物", "创新药", "健康"],
    "科技": ["科技", "人工智能", "芯片", "半导体", "软件", "信息技术", "计算机"],
    "新能源": ["新能源", "光伏", "锂电", "电力", "碳中和", "储能"],
    "金融": ["金融", "银行", "证券", "保险", "券商"],
    "军工": ["军工", "国防"],
    "红利": ["红利", "高股息", "股息"],
}

DEFAULT_THEME = "消费"

# 持仓过滤名单：持仓中包含这些股票的基金将被排除
# 键为股票代码（6位），值为股票名称（仅用于打印说明）
STOCK_BLACKLIST: dict[str, str] = {
    # ── 白酒 ──
    "600519": "贵州茅台",
    "000858": "五粮液",
    "000568": "泸州老窖",
    "600809": "山西汾酒",
    "002304": "洋河股份",
    "000799": "酒鬼酒",
    "603369": "今世缘",
    "000596": "古井贡酒",
    "600779": "水井坊",
    "603589": "口子窖",
    "600559": "老白干酒",
    "000860": "顺鑫农业",
    "603198": "迎驾贡酒",
    "600199": "金种子酒",
    "600702": "舍得酒业",
    "603919": "金徽酒",
    # "600600": "青岛啤酒",  # 虽非白酒，但酒类基金常持有
}


def build_keyword_pattern(keywords: list[str]) -> str:
    return "|".join(keywords)


def _find_name_col(df: pd.DataFrame) -> str | None:
    """在 DataFrame 中定位基金名称列，优先 '基金简称'，其次含 '名称'/'简称' 的列。"""
    if "基金简称" in df.columns:
        return "基金简称"
    for c in df.columns:
        if "名称" in c or "简称" in c:
            return c
    return None


# ── 持仓过滤 ──


def _normalize_stock_code(code) -> str:
    """统一为 6 位股票代码。"""
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


def _fetch_fund_basic(fund_code: str) -> tuple[float | None, str | None]:
    """
    查询单只基金最新规模（亿元）和成立时间。
    优先用 akshare 雪球接口，失败时回退到天天基金页面解析。
    返回 (规模, 成立时间)，无法获取的字段为 None。
    """
    scale: float | None = None
    inception: str | None = None

    # 方式1: akshare 雪球接口
    try:
        info = ak.fund_individual_basic_info_xq(symbol=fund_code)
        if info is not None and not info.empty:
            for _, row in info.iterrows():
                item = str(row.iloc[0]) if len(row) > 0 else ""
                val = str(row.iloc[1]) if len(row) > 1 else ""
                if "规模" in item and val and scale is None:
                    num = re.sub(r"[^\d.]", "", val)
                    if num:
                        scale = float(num)
                        if "万" in val and "亿" not in val:
                            scale = scale / 10000
                if "成立" in item and val and inception is None:
                    date_m = re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", val)
                    if date_m:
                        inception = date_m.group(0)
            if scale is not None:
                return scale, inception
    except Exception:
        pass

    # 方式2: 天天基金概况页
    try:
        url = f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = r.apparent_encoding or "utf-8"
        m = re.search(r"资产规模[：:]\s*([\d.]+)\s*亿", r.text)
        if m and scale is None:
            scale = float(m.group(1))
        if inception is None:
            dm = re.search(r"成立日期[/成立时间]*[：:]\s*(\d{4}[-/]\d{2}[-/]\d{2})", r.text)
            if dm:
                inception = dm.group(1)
    except Exception:
        pass

    return scale, inception


MIN_FUND_SCALE = 1.0  # 最小基金规模（亿元），低于此值的基金将被排除


def filter_by_holdings_and_scale(
    df: pd.DataFrame,
    blacklist: dict[str, str],
    min_scale: float = MIN_FUND_SCALE,
) -> pd.DataFrame:
    """
    逐只查询基金，同时做两项过滤：
    1. 持仓黑名单：持仓中包含黑名单股票的基金被排除；
    2. 规模过滤：最新规模低于 min_scale（亿元）的基金被排除。
    合并为一次循环，避免重复请求。
    """
    if df.empty:
        return df

    code_col = "基金代码" if "基金代码" in df.columns else None
    name_col = "基金简称" if "基金简称" in df.columns else None
    if code_col is None:
        print("  过滤跳过：无基金代码列")
        return df

    year = str(datetime.now().year - 1)

    bl_codes = set(blacklist.keys()) if blacklist else set()
    if bl_codes:
        bl_names_str = ", ".join(f"{v}({k})" for k, v in list(blacklist.items())[:8])
        print(f"\n正在逐只查询，过滤持仓黑名单 + 规模 < {min_scale}亿 的基金 ...")
        print(f"  黑名单({len(blacklist)}只): {bl_names_str} ...")
    else:
        print(f"\n正在逐只查询，过滤规模 < {min_scale}亿 的基金 ...")

    # 预构建"近1年"值映射，无值的基金跳过持仓查询
    perf_col = "近1年"
    has_perf_map: dict[str, bool] = {}
    if perf_col in df.columns:
        for fc_tmp in df[code_col].unique():
            vals = df.loc[df[code_col] == fc_tmp, perf_col]
            has_perf_map[fc_tmp] = vals.notna().any()

    fund_codes = df[code_col].unique().tolist()
    total = len(fund_codes)
    exclude_codes: set[str] = set()
    exclude_holding_details: list[str] = []
    exclude_scale_details: list[str] = []
    scale_map: dict[str, float | None] = {}
    inception_map: dict[str, str | None] = {}
    holdings_map: dict[str, str] = {}
    checked = 0
    skipped = 0
    skipped_no_perf = 0

    for i, fc in enumerate(fund_codes, 1):
        if i % 20 == 0 or i == total:
            print(f"  进度: {i}/{total} (已排除 {len(exclude_codes)} 只)")

        fund_name = ""
        if name_col:
            names = df.loc[df[code_col] == fc, name_col]
            fund_name = names.iloc[0] if not names.empty else ""

        # 近1年业绩无值 → 跳过持仓查询和规模过滤，只展示基本信息
        if has_perf_map and not has_perf_map.get(fc, True):
            skipped_no_perf += 1
            continue

        # 规模 + 成立时间
        scale, inception = _fetch_fund_basic(fc)
        scale_map[fc] = scale
        inception_map[fc] = inception
        if scale is not None and scale < min_scale:
            exclude_codes.add(fc)
            exclude_scale_details.append(f"  ✗ {fund_name}({fc}) 规模: {scale:.2f}亿")
            checked += 1
            continue

        # 查询持仓（同时用于黑名单检查和提取前十持仓）
        try:
            holdings = ak.fund_portfolio_hold_em(symbol=fc, date=year)
        except Exception:
            skipped += 1
            continue

        if holdings is None or holdings.empty:
            skipped += 1
            continue

        if "季度" in holdings.columns:
            q = holdings["季度"].max()
            holdings = holdings[holdings["季度"] == q]

        h_code_col = "股票代码" if "股票代码" in holdings.columns else holdings.columns[0]
        h_name_col = "股票名称" if "股票名称" in holdings.columns else None

        # 提取前十重仓股名称
        if h_name_col:
            top_names = holdings[h_name_col].head(10).tolist()
            holdings_map[fc] = "、".join(str(n) for n in top_names if pd.notna(n))
        else:
            top_codes = holdings[h_code_col].head(10).tolist()
            holdings_map[fc] = "、".join(str(c) for c in top_codes if pd.notna(c))

        # 持仓黑名单检查：持有超过1只黑名单股票则排除
        if bl_codes:
            held_codes = {_normalize_stock_code(c) for c in holdings[h_code_col]}
            hit = held_codes & bl_codes
            if len(hit) > 1:
                exclude_codes.add(fc)
                hit_names = [f"{blacklist[c]}" for c in hit]
                exclude_holding_details.append(f"  ✗ {fund_name}({fc}) 持有{len(hit)}只黑名单: {', '.join(hit_names)}")

        checked += 1

    # 汇总打印
    parts = [f"检查 {checked} 只", f"跳过(无持仓) {skipped} 只"]
    if skipped_no_perf:
        parts.append(f"跳过(近1年无值) {skipped_no_perf} 只")
    parts.append(f"排除 {len(exclude_codes)} 只")
    print(f"\n过滤完成: {', '.join(parts)}")
    if exclude_scale_details:
        print(f"\n规模不足 {min_scale}亿 排除 ({len(exclude_scale_details)} 只):")
        for line in exclude_scale_details:
            print(line)
    if exclude_holding_details:
        print(f"\n持仓含黑名单排除 ({len(exclude_holding_details)} 只):")
        for line in exclude_holding_details:
            print(line)

    filtered = df[~df[code_col].isin(exclude_codes)].copy()

    if scale_map:
        filtered["基金规模(亿)"] = filtered[code_col].map(scale_map)
    if holdings_map:
        filtered["前十持仓"] = filtered[code_col].map(holdings_map)
    if inception_map:
        filtered["成立时间"] = filtered[code_col].map(inception_map)

    return filtered


def filter_by_scale_only(
    df: pd.DataFrame,
    min_scale: float = MIN_FUND_SCALE,
) -> pd.DataFrame:
    """仅做规模过滤（--no-filter 模式下调用，不查持仓）。"""
    if df.empty:
        return df

    code_col = "基金代码" if "基金代码" in df.columns else None
    name_col = "基金简称" if "基金简称" in df.columns else None
    if code_col is None:
        return df

    print(f"\n正在逐只查询基金规模，过滤规模 < {min_scale}亿 ...")

    fund_codes = df[code_col].unique().tolist()
    total = len(fund_codes)
    exclude_codes: set[str] = set()
    exclude_details: list[str] = []
    scale_map: dict[str, float | None] = {}
    inception_map: dict[str, str | None] = {}

    for i, fc in enumerate(fund_codes, 1):
        if i % 20 == 0 or i == total:
            print(f"  进度: {i}/{total} (已排除 {len(exclude_codes)} 只)")

        fund_name = ""
        if name_col:
            names = df.loc[df[code_col] == fc, name_col]
            fund_name = names.iloc[0] if not names.empty else ""

        scale, inception = _fetch_fund_basic(fc)
        scale_map[fc] = scale
        inception_map[fc] = inception
        if scale is not None and scale < min_scale:
            exclude_codes.add(fc)
            exclude_details.append(f"  ✗ {fund_name}({fc}) 规模: {scale:.2f}亿")

    if exclude_details:
        print(f"\n规模不足 {min_scale}亿 排除 ({len(exclude_details)} 只):")
        for line in exclude_details:
            print(line)

    filtered = df[~df[code_col].isin(exclude_codes)].copy()
    if scale_map:
        filtered["基金规模(亿)"] = filtered[code_col].map(scale_map)
    if inception_map:
        filtered["成立时间"] = filtered[code_col].map(inception_map)

    return filtered


# ── 指数基金查询 ──


def query_index_funds(keywords: list[str]) -> pd.DataFrame:
    """查询行业主题指数基金并按关键词筛选。"""
    print("正在查询行业主题指数基金 (fund_info_index_em) ...")
    try:
        df = ak.fund_info_index_em(symbol="行业主题", indicator="全部")
    except Exception as e:
        print(f"  查询失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        print("  未获取到数据")
        return pd.DataFrame()

    name_col = _find_name_col(df)
    if name_col is None:
        print(f"  无法定位基金名称列，现有列: {list(df.columns)}")
        return pd.DataFrame()

    # 统一列名为"基金简称"，确保与全市场数据合并后不出现 NaN
    if name_col != "基金简称":
        df = df.rename(columns={name_col: "基金简称"})
        name_col = "基金简称"

    pattern = build_keyword_pattern(keywords)
    matched = df[df[name_col].str.contains(pattern, na=False)].copy()

    # 按名称排除明显白酒/酒类基金
    exclude_name_kw = ["白酒", "酒ETF", "中证酒"]
    if exclude_name_kw:
        ex_pattern = "|".join(exclude_name_kw)
        name_excluded = matched[name_col].str.contains(ex_pattern, na=False)
        if name_excluded.any():
            print(f"  按名称排除白酒类基金 {name_excluded.sum()} 只")
            matched = matched[~name_excluded]

    matched.insert(0, "来源", "指数基金")
    print(f"  行业主题指数基金总计 {len(df)} 只，匹配关键词 {len(matched)} 只")
    return matched


# ── 主动基金查询 ──


def query_active_funds(keywords: list[str]) -> pd.DataFrame:
    """
    从全量基金名录中筛选主动型消费基金（排除已被指数基金覆盖的部分）。
    再尝试通过 fund_open_fund_info_em 拉取收益率数据进行合并。
    """
    print("正在查询全市场基金名录 (fund_name_em) ...")
    try:
        all_funds = ak.fund_name_em()
    except Exception as e:
        print(f"  查询失败: {e}")
        return pd.DataFrame()

    if all_funds is None or all_funds.empty:
        print("  未获取到数据")
        return pd.DataFrame()

    name_col = _find_name_col(all_funds)
    if name_col is None:
        print(f"  无法定位基金名称列，现有列: {list(all_funds.columns)}")
        return pd.DataFrame()

    if name_col != "基金简称":
        all_funds = all_funds.rename(columns={name_col: "基金简称"})
        name_col = "基金简称"

    pattern = build_keyword_pattern(keywords)
    matched = all_funds[all_funds[name_col].str.contains(pattern, na=False)].copy()
    print(f"  全市场基金总计 {len(all_funds)} 只，匹配关键词 {len(matched)} 只")

    # 按名称排除明显白酒/酒类基金
    exclude_name_kw = ["白酒", "酒ETF", "中证酒"]
    if exclude_name_kw:
        ex_pattern = "|".join(exclude_name_kw)
        name_excluded = matched[name_col].str.contains(ex_pattern, na=False)
        if name_excluded.any():
            print(f"  按名称排除白酒类基金 {name_excluded.sum()} 只")
            matched = matched[~name_excluded]

    # 排除货币型、短期理财等类型
    if "基金类型" in matched.columns:
        exclude_types = ["货币型", "短期理财", "理财型", "Reits", "REITs"]
        before = len(matched)
        matched = matched[~matched["基金类型"].str.contains("|".join(exclude_types), na=False)]
        excluded = before - len(matched)
        if excluded:
            print(f"  排除货币/理财/REITs型 {excluded} 只，剩余 {len(matched)} 只")

    matched.insert(0, "来源", "全市场")
    return matched


def try_enrich_with_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """尝试用开放式基金排行 + 场内ETF排行数据补充收益率字段。"""
    if df.empty:
        return df
    code_col = "基金代码" if "基金代码" in df.columns else None
    if code_col is None:
        return df

    perf_fields = ["单位净值", "日期", "日增长率", "近1周", "近1月", "近3月", "近1年", "近2年", "近3年", "今年来", "成立来"]

    # 1) 开放式基金排行（覆盖场外基金）
    print("正在查询开放式基金排行以补充收益率 (fund_open_fund_rank_em) ...")
    rank_df = None
    try:
        rank_df = ak.fund_open_fund_rank_em(symbol="全部")
    except Exception as e:
        print(f"  排行数据查询失败（不影响主结果）: {e}")

    if rank_df is not None and not rank_df.empty and "基金代码" in rank_df.columns:
        merge_cols = ["基金代码"]
        for c in perf_fields:
            if c in rank_df.columns and c not in df.columns:
                merge_cols.append(c)
        if len(merge_cols) > 1:
            df = df.merge(rank_df[merge_cols], on="基金代码", how="left")
            print(f"  开放式排行匹配 {df[merge_cols[1]].notna().sum()} / {len(df)} 只")

    # 2) 场内ETF排行（覆盖未被开放式排行覆盖的ETF）
    has_perf = [c for c in perf_fields if c in df.columns]
    if has_perf:
        missing_mask = df[has_perf].isna().all(axis=1)
    else:
        missing_mask = pd.Series(True, index=df.index)
    missing_count = missing_mask.sum()

    if missing_count > 0:
        print(f"  尚有 {missing_count} 只无业绩数据，尝试场内ETF排行 (fund_exchange_rank_em) ...")
        try:
            etf_rank = ak.fund_exchange_rank_em()
        except Exception as e:
            print(f"  ETF排行查询失败（不影响主结果）: {e}")
            etf_rank = None

        if etf_rank is not None and not etf_rank.empty and "基金代码" in etf_rank.columns:
            etf_cols = ["基金代码"]
            for c in perf_fields:
                if c in etf_rank.columns:
                    etf_cols.append(c)
            if len(etf_cols) > 1:
                etf_sub = etf_rank[etf_cols].copy()
                fill_cols = [c for c in etf_cols if c != "基金代码"]
                etf_sub = etf_sub.rename(columns={c: f"_etf_{c}" for c in fill_cols})

                df = df.merge(etf_sub, on="基金代码", how="left")
                for c in fill_cols:
                    etf_c = f"_etf_{c}"
                    if c in df.columns:
                        df[c] = df[c].fillna(df[etf_c])
                    else:
                        df[c] = df[etf_c]
                    df.drop(columns=[etf_c], inplace=True)

                filled = 0
                if has_perf or fill_cols:
                    check_col = has_perf[0] if has_perf else fill_cols[0]
                    filled = missing_count - df.loc[missing_mask, check_col].isna().sum()
                print(f"  ETF排行补充 {filled} 只")

    return df


# ── 去重同名 C/D/E/Y 份额 ──


_SHARE_SUFFIX_RE = re.compile(r"[A-EY]$")


def _base_fund_name(name: str) -> str:
    """
    去除基金简称末尾的份额后缀(A/B/C/D/E/Y)得到基名，用于判断同基金不同份额。
    例: "平安消费精选混合C" → "平安消费精选混合"
        "汇添富新兴消费股票D" → "汇添富新兴消费股票"
    """
    if not name:
        return name
    return _SHARE_SUFFIX_RE.sub("", name.strip())


def dedup_fund_shares(df: pd.DataFrame) -> pd.DataFrame:
    """
    同一只基金的 A/C/D/E/Y 份额只保留 A 份额（若无 A 则保留首条）。
    指数基金按手续费判断：手续费为 0 的视为 C 份额。
    """
    if df.empty:
        return df

    name_col = "基金简称" if "基金简称" in df.columns else None

    # 指数基金部分往往无基金简称，用手续费区分：手续费=0 的是 C 份额
    if "手续费" in df.columns:
        fee_col = df["手续费"]
        fee_numeric = pd.to_numeric(fee_col, errors="coerce")
        is_c_by_fee = (fee_numeric == 0) & (df.get("来源", "") == "指数基金")
        c_by_fee_count = is_c_by_fee.sum()
        if c_by_fee_count > 0:
            df = df[~is_c_by_fee].copy()
            print(f"  按手续费去重指数基金 C 份额 {c_by_fee_count} 只")

    if name_col is None:
        return df

    # 全市场部分按名称去重
    market_mask = df["来源"] == "全市场" if "来源" in df.columns else pd.Series(True, index=df.index)
    market_df = df[market_mask].copy()
    other_df = df[~market_mask].copy()

    if market_df.empty:
        return df

    market_df["_base"] = market_df[name_col].apply(_base_fund_name)
    market_df["_suffix"] = market_df[name_col].str.strip().str[-1:]
    market_df["_is_a"] = market_df["_suffix"] == "A"

    # 对每组：优先保留 A 份额，否则保留第一条
    keep_idx = []
    for base, group in market_df.groupby("_base", sort=False):
        if len(group) == 1:
            keep_idx.append(group.index[0])
            continue
        a_rows = group[group["_is_a"]]
        if not a_rows.empty:
            keep_idx.append(a_rows.index[0])
        else:
            keep_idx.append(group.index[0])

    before = len(market_df)
    market_df = market_df.loc[keep_idx].drop(columns=["_base", "_suffix", "_is_a"])
    removed = before - len(market_df)
    if removed:
        print(f"  去重同名 C/D/E/Y 份额 {removed} 只（保留 A 份额）")

    result = pd.concat([other_df, market_df], ignore_index=True)
    return result


# ── 输出 ──


def display_and_save(
    df: pd.DataFrame,
    keywords: list[str],
    theme_name: str,
) -> None:
    if df.empty:
        print("\n未找到匹配的基金。")
        return

    sort_col = "近1年"
    if sort_col in df.columns:
        df[sort_col] = pd.to_numeric(df[sort_col], errors="coerce")
        df = df.sort_values(sort_col, ascending=False, na_position="last")

    display_cols = []
    for c in ["来源", "基金代码", "基金简称", "基金类型", "基金规模(亿)", "单位净值", "日增长率",
              "近1周", "近1月", "近3月", "近1年", "近2年", "近3年",
              "今年来", "成立来", "前十持仓", "成立时间"]:
        if c in df.columns:
            display_cols.append(c)
    if not display_cols:
        display_cols = list(df.columns)

    print(f"\n{'=' * 100}")
    print(f"【{theme_name}主题基金查询结果】关键词: {', '.join(keywords)}")
    suffix = f"（按 {sort_col} 降序）" if sort_col in df.columns else ""
    print(f"  共 {len(df)} 只{suffix}")
    print(f"{'=' * 100}")

    show = df[display_cols]
    pd.set_option("display.max_colwidth", 30)
    pd.set_option("display.width", 200)
    print(show.to_string(index=False))

    # 按来源分类统计
    if "来源" in df.columns:
        print(f"\n【来源分布】")
        print(df["来源"].value_counts().to_string())

    if "基金类型" in df.columns:
        print(f"\n【类型分布】")
        print(df["基金类型"].value_counts().to_string())

    os.makedirs("output", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"output/theme_funds_{theme_name}_{ts}.csv"
    df[display_cols].to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n完整数据已保存: {path}")


# ── 主流程 ──


def main() -> None:
    parser = argparse.ArgumentParser(
        description="按主题关键词查询全市场基金（指数基金 + 主动基金）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
预置主题: """ + ", ".join(THEME_PRESETS.keys()) + """

示例:
  python app/query_theme_funds.py                        # 默认查询消费主题
  python app/query_theme_funds.py --theme 医药            # 使用预置医药关键词
  python app/query_theme_funds.py --keywords 消费,白酒    # 自定义关键词
  python app/query_theme_funds.py --mode index            # 仅查指数基金
  python app/query_theme_funds.py --mode active           # 仅查主动基金
        """,
    )
    parser.add_argument(
        "--theme", type=str, default=DEFAULT_THEME,
        help=f"预置主题名称（{', '.join(THEME_PRESETS.keys())}），默认: {DEFAULT_THEME}",
    )
    parser.add_argument(
        "--keywords", type=str, default="",
        help="自定义关键词，逗号分隔（覆盖 --theme）",
    )
    parser.add_argument(
        "--mode", type=str, choices=["all", "index", "active"], default="all",
        help="查询模式: all=指数+主动, index=仅指数基金, active=仅主动基金（默认 all）",
    )
    parser.add_argument(
        "--min-scale", type=float, default=MIN_FUND_SCALE,
        help=f"最小基金规模（亿元），低于此值排除（默认 {MIN_FUND_SCALE}）",
    )
    parser.add_argument(
        "--no-filter", action="store_true",
        help="跳过持仓过滤（不逐只查持仓），但仍做规模过滤",
    )
    args = parser.parse_args()

    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
        theme_name = "自定义"
    else:
        theme_name = args.theme
        keywords = THEME_PRESETS.get(theme_name)
        if not keywords:
            print(f"未知主题: {theme_name}，可用: {', '.join(THEME_PRESETS.keys())}")
            sys.exit(1)

    print(f"主题: {theme_name}")
    print(f"关键词: {', '.join(keywords)}")
    print(f"模式: {args.mode}")
    print("=" * 100)

    dfs: list[pd.DataFrame] = []

    if args.mode in ("all", "index"):
        idx_df = query_index_funds(keywords)
        if not idx_df.empty:
            dfs.append(idx_df)

    if args.mode in ("all", "active"):
        active_df = query_active_funds(keywords)
        if not active_df.empty:
            # 如果同时查了指数基金，去掉主动查询中的重复项
            if dfs and "基金代码" in active_df.columns:
                idx_codes = set()
                for d in dfs:
                    if "基金代码" in d.columns:
                        idx_codes.update(d["基金代码"].tolist())
                before = len(active_df)
                active_df = active_df[~active_df["基金代码"].isin(idx_codes)]
                deduped = before - len(active_df)
                if deduped:
                    print(f"  去重（已在指数基金中出现）{deduped} 只，剩余 {len(active_df)} 只")

            active_df = try_enrich_with_ranking(active_df)
            dfs.append(active_df)

    if not dfs:
        print("\n未查询到任何匹配基金。")
        return

    combined = pd.concat(dfs, ignore_index=True)

    # 去重同名 C/D/E/Y 份额，只保留 A 份额
    combined = dedup_fund_shares(combined)

    # 统一数值列（提前转换，供业绩过滤使用）
    for col in ["近1周", "近1月", "近3月", "近1年", "近2年", "近3年", "今年来", "成立来", "日增长率"]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # 排除成立来业绩为负的基金（在逐只查询前做，减少请求量）
    if "成立来" in combined.columns:
        before = len(combined)
        combined = combined[~(combined["成立来"] < 0)].copy()
        excluded = before - len(combined)
        if excluded:
            print(f"\n排除成立来业绩为负 {excluded} 只，剩余 {len(combined)} 只")

    # 逐只过滤：持仓黑名单 + 规模
    min_scale = args.min_scale
    if not args.no_filter:
        before = len(combined)
        combined = filter_by_holdings_and_scale(
            combined, STOCK_BLACKLIST, min_scale=min_scale,
        )
        after = len(combined)
        if before != after:
            print(f"\n过滤汇总: {before} → {after} 只（排除 {before - after} 只）")
    else:
        print("\n已跳过持仓过滤（--no-filter）")
        if min_scale > 0:
            before = len(combined)
            combined = filter_by_scale_only(combined, min_scale=min_scale)
            after = len(combined)
            if before != after:
                print(f"\n规模过滤: {before} → {after} 只（排除 {before - after} 只）")

    display_and_save(combined, keywords, theme_name)


if __name__ == "__main__":
    main()
