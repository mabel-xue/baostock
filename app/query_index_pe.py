"""
根据指数查询历史市盈率（以滚动市盈率为主）。

数据源（AkShare）：
- 中证指数：stock_zh_index_hist_csindex(indexCode)，返回列「滚动市盈率」等；
  适用于中证发布的代码（如 000300、399987、H30590、930707）。
- 乐咕乐股：乐咕 API 的 date 现为 ISO 字符串，AkShare 的 stock_index_pe_lg 仍按毫秒解析会报错；
  本脚本内 fetch_pe_legulegu 已做兼容解析。

用法（在项目根目录）：
  python app/query_index_pe.py --csindex 000300 --start 20230101
  python app/query_index_pe.py --lg 深证红利
  python app/query_index_pe.py --tracking 深证红利指数
  python app/query_index_pe.py --etf-routes          # 按 MY_ETFS 已配置路由拉最新 PE

加仓提醒（INDEX_ADD_ALERT_PE）：
- 键为与 MY_ETFS 第三列一致的「跟踪指数全称」；
- 规则：最新「滚动市盈率」视为动态市盈率，当 最新滚动市盈率 <= 提醒点 时视为「触及」，输出操作建议（非投资建议）。
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd
import py_mini_racer
import requests

from akshare.stock_feature.stock_a_indicator import get_cookie_csrf
from akshare.stock_feature.stock_a_pe_and_pb import hash_code as _LG_HASH_CODE

# 与 query_my_funds.MY_ETFS 第三列「跟踪指数名称」对齐：("csindex"|"lg", 参数, 说明)
INDEX_PE_ROUTES: dict[str, tuple[str, str, str]] = {
    "标普中国新经济行业指数": ("none", "", "标普系，中证/乐咕无直接序列"),
    "深证红利指数": ("lg", "深证红利", ""),
    "中证酒指数": ("csindex", "399987", ""),
    "中证畜牧养殖指数": ("csindex", "930707", ""),
    "恒生互联网科技业指数": (
        "csindex",
        "H30533",
        "市盈率取中证H30533(中国互联网50等)作近似，与恒生互联网科技业指数非同一标的",
    ),
    "纳斯达克100指数": ("none", "", "境外指数"),
    "东京日经225指数": ("none", "", "境外指数"),
    "恒生中国企业指数": ("none", "", "恒生指数公司"),
    "中证全指证券公司指数": ("csindex", "399975", ""),
    "上证科创板50成份指数": ("csindex", "000688", ""),
    "MSCI中国A50互联互通人民币指数": ("none", "", "请用行情终端或 MSCI/中证披露；本接口未配置代码"),
    "中证机器人指数": ("csindex", "H30590", ""),
    "中证全指电力公用事业指数": ("csindex", "H30199", ""),
    "创业板人工智能指数": ("none", "", "国证 970070 等，中证 perf 接口无此代码；可查国证官网或第三方"),
}

# 加仓提醒点：跟踪指数全称 -> 滚动市盈率阈值（最新 PE <= 阈值视为触及）
INDEX_ADD_ALERT_PE: dict[str, float] = {
    "深证红利指数": 15.0,
}

# 乐咕简称 -> 跟踪指数全称（用于 --lg 单查时匹配提醒点）
LEGULEGU_SHORT_TO_TRACKING_NAME: dict[str, str] = {
    "深证红利": "深证红利指数",
}


def fetch_pe_csindex(
    index_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """中证指数历史行情，含滚动市盈率。"""
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    if start_date is None:
        start_date = "20180101"
    df = ak.stock_zh_index_hist_csindex(
        symbol=index_code.strip(),
        start_date=start_date,
        end_date=end_date,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    cols = [
        c
        for c in [
            "日期",
            "指数代码",
            "指数中文简称",
            "收盘",
            "滚动市盈率",
        ]
        if c in df.columns
    ]
    return df[cols].copy()


# 与 AkShare stock_index_pe_lg 一致（乐咕 index-basic-pe 的 indexCode）
LEGULEGU_INDEX_SYMBOL_MAP: dict[str, str] = {
    "上证50": "000016.SH",
    "沪深300": "000300.SH",
    "上证380": "000009.SH",
    "创业板50": "399673.SZ",
    "中证500": "000905.SH",
    "上证180": "000010.SH",
    "深证红利": "399324.SZ",
    "深证100": "399330.SZ",
    "中证1000": "000852.SH",
    "上证红利": "000015.SH",
    "中证100": "000903.SH",
    "中证800": "000906.SH",
}


def _normalize_legulegu_dates(series: pd.Series) -> pd.Series:
    """乐咕返回的 date 可能是毫秒时间戳或 'YYYY-MM-DD' 字符串。"""
    if series.empty:
        return series
    first = series.iloc[0]
    if isinstance(first, (int, float)) or (
        isinstance(first, str) and first.isdigit()
    ):
        return (
            pd.to_datetime(series, unit="ms", utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.normalize()
        )
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def fetch_pe_legulegu(short_name: str) -> pd.DataFrame:
    """
    乐咕 index-basic-pe：等权/滚动/中位数等多口径 PE。
    不依赖 AkShare 内建的毫秒日期解析（与当前乐咕 JSON 格式兼容）。
    """
    name = short_name.strip()
    code = LEGULEGU_INDEX_SYMBOL_MAP.get(name)
    if not code:
        print(f"  [乐咕] 未支持的指数简称: {name}", file=sys.stderr)
        return pd.DataFrame()
    try:
        js = py_mini_racer.MiniRacer()
        js.eval(_LG_HASH_CODE)
        token = js.call("hex", datetime.now().date().isoformat()).lower()
        url = "https://legulegu.com/api/stockdata/index-basic-pe"
        params = {"token": token, "indexCode": code}
        r = requests.get(
            url,
            params=params,
            timeout=30,
            **get_cookie_csrf(url="https://legulegu.com/stockdata/sz50-ttm-lyr"),
        )
        r.raise_for_status()
        data_json = r.json()
        rows = data_json.get("data") or []
        if not rows:
            return pd.DataFrame()
        temp_df = pd.DataFrame(rows)
        temp_df["date"] = _normalize_legulegu_dates(temp_df["date"]).dt.date
        temp_df = temp_df[
            [
                "date",
                "close",
                "lyrPe",
                "addLyrPe",
                "middleLyrPe",
                "ttmPe",
                "addTtmPe",
                "middleTtmPe",
            ]
        ]
        temp_df.columns = [
            "日期",
            "指数",
            "等权静态市盈率",
            "静态市盈率",
            "静态市盈率中位数",
            "等权滚动市盈率",
            "滚动市盈率",
            "滚动市盈率中位数",
        ]
        return temp_df
    except (requests.RequestException, KeyError, ValueError, TypeError, OSError) as e:
        print(f"  [乐咕] {name} 拉取失败: {e}", file=sys.stderr)
        return pd.DataFrame()


def summarize_dynamic_pe_interval(df: pd.DataFrame) -> dict[str, object]:
    """
    基于当前 DataFrame 内「滚动市盈率」列（视作动态/TTM 市盈率）汇总区间与中位数。
    乐咕数据含「滚动市盈率中位数」时，取最后一期作为最新动态市盈率中位数；中证序列无该列则为空。
    记录区间与最大/最小统计均基于有效滚动市盈率样本对应的日期范围。
    """
    out: dict[str, object] = {
        "记录区间": None,
        "区间最大动态市盈率": None,
        "区间最小动态市盈率": None,
        "最新动态市盈率中位数": None,
    }
    if df is None or df.empty or "日期" not in df.columns:
        return out
    if "滚动市盈率" not in df.columns:
        return out
    work = df.copy()
    work["_dt"] = pd.to_datetime(work["日期"], errors="coerce")
    work["滚动市盈率"] = pd.to_numeric(work["滚动市盈率"], errors="coerce")
    valid = work.dropna(subset=["滚动市盈率"])
    if valid.empty:
        if work["_dt"].notna().any():
            out["记录区间"] = f"{work['_dt'].min().date()} ～ {work['_dt'].max().date()}"
        return out
    out["记录区间"] = f"{valid['_dt'].min().date()} ～ {valid['_dt'].max().date()}"
    pe = valid["滚动市盈率"]
    out["区间最大动态市盈率"] = float(pe.max())
    out["区间最小动态市盈率"] = float(pe.min())
    if "滚动市盈率中位数" in df.columns:
        med_series = pd.to_numeric(df["滚动市盈率中位数"], errors="coerce")
        last_med = med_series.iloc[-1]
        if pd.notna(last_med):
            out["最新动态市盈率中位数"] = float(last_med)
    return out


def resolve_add_alert_threshold(
    tracking_index_name: str | None = None,
    legulegu_short: str | None = None,
) -> float | None:
    """根据跟踪指数全称或乐咕简称解析加仓提醒点。"""
    if tracking_index_name and tracking_index_name.strip() in INDEX_ADD_ALERT_PE:
        return INDEX_ADD_ALERT_PE[tracking_index_name.strip()]
    if legulegu_short:
        full = LEGULEGU_SHORT_TO_TRACKING_NAME.get(legulegu_short.strip())
        if full and full in INDEX_ADD_ALERT_PE:
            return INDEX_ADD_ALERT_PE[full]
    return None


def add_position_alert_fields(
    latest_pe: float | None,
    *,
    tracking_index_name: str | None = None,
    legulegu_short: str | None = None,
) -> dict[str, object]:
    """
    生成加仓提醒相关列：加仓提醒点、是否触及加仓提醒、操作建议。
    触及条件：最新滚动市盈率 <= 加仓提醒点（估值回落至设定区间）。
    """
    th = resolve_add_alert_threshold(
        tracking_index_name=tracking_index_name,
        legulegu_short=legulegu_short,
    )
    empty = {
        "加仓提醒点": None,
        "是否触及加仓提醒": "",
        "操作建议": "",
    }
    if th is None:
        return empty
    empty["加仓提醒点"] = float(th)
    if latest_pe is None:
        empty["操作建议"] = "暂无最新滚动市盈率，无法与加仓提醒点比较。"
        return empty
    pe = float(latest_pe)
    if pe <= th:
        empty["是否触及加仓提醒"] = "是"
        empty["操作建议"] = (
            f"最新滚动市盈率({pe:.2f})已低于或等于加仓提醒点({th:.2f})，"
            "触及你设定的加仓区间；可按纪律考虑分批加仓或加大定投，并遵守单品种/权益总仓位上限。"
            "（程序提示，不构成投资建议。）"
        )
    else:
        empty["是否触及加仓提醒"] = "否"
        empty["操作建议"] = (
            f"最新滚动市盈率({pe:.2f})高于加仓提醒点({th:.2f})，未触及加仓条件；"
            "可维持现有节奏或观望。（程序提示，不构成投资建议。）"
        )
    return empty


def fetch_by_tracking_name(
    tracking_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[pd.DataFrame, str]:
    """
    按「跟踪指数全称」解析路由并拉取。
    返回 (DataFrame, 说明备注)
    """
    route = INDEX_PE_ROUTES.get(tracking_name.strip())
    if not route:
        return pd.DataFrame(), f"未在 INDEX_PE_ROUTES 中配置: {tracking_name}"
    provider, param, note = route
    if provider == "none":
        return pd.DataFrame(), note or "当前无自动数据源"
    if provider == "csindex":
        df = fetch_pe_csindex(param, start_date=start_date, end_date=end_date)
        return df, note
    if provider == "lg":
        df = fetch_pe_legulegu(param)
        if df.empty:
            return df, (note + "；乐咕接口不可用").strip("；")
        return df, note
    return pd.DataFrame(), f"未知 provider: {provider}"


def run_etf_routes(start_date: str | None, end_date: str | None, tail: int) -> None:
    app_dir = os.path.dirname(os.path.abspath(__file__))
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    import query_my_funds as q

    rows = []
    for fund_code, fund_name, track in q.MY_ETFS:
        df, msg = fetch_by_tracking_name(track, start_date=start_date, end_date=end_date)
        base = {
            "ETF代码": fund_code,
            "ETF简称": fund_name,
            "跟踪指数": track,
            "备注": msg or "",
        }
        summ = summarize_dynamic_pe_interval(df if not df.empty else pd.DataFrame())
        if df.empty:
            alert = add_position_alert_fields(None, tracking_index_name=track)
            rows.append(
                {
                    **base,
                    "最新日期": None,
                    "滚动市盈率": None,
                    "记录区间": None,
                    "区间最大动态市盈率": None,
                    "区间最小动态市盈率": None,
                    "最新动态市盈率中位数": None,
                    **alert,
                }
            )
            continue
        last = df.dropna(subset=["滚动市盈率"]).tail(1) if "滚动市盈率" in df.columns else df.tail(1)
        if last.empty:
            last = df.tail(1)
        latest_pe = None
        if "滚动市盈率" in last.columns:
            latest_pe = pd.to_numeric(last["滚动市盈率"].iloc[-1], errors="coerce")
            if pd.isna(latest_pe):
                latest_pe = None
            else:
                latest_pe = float(latest_pe)
        alert = add_position_alert_fields(latest_pe, tracking_index_name=track)
        rows.append(
            {
                **base,
                "最新日期": last["日期"].iloc[-1] if "日期" in last.columns else None,
                "滚动市盈率": latest_pe,
                **summ,
                **alert,
            }
        )

    col_order = [
        "ETF代码",
        "ETF简称",
        "跟踪指数",
        "记录区间",
        "区间最大动态市盈率",
        "区间最小动态市盈率",
        "最新动态市盈率中位数",
        "最新日期",
        "滚动市盈率",
        "加仓提醒点",
        "是否触及加仓提醒",
        "操作建议",
        "备注",
    ]
    out = pd.DataFrame(rows)
    out = out[[c for c in col_order if c in out.columns]]
    print(out.to_string(index=False))
    os.makedirs("output", exist_ok=True)
    path = os.path.join(
        "output",
        f"index_pe_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    )
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n已保存: {path}")

    triggered = out[out["是否触及加仓提醒"] == "是"]
    if not triggered.empty:
        print("\n" + "=" * 80)
        print("【加仓提醒：已触及】（最新滚动市盈率 <= 加仓提醒点）")
        for _, r in triggered.iterrows():
            print(f"\n· {r['ETF简称']} ({r['ETF代码']}) | {r['跟踪指数']}")
            print(f"  {r['操作建议']}")

    # 打印有历史序列的指数的尾部（便于核对）
    print("\n—— 有中证/乐咕历史序列的指数（最近 {} 条）——".format(tail))
    for fund_code, fund_name, track in q.MY_ETFS:
        df, msg = fetch_by_tracking_name(track, start_date=start_date, end_date=end_date)
        if df.empty:
            continue
        print(f"\n[{fund_code} {fund_name}] {track}")
        print(df.tail(tail).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="指数市盈率查询")
    parser.add_argument("--csindex", type=str, help="中证指数代码，如 000300、399987、H30590")
    parser.add_argument("--lg", type=str, help="乐咕支持的指数简称，如 深证红利、沪深300")
    parser.add_argument("--tracking", type=str, help="与 MY_ETFS 第三列一致的跟踪指数全称")
    parser.add_argument("--etf-routes", action="store_true", help="按 MY_ETFS + INDEX_PE_ROUTES 批量查询")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD（仅 csindex）")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD（仅 csindex）")
    parser.add_argument("--tail", type=int, default=5, help="--etf-routes 时打印历史尾部行数")
    args = parser.parse_args()

    if args.csindex:
        df = fetch_pe_csindex(args.csindex, start_date=args.start, end_date=args.end)
        print(df.tail(20).to_string(index=False))
        return
    if args.lg:
        df = fetch_pe_legulegu(args.lg)
        print(df.tail(20).to_string(index=False))
        if not df.empty and "滚动市盈率" in df.columns:
            lp = pd.to_numeric(df["滚动市盈率"].iloc[-1], errors="coerce")
            lp_f = float(lp) if pd.notna(lp) else None
            adv = add_position_alert_fields(lp_f, legulegu_short=args.lg)
            if adv.get("加仓提醒点") is not None:
                print("\n【加仓提醒】")
                print(f"  加仓提醒点: {adv['加仓提醒点']}")
                print(f"  是否触及: {adv['是否触及加仓提醒']}")
                print(f"  {adv['操作建议']}")
        return
    if args.tracking:
        df, msg = fetch_by_tracking_name(args.tracking, start_date=args.start, end_date=args.end)
        if msg:
            print("说明:", msg)
        if df.empty:
            return
        print(df.tail(20).to_string(index=False))
        if "滚动市盈率" in df.columns:
            lp = pd.to_numeric(df["滚动市盈率"].iloc[-1], errors="coerce")
            lp_f = float(lp) if pd.notna(lp) else None
            adv = add_position_alert_fields(lp_f, tracking_index_name=args.tracking)
            if adv.get("加仓提醒点") is not None:
                print("\n【加仓提醒】")
                print(f"  加仓提醒点: {adv['加仓提醒点']}")
                print(f"  是否触及: {adv['是否触及加仓提醒']}")
                print(f"  {adv['操作建议']}")
        return
    if args.etf_routes:
        run_etf_routes(args.start, args.end, args.tail)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
