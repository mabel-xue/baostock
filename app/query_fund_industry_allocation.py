"""
查询基金行业配置数据
使用 akshare 的 fund_report_industry_allocation_cninfo 接口
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


def get_year_report_dates(year: int):
    """
    获取指定年份的所有季度报告日期
    
    Args:
        year: 年份，如 2024
    
    Returns:
        list: 报告日期列表，格式为 YYYYMMDD，按时间顺序排列（Q1, Q2, Q3, Q4）
    """
    # 季度报告日期：03-31, 06-30, 09-30, 12-31
    quarter_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
    dates = []
    for q_month, q_day in quarter_ends:
        dates.append(f"{year}{q_month:02d}{q_day:02d}")
    return dates


def query_fund_industry_allocation_by_dates(dates: list):
    """
    查询基金行业配置数据
    
    Args:
        dates: 报告日期列表，格式为 YYYYMMDD，如 ["20231231", "20230930"]
    
    Returns:
        DataFrame: 合并后的数据，包含报告期列
    """
    all_data = []
    
    try:
        print(f"查询报告期: {', '.join(dates)}\n")
        
        # 查询每个报告期的数据
        for report_date in dates:
            try:
                print(f"正在查询 {report_date}...", end=" ")
                df = ak.fund_report_industry_allocation_cninfo(date=report_date)
                
                if df is not None and not df.empty:
                    # 添加报告期列（如果不存在）
                    if '报告期' not in df.columns:
                        df['报告期'] = report_date
                    all_data.append(df)
                    print(f"✓ 获取 {len(df)} 条记录")
                else:
                    print(f"✗ 无数据（返回空DataFrame）")
                    
            except Exception as e:
                # 打印详细的错误信息
                error_type = type(e).__name__
                error_msg = str(e)
                print(f"✗ 查询失败")
                print(f"  错误类型: {error_type}")
                print(f"  错误信息: {error_msg}")
                
                # 如果是JSON解析错误，可能是数据源问题
                if "JSON" in error_type or "json" in error_msg.lower() or "Expecting value" in error_msg:
                    print(f"  可能原因: 该报告期的数据可能尚未发布或数据源暂时不可用")
                    print(f"  建议: 检查数据源是否有该报告期的数据，或稍后重试")
                
                # 打印完整的异常堆栈（用于深度调试）
                import traceback
                print(f"  详细堆栈:")
                for line in traceback.format_exc().split('\n')[-5:-1]:  # 只打印最后几行
                    if line.strip():
                        print(f"    {line}")
        
        if not all_data:
            print("\n所有报告期均无数据")
            return None
        
        # 合并所有数据
        df_all = pd.concat(all_data, ignore_index=True)
        
        # 打印原始列名，便于调试
        print(f"\n数据列: {df_all.columns.tolist()}\n")
        
        return df_all
        
    except Exception as e:
        print(f"查询失败: {str(e)}")
        return None


def format_date_display(date_str: str) -> str:
    """
    格式化日期显示
    
    Args:
        date_str: YYYYMMDD 格式的日期字符串
    
    Returns:
        格式化后的日期字符串，如 2025-12-31
    """
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str


def calculate_period_change(df, ratio_col):
    """
    计算环比数据：每个行业在相邻报告期之间的占净资产比例变化
    
    Args:
        df: 原始DataFrame
        ratio_col: 占净资产比例列名
    
    Returns:
        DataFrame: 添加了环比列的DataFrame
    """
    if df is None or df.empty or ratio_col is None:
        return df
    
    # 确保有报告期和行业名称列
    if '报告期' not in df.columns:
        return df
    
    # 找到行业名称列
    industry_col = None
    for col in df.columns:
        if '行业名称' in col or '证监会行业名称' in col:
            industry_col = col
            break
    
    if industry_col is None:
        return df
    
    # 按报告期排序（从旧到新）
    periods = sorted(df['报告期'].unique())
    
    # 为每个行业计算环比
    df_with_change = df.copy()
    df_with_change['环比变化'] = '-'
    
    for i in range(1, len(periods)):
        current_period = periods[i]
        previous_period = periods[i-1]
        
        # 获取当前期和上一期的数据
        current_data = df[df['报告期'] == current_period].set_index(industry_col)
        previous_data = df[df['报告期'] == previous_period].set_index(industry_col)
        
        # 计算环比变化
        for industry in current_data.index:
            if industry in previous_data.index:
                current_ratio = current_data.loc[industry, ratio_col]
                previous_ratio = previous_data.loc[industry, ratio_col]
                
                if pd.notna(current_ratio) and pd.notna(previous_ratio) and previous_ratio != 0:
                    change = current_ratio - previous_ratio
                    # 更新环比变化值
                    mask = (df_with_change['报告期'] == current_period) & (df_with_change[industry_col] == industry)
                    df_with_change.loc[mask, '环比变化'] = f"{change:+.2f}"
    
    return df_with_change


def process_data(df):
    """
    处理数据：找到关键列、转换数据类型、排序、计算环比
    
    Args:
        df: 原始DataFrame
    
    Returns:
        tuple: (按比例排序的DataFrame, 按环比变化排序的DataFrame, ratio_col, periods, period_tables)
    """
    if df is None or df.empty:
        return None, None, [], {}
    
    # 找到关键列
    ratio_col = None
    for col in df.columns:
        if '占净资产比例' in col or '占净值比例' in col or '市值占净值比' in col:
            ratio_col = col
            break
    
    if '报告期' not in df.columns:
        print("警告: 数据中没有报告期列")
        return df, ratio_col, [], {}
    
    periods = sorted(df['报告期'].unique(), reverse=True)
    
    # 确保比例列是数值类型
    if ratio_col and df[ratio_col].dtype == 'object':
        df[ratio_col] = pd.to_numeric(df[ratio_col], errors='coerce')
    
    # 计算环比变化
    df = calculate_period_change(df, ratio_col)
    
    # 生成总排名表（所有报告期数据按比例倒序）
    if ratio_col:
        df_total = df.sort_values([ratio_col], ascending=False).copy()
    else:
        df_total = df.copy()
    
    # 生成按环比变化倒序的排名表
    # 需要将环比变化列转换为数值类型以便排序
    df_change = df.copy()
    # 创建一个数值型的环比变化列用于排序
    df_change['环比变化_数值'] = df_change['环比变化'].apply(
        lambda x: float(x) if isinstance(x, str) and x != '-' else float('-inf')
    )
    df_total_by_change = df_change.sort_values(['环比变化_数值'], ascending=False).copy()
    # 删除临时的数值列
    df_total_by_change = df_total_by_change.drop(columns=['环比变化_数值'])
    
    # 按报告期分组
    period_tables = {}
    for period in periods:
        df_period = df[df['报告期'] == period].copy()
        if ratio_col:
            df_period = df_period.sort_values(ratio_col, ascending=False)
        period_tables[period] = df_period
    
    return df_total, df_total_by_change, ratio_col, periods, period_tables


def write_report_file(df_total, df_total_by_change, ratio_col, periods, period_tables, years: list, output_dir: str = "output"):
    """
    写入报告文件
    
    Args:
        df_total: 按比例排序的总排名DataFrame
        df_total_by_change: 按环比变化排序的总排名DataFrame
        ratio_col: 比例列名
        periods: 报告期列表
        period_tables: 按报告期分组的字典
        years: 年份列表
        output_dir: 输出目录
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    year_str = "_".join([str(y) for y in years]) if years else "all"
    output_file = output_path / f"fund_industry_allocation_{year_str}_{timestamp}.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入标题
        f.write("=" * 100 + "\n")
        if len(years) == 1:
            f.write(f"基金行业配置报告 - {years[0]}年\n")
        else:
            f.write(f"基金行业配置报告 - {', '.join([str(y) for y in years])}年\n")
        f.write(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 100 + "\n\n")
        
        # 写入统计信息
        f.write(f"数据记录数: {len(df_total)}\n")
        f.write(f"报告期数: {len(periods)}\n")
        f.write(f"报告期: {', '.join([format_date_display(str(p)) for p in periods])}\n")
        if ratio_col:
            f.write(f"排序依据: {ratio_col}\n")
        f.write("\n")
        
        # 写入总排名表（按占净资产比例排序）
        f.write("=" * 100 + "\n")
        if len(years) == 1:
            f.write(f"总排名表 - 按占净资产比例排序（{years[0]}年所有报告期数据汇总）\n")
        else:
            f.write("总排名表 - 按占净资产比例排序（所有报告期数据汇总）\n")
        f.write("=" * 100 + "\n\n")
        f.write(df_total.to_string(index=False))
        f.write("\n\n")
        
        # 写入总排名表（按环比变化排序）
        f.write("=" * 100 + "\n")
        if len(years) == 1:
            f.write(f"总排名表 - 按环比变化排序（{years[0]}年所有报告期数据汇总）\n")
        else:
            f.write("总排名表 - 按环比变化排序（所有报告期数据汇总）\n")
        f.write("=" * 100 + "\n\n")
        f.write(df_total_by_change.to_string(index=False))
        f.write("\n\n")
        
        # 写入各报告期分表
        for period in periods:
            df_period = period_tables[period]
            f.write("=" * 100 + "\n")
            f.write(f"报告期: {format_date_display(str(period))} - 共 {len(df_period)} 条记录\n")
            f.write("=" * 100 + "\n\n")
            f.write(df_period.to_string(index=False))
            f.write("\n\n")
        
        f.write("=" * 100 + "\n")
        f.write("报告生成完成\n")
        f.write("=" * 100 + "\n")
    
    return output_file


def print_report_summary(df_total, df_total_by_change, ratio_col, periods, period_tables, years: list):
    """
    在控制台打印报告摘要
    
    Args:
        df_total: 按比例排序的总排名DataFrame
        df_total_by_change: 按环比变化排序的总排名DataFrame
        ratio_col: 比例列名
        periods: 报告期列表
        period_tables: 按报告期分组的字典
        years: 年份列表
    """
    print(f"\n报告已保存")
    
    # 在控制台显示摘要 - 按占净资产比例排序
    print("\n" + "=" * 100)
    if len(years) == 1:
        print(f"总排名表 - 按占净资产比例排序（{years[0]}年前20条）")
    else:
        print("总排名表 - 按占净资产比例排序（前20条）")
    print("=" * 100)
    print(df_total.head(20).to_string(index=False))
    
    # 在控制台显示摘要 - 按环比变化排序
    print("\n" + "=" * 100)
    if len(years) == 1:
        print(f"总排名表 - 按环比变化排序（{years[0]}年前20条）")
    else:
        print("总排名表 - 按环比变化排序（前20条）")
    print("=" * 100)
    print(df_total_by_change.head(20).to_string(index=False))
    
    for period in periods:
        df_period = period_tables[period]
        print(f"\n{'=' * 100}")
        print(f"报告期: {format_date_display(str(period))} - 前10条记录")
        print("=" * 100)
        print(df_period.head(10).to_string(index=False))
    
    print("\n" + "=" * 100)
    print("报告生成完成")
    print("=" * 100)


def generate_report(years: list, output_dir: str = "output"):
    """
    生成基金行业配置报告，包含总排名表和按报告期分表
    
    Args:
        years: 年份列表，如 [2024] 或 [2023, 2024]
        output_dir: 输出目录
    
    Returns:
        tuple: (总排名DataFrame, 按报告期分组的字典)
    """
    print("=" * 80)
    print(f"基金行业配置报告")
    print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # 获取所有年份的报告日期
    all_dates = []
    for year in years:
        year_dates = get_year_report_dates(year)
        all_dates.extend(year_dates)
        print(f"{year}年报告期: {', '.join([format_date_display(d) for d in year_dates])}")
    print()
    
    # 查询数据
    df = query_fund_industry_allocation_by_dates(all_dates)
    
    if df is None or df.empty:
        print("未获取到数据")
        return None, None
    
    # 显示数据统计
    print(f"数据记录数: {len(df)}")
    
    # 处理数据
    df_total, df_total_by_change, ratio_col, periods, period_tables = process_data(df)
    
    if df_total is None:
        return None, None
    
    print(f"报告期数: {len(periods)}")
    print(f"报告期: {', '.join([format_date_display(str(p)) for p in periods])}")
    
    # 写入文件
    output_file = write_report_file(df_total, df_total_by_change, ratio_col, periods, period_tables, years, output_dir)
    print(f"\n报告已保存到: {output_file}")
    
    # 打印摘要
    print_report_summary(df_total, df_total_by_change, ratio_col, periods, period_tables, years)
    
    return df_total, period_tables


def test_single_date(date: str):
    """
    测试单个日期的查询，用于调试
    
    Args:
        date: 报告日期，格式为 YYYYMMDD，如 "20241231"
    """
    print("=" * 80)
    print(f"测试单个日期查询: {date}")
    print("=" * 80)
    print()
    
    try:
        import akshare as ak
        
        print(f"正在调用 ak.fund_report_industry_allocation_cninfo(date='{date}')...")
        df = ak.fund_report_industry_allocation_cninfo(date=date)
        
        if df is not None:
            print(f"✓ 查询成功")
            print(f"  数据类型: {type(df)}")
            print(f"  是否为空: {df.empty if hasattr(df, 'empty') else 'N/A'}")
            if hasattr(df, 'shape'):
                print(f"  数据形状: {df.shape}")
            if hasattr(df, 'columns'):
                print(f"  列名: {list(df.columns)}")
            if hasattr(df, 'head'):
                print(f"\n前5行数据:")
                print(df.head())
        else:
            print(f"✗ 查询返回 None")
            
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"✗ 查询失败")
        print(f"  错误类型: {error_type}")
        print(f"  错误信息: {error_msg}")
        
        import traceback
        print(f"\n完整堆栈跟踪:")
        print(traceback.format_exc())


def main():
    """主函数"""
    # 生成2024年的数据，可以传入多个年份，如 [2023, 2024]
    years = [2023,2024,2025]
    
    # 如果需要调试单个日期，取消下面的注释
    # test_single_date("20241231")
    # return
    
    # 生成报告
    generate_report(years=years)


if __name__ == "__main__":
    main()
