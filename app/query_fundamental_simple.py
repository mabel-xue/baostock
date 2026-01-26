"""
简单的基本面查询示例
直接使用akshare接口，不依赖复杂的类结构
"""

import pandas as pd


def query_fundamental_simple(stock_codes, year=None):
    """
    简单的基本面查询函数
    
    Args:
        stock_codes: 股票代码列表，如 ["600519", "000858"]
        year: 年份（可选），如 2024
    
    Returns:
        DataFrame: 基本面信息汇总表
    """
    try:
        import akshare as ak
    except ImportError:
        print("请先安装akshare: pip install akshare")
        return None
    
    print(f"\n开始查询 {len(stock_codes)} 个股票的基本面信息...")
    if year:
        print(f"查询年份: {year}")
    
    summary_data = []
    
    for code in stock_codes:
        try:
            print(f"正在查询 {code}...", end=" ")
            
            # 使用akshare查询财务指标
            df = ak.stock_financial_analysis_indicator(symbol=code)
            
            if df is None or df.empty:
                print("无数据")
                continue
            
            # 如果指定了年份，进行过滤
            if year and '报告期' in df.columns:
                df = df[df['报告期'].str.contains(str(year))]
            
            if df.empty:
                print(f"无{year}年数据")
                continue
            
            # 获取最近一期数据
            latest = df.iloc[-1]
            
            # 提取关键指标
            row_data = {
                '股票代码': code,
                '报告期': latest.get('报告期', 'N/A'),
            }
            
            # 添加常见指标（根据实际返回的列名）
            indicators = [
                '净利润', '净利润(亿元)', 
                '营业总收入', '营业总收入(亿元)',
                '净资产收益率', '净资产收益率(%)',
                '毛利率', '毛利率(%)',
                '总资产收益率', '总资产收益率(%)',
                '资产负债率', '资产负债率(%)',
                '每股收益', '每股收益(元)',
            ]
            
            for indicator in indicators:
                if indicator in latest.index:
                    row_data[indicator] = latest[indicator]
            
            summary_data.append(row_data)
            print("成功")
            
        except Exception as e:
            print(f"失败: {str(e)}")
            continue
    
    if not summary_data:
        print("\n未查询到任何数据")
        return None
    
    # 创建汇总表
    summary_df = pd.DataFrame(summary_data)
    return summary_df


def main():
    """主函数"""
    print("\n" + "="*80)
    print("上市公司基本面信息查询工具（简化版）")
    print("="*80)
    
    # 示例1: 查询白酒股
    print("\n【示例1】白酒行业基本面:")
    liquor_stocks = [
        "600519",  # 贵州茅台
        "000858",  # 五粮液
        "000568",  # 泸州老窖
        "600809",  # 山西汾酒
        "002304",  # 洋河股份
    ]
    
    df1 = query_fundamental_simple(liquor_stocks, year=2024)
    
    if df1 is not None and not df1.empty:
        print("\n基本面信息汇总表:")
        print("-" * 80)
        
        # 设置pandas显示选项
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 20)
        
        print(df1.to_string(index=False))
        
        # 保存到CSV
        output_file = "fundamental_liquor.csv"
        df1.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n数据已保存到: {output_file}")
    
    # 示例2: 查询银行股
    print("\n" + "="*80)
    print("\n【示例2】银行股基本面:")
    bank_stocks = [
        "600000",  # 浦发银行
        "601398",  # 工商银行
        "601288",  # 农业银行
    ]
    
    df2 = query_fundamental_simple(bank_stocks, year=2024)
    
    if df2 is not None and not df2.empty:
        print("\n基本面信息汇总表:")
        print("-" * 80)
        print(df2.to_string(index=False))
        
        # 保存到CSV
        output_file = "fundamental_banks.csv"
        df2.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n数据已保存到: {output_file}")
    
    print("\n" + "="*80)
    print("查询完成!")
    print("="*80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
