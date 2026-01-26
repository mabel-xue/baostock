"""
自定义查询上市公司基本面信息
支持传入多个公司名称或代码，输出基本面信息表
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.datasource.akshare_datasource import AkShareDataSource
from src.queries.fundamental_query import FundamentalQuery
from src.utils.logger import setup_logger


def query_by_names(company_names, year=None):
    """
    根据公司名称查询基本面信息
    
    Args:
        company_names: 公司名称列表
        year: 年份（可选）
    """
    print("\n" + "="*80)
    print(f"查询 {len(company_names)} 家公司的基本面信息")
    if year:
        print(f"查询年份: {year}")
    print("="*80 + "\n")
    
    # 使用AkShare数据源
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 查询基本面数据
        df = query.query_by_names(names=company_names, year=year)
        
        if df is not None and not df.empty:
            print("基本面信息汇总表:")
            print("-" * 80)
            
            # 设置pandas显示选项
            import pandas as pd
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 20)
            
            print(df.to_string(index=False))
            print("\n" + "="*80)
            print(f"查询完成! 共 {len(df)} 家公司")
            print("="*80 + "\n")
            
            return df
        else:
            print("未查询到数据\n")
            return None


def query_by_codes(stock_codes, year=None):
    """
    根据股票代码查询基本面信息
    
    Args:
        stock_codes: 股票代码列表
        year: 年份（可选）
    """
    print("\n" + "="*80)
    print(f"查询 {len(stock_codes)} 个股票的基本面信息")
    if year:
        print(f"查询年份: {year}")
    print("="*80 + "\n")
    
    # 使用AkShare数据源
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 批量查询基本面数据
        results = query.query_multiple(codes=stock_codes, year=year)
        
        # 汇总结果
        summary_data = []
        for code, df in results.items():
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                row_data = {
                    '股票代码': code,
                    '报告期': latest.get('报告期', 'N/A'),
                }
                
                # 添加常见指标
                indicators = [
                    '净利润', '净利润(亿元)', '营业总收入', '营业总收入(亿元)',
                    '净资产收益率', '净资产收益率(%)',
                    '毛利率', '毛利率(%)',
                    '资产负债率', '资产负债率(%)',
                    '每股收益', '每股收益(元)',
                ]
                
                for indicator in indicators:
                    if indicator in latest.index:
                        row_data[indicator] = latest[indicator]
                
                summary_data.append(row_data)
            else:
                print(f"{code}: 无数据")
        
        if summary_data:
            import pandas as pd
            summary_df = pd.DataFrame(summary_data)
            
            print("基本面信息汇总表:")
            print("-" * 80)
            
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 20)
            
            print(summary_df.to_string(index=False))
            print("\n" + "="*80)
            print(f"查询完成! 共 {len(summary_df)} 个股票")
            print("="*80 + "\n")
            
            return summary_df
        else:
            print("未查询到数据\n")
            return None


def main():
    """主函数 - 演示不同的查询方式"""
    # 设置日志 - 只显示WARNING及以上级别
    setup_logger(level=30)
    
    print("\n" + "="*80)
    print("上市公司基本面信息查询工具")
    print("="*80)
    
    # 示例1: 根据公司名称查询
    print("\n【示例1】根据公司名称查询白酒行业基本面:")
    company_names = [
        "贵州茅台",
        "五粮液",
        "泸州老窖",
        "山西汾酒",
        "洋河股份",
    ]
    df1 = query_by_names(company_names, year=2024)
    
    # 示例2: 根据股票代码查询
    print("\n【示例2】根据股票代码查询银行股基本面:")
    stock_codes = [
        "600000",  # 浦发银行
        "601398",  # 工商银行
        "601288",  # 农业银行
        "601939",  # 建设银行
        "601328",  # 交通银行
    ]
    df2 = query_by_codes(stock_codes, year=2024)
    
    # 可选：保存到CSV文件
    if df1 is not None and not df1.empty:
        output_file1 = "fundamental_data_liquor.csv"
        df1.to_csv(output_file1, index=False, encoding='utf-8-sig')
        print(f"白酒行业数据已保存到: {output_file1}")
    
    if df2 is not None and not df2.empty:
        output_file2 = "fundamental_data_banks.csv"
        df2.to_csv(output_file2, index=False, encoding='utf-8-sig')
        print(f"银行股数据已保存到: {output_file2}")
    
    print("\n" + "="*80)
    print("查询完成!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
