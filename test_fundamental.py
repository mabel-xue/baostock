"""
测试基本面查询功能
"""

import sys
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from src.datasource.akshare_datasource import AkShareDataSource
from src.queries.fundamental_query import FundamentalQuery
from src.utils.logger import setup_logger


def test_single_query():
    """测试单个股票查询"""
    print("\n测试1: 查询单个股票基本面")
    print("-" * 60)
    
    setup_logger(level=20)  # INFO级别
    
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 查询贵州茅台
        df = query.query(code="600519", year=2024)
        
        if df is not None and not df.empty:
            print(f"成功查询到 {len(df)} 条数据")
            print(f"列名: {list(df.columns)[:10]}...")  # 只显示前10列
            print(f"\n最近一期数据:")
            latest = df.iloc[-1]
            print(f"报告期: {latest.get('报告期', 'N/A')}")
            print(f"净利润: {latest.get('净利润', latest.get('净利润(亿元)', 'N/A'))}")
            print(f"净资产收益率: {latest.get('净资产收益率', latest.get('净资产收益率(%)', 'N/A'))}")
        else:
            print("未查询到数据")


def test_multiple_query():
    """测试批量查询"""
    print("\n测试2: 批量查询多个股票")
    print("-" * 60)
    
    setup_logger(level=30)  # WARNING级别
    
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 批量查询
        codes = ["600519", "000858", "000568"]
        results = query.query_multiple(codes=codes, year=2024)
        
        for code, df in results.items():
            if df is not None and not df.empty:
                print(f"{code}: 成功 ({len(df)} 条数据)")
            else:
                print(f"{code}: 无数据")


def test_query_by_names():
    """测试根据公司名称查询"""
    print("\n测试3: 根据公司名称查询")
    print("-" * 60)
    
    setup_logger(level=30)  # WARNING级别
    
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 根据名称查询
        names = ["贵州茅台", "五粮液"]
        df = query.query_by_names(names=names, year=2024)
        
        if df is not None and not df.empty:
            print(f"成功查询 {len(df)} 家公司")
            print(f"\n汇总表列名: {list(df.columns)}")
            print(f"\n数据预览:")
            print(df.to_string(index=False))
        else:
            print("未查询到数据")


if __name__ == "__main__":
    try:
        print("\n" + "="*60)
        print("基本面查询功能测试")
        print("="*60)
        
        test_single_query()
        test_multiple_query()
        test_query_by_names()
        
        print("\n" + "="*60)
        print("测试完成!")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
