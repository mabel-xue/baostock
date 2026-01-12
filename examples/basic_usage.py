"""
基础使用示例
"""

import sys
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.connection import BaoStockConnection
from queries.cashflow_query import CashFlowQuery
from utils.logger import setup_logger


def example_single_query():
    """示例1: 查询单个公司的现金流量"""
    print("\n" + "="*60)
    print("示例1: 查询单个公司的现金流量")
    print("="*60)
    
    # 使用上下文管理器自动管理连接
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 查询浦发银行(600000)2023年的现金流量
        df = query.query(code="600000", year=2023)
        
        if df is not None and not df.empty:
            print(f"\n浦发银行2023年现金流量数据:")
            print(df.to_string())
        else:
            print("未查询到数据")


def example_multiple_query():
    """示例2: 批量查询多个公司的现金流量"""
    print("\n" + "="*60)
    print("示例2: 批量查询多个公司的现金流量")
    print("="*60)
    
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 查询多家银行的现金流量
        codes = [
            "600000",  # 浦发银行
            "601398",  # 工商银行
            "601939",  # 建设银行
        ]
        
        results = query.query_multiple(codes=codes, year=2023)
        
        for code, df in results.items():
            print(f"\n{code} 的现金流量数据:")
            if df is not None and not df.empty:
                print(df[['code', 'pubDate', 'statDate', 'CAToAsset', 'NCAToAsset']].to_string())
            else:
                print("未查询到数据")


def example_history_query():
    """示例3: 查询历史现金流量数据"""
    print("\n" + "="*60)
    print("示例3: 查询历史现金流量数据")
    print("="*60)
    
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 查询浦发银行2020-2023年的现金流量
        df = query.query_history(
            code="600000",
            start_year=2020,
            end_year=2023
        )
        
        if df is not None and not df.empty:
            print(f"\n浦发银行2020-2023年现金流量数据:")
            print(df[['code', 'pubDate', 'statDate', 'CAToAsset', 'NCAToAsset']].to_string())
        else:
            print("未查询到数据")


def example_compare_companies():
    """示例4: 对比多个公司的现金流量指标"""
    print("\n" + "="*60)
    print("示例4: 对比多个公司的现金流量指标")
    print("="*60)
    
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 对比多家银行的现金流量指标
        codes = [
            "600000",  # 浦发银行
            "601398",  # 工商银行
            "601939",  # 建设银行
        ]
        
        df = query.compare_companies(
            codes=codes,
            year=2023,
            metrics=['CAToAsset', 'NCAToAsset', 'NCFFromFAToAsset']
        )
        
        if df is not None and not df.empty:
            print(f"\n银行现金流量对比 (2023年):")
            print(df.to_string())
        else:
            print("未查询到数据")


def main():
    """主函数"""
    # 设置日志
    setup_logger(level=20)  # INFO级别
    
    print("\n" + "="*60)
    print("BaoStock 现金流量查询示例")
    print("="*60)
    
    # 运行各个示例
    example_single_query()
    example_multiple_query()
    example_history_query()
    example_compare_companies()
    
    print("\n" + "="*60)
    print("所有示例执行完成")
    print("="*60)


if __name__ == "__main__":
    main()
