"""
主程序入口
"""

import sys
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.connection import BaoStockConnection
from queries.cashflow_query import CashFlowQuery
from utils.logger import setup_logger


def main():
    """主函数 - 快速开始示例"""
    # 设置日志
    logger = setup_logger(level=20)
    
    print("\n" + "="*60)
    print("BaoStock 公司分析工具 - 现金流量查询")
    print("="*60)
    
    # 使用上下文管理器自动管理连接
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 示例: 查询几家知名公司的现金流量
        companies = {
            "600000": "浦发银行",
            "601398": "工商银行",
            "600519": "贵州茅台",
        }
        
        print(f"\n正在查询 {len(companies)} 家公司的2023年现金流量数据...\n")
        
        for code, name in companies.items():
            df = query.query(code=code, year=2023)
            
            if df is not None and not df.empty:
                print(f"\n{name} ({code}):")
                print("-" * 60)
                
                # 显示关键指标
                if not df.empty:
                    latest = df.iloc[-1]
                    print(f"  统计日期: {latest.get('statDate', 'N/A')}")
                    print(f"  发布日期: {latest.get('pubDate', 'N/A')}")
                    print(f"  经营活动现金流: {latest.get('CAToAsset', 'N/A')}")
                    print(f"  投资活动现金流: {latest.get('NCAToAsset', 'N/A')}")
                    print(f"  筹资活动现金流: {latest.get('NCFFromFAToAsset', 'N/A')}")
            else:
                print(f"\n{name} ({code}): 未查询到数据")
        
        print("\n" + "="*60)
        print("查询完成!")
        print("\n更多示例请查看 examples/ 目录:")
        print("  - examples/basic_usage.py: 基础使用示例")
        print("  - examples/advanced_usage.py: 高级使用示例")
        print("="*60 + "\n")


if __name__ == "__main__":
    main()
