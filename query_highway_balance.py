"""
查询高速公路公司资产负债率
"""

import sys
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.core.connection import BaoStockConnection
from src.queries.balance_query import BalanceQuery
from src.utils.logger import setup_logger


def main():
    """查询高速公路公司资产负债率"""
    # 设置日志 - 只显示WARNING及以上级别
    setup_logger(level=30)  # WARNING级别
    
    # 高速公路公司列表
    companies = [
        ("601107", "四川成渝"),
        ("000429", "粤高速A"),
        ("600350", "山东高速"),
        ("000828", "东莞控股"),
        ("600035", "楚天高速"),
        ("001965", "招商公路"),
        ("600377", "宁沪高速"),
        ("600012", "皖通高速"),
        ("000755", "山西路桥"),
    ]
    
    with BaoStockConnection():
        query = BalanceQuery()
        
        # 查询2025年第3季度数据
        for code, name in companies:
            df = query.query(code=code, year=2025, quarter=3)
            
            if df is not None and not df.empty and 'liabilityToAsset' in df.columns:
                liability_to_asset = float(df['liabilityToAsset'].iloc[-1]) * 100
                print(f"{liability_to_asset}")
            else:
                print(f"{name}({code}): 无数据")


if __name__ == "__main__":
    main()
