"""
查询上市公司基本面信息
支持传入多个公司名称，输出基本面信息表
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.datasource.akshare_datasource import AkShareDataSource
from src.queries.fundamental_query import FundamentalQuery
from src.utils.logger import setup_logger


def main():
    """查询上市公司基本面信息"""
    # 设置日志 - 只显示WARNING及以上级别
    setup_logger(level=30)  # WARNING级别
    
    # 要查询的公司名称列表
    company_names = [
        "贵州茅台",
        "五粮液",
        "泸州老窖",
        "山西汾酒",
        "洋河股份",
    ]
    
    print("\n" + "="*80)
    print(f"查询 {len(company_names)} 家白酒公司的基本面信息")
    print("="*80 + "\n")
    
    # 使用AkShare数据源
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 查询最近一期的基本面数据
        df = query.query_by_names(names=company_names, year=2024)
        
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
            
            # 可选：保存到CSV文件
            output_file = "fundamental_data.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"数据已保存到: {output_file}\n")
        else:
            print("未查询到数据\n")


if __name__ == "__main__":
    main()
