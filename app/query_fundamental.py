"""
查询上市公司基本面信息
支持传入多个公司名称，输出基本面信息表
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.datasource.akshare_datasource import AkShareDataSource
from src.queries.fundamental_query import FundamentalQuery
from src.utils.logger import setup_logger


def main():
    """查询上市公司基本面信息"""
    # 设置日志 - 只显示WARNING及以上级别
    setup_logger(level=30)  # WARNING级别
    
    # 要查询的股票代码列表
    stock_codes = [
        "000429",  # 粤高速A
        "600350",  # 山东高速
        "001965",  # 招商公路
        "600377",  # 宁沪高速
        "600012",  # 皖通高速
        "600033",  # 福建高速
    ]
    
    print("\n" + "="*80)
    print(f"查询 {len(stock_codes)} 家高速公路公司的基本面信息")
    print("="*80 + "\n")
    
    # 使用AkShare数据源
    with AkShareDataSource() as datasource:
        query = FundamentalQuery(datasource=datasource)
        
        # 设置pandas显示选项
        import pandas as pd
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 20)
        
        # 获取股票基本信息，建立代码到名称的映射
        print("\n获取股票基本信息...")
        stock_info = datasource.query_stock_basic()
        code_to_name = {}
        if stock_info is not None and not stock_info.empty:
            for _, row in stock_info.iterrows():
                code_val = str(row.get('code', ''))
                name_val = str(row.get('name', ''))
                if code_val and name_val:
                    code_to_name[code_val] = name_val
            print(f"✓ 已获取 {len(code_to_name)} 个股票信息")
        
        # 存储所有查询结果
        all_results = []
        
        # 直接调用 query 方法，传入 code 参数
        for code in stock_codes:
            stock_name = code_to_name.get(code, "未知")
            print(f"\n查询股票: {code} ({stock_name})")
            df = query.query(code=code, year=2025)
            
            if df is not None and not df.empty:
                print(f"✓ 查询成功，返回 {len(df)} 条数据")
                print(df.to_string(index=False))
                
                # 添加股票代码和股票名称列（如果不存在）
                if '股票代码' not in df.columns:
                    df.insert(0, '股票代码', code)
                if '股票名称' not in df.columns:
                    df.insert(1, '股票名称', stock_name)
                
                all_results.append(df)
            else:
                print(f"✗ 未查询到数据")
        
        # 合并所有结果并导出
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            
            # 导出到CSV 加时间戳
            output_file = f"fundamental_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print("\n" + "="*80)
            print(f"查询完成! 共查询 {len(all_results)} 家公司，{len(combined_df)} 条数据")
            print(f"数据已导出到: {output_file}")
            print("="*80 + "\n")
        else:
            print("\n" + "="*80)
            print("未查询到任何数据")
            print("="*80 + "\n")


if __name__ == "__main__":
    main()
