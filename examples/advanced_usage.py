"""
高级使用示例
"""

import sys
from pathlib import Path
import pandas as pd

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.connection import BaoStockConnection
from queries.cashflow_query import CashFlowQuery
from utils.logger import setup_logger


def example_export_to_csv():
    """示例: 导出数据到CSV"""
    print("\n" + "="*60)
    print("示例: 导出多公司现金流量数据到CSV")
    print("="*60)
    
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 查询多家公司
        codes = ["600000", "601398", "601939", "000001"]
        results = query.query_multiple(codes=codes, year=2023)
        
        # 合并所有数据
        all_data = []
        for code, df in results.items():
            if df is not None and not df.empty:
                all_data.append(df)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 导出到CSV
            output_file = "cashflow_data_2023.csv"
            combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n数据已导出到: {output_file}")
            print(f"共 {len(combined_df)} 条记录")
        else:
            print("没有数据可导出")


def example_custom_analysis():
    """示例: 自定义分析"""
    print("\n" + "="*60)
    print("示例: 现金流量趋势分析")
    print("="*60)
    
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 查询历史数据
        df = query.query_history(
            code="600000",
            start_year=2019,
            end_year=2023
        )
        
        if df is not None and not df.empty:
            # 转换数据类型
            df['CAToAsset'] = pd.to_numeric(df['CAToAsset'], errors='coerce')
            df['year'] = df['statDate'].str[:4]
            
            # 按年份分组统计
            yearly_stats = df.groupby('year')['CAToAsset'].agg(['mean', 'sum', 'count'])
            
            print(f"\n浦发银行历年经营活动现金流量统计:")
            print(yearly_stats.to_string())
            
            # 计算增长率
            if len(yearly_stats) > 1:
                print(f"\n年度增长率:")
                for i in range(1, len(yearly_stats)):
                    prev_val = yearly_stats.iloc[i-1]['sum']
                    curr_val = yearly_stats.iloc[i]['sum']
                    if prev_val != 0:
                        growth_rate = ((curr_val - prev_val) / prev_val) * 100
                        print(f"{yearly_stats.index[i]}: {growth_rate:.2f}%")
        else:
            print("未查询到数据")


def example_filter_and_rank():
    """示例: 筛选和排名"""
    print("\n" + "="*60)
    print("示例: 根据现金流量指标排名")
    print("="*60)
    
    with BaoStockConnection():
        query = CashFlowQuery()
        
        # 查询多家公司
        codes = [
            "600000", "601398", "601939",  # 银行
            "600519", "000858",  # 白酒
            "000001", "000002",  # 深圳
        ]
        
        df = query.compare_companies(
            codes=codes,
            year=2023,
            metrics=['CAToAsset', 'NCAToAsset', 'NCFFromFAToAsset']
        )
        
        if df is not None and not df.empty:
            # 转换为数值类型
            for col in ['CAToAsset', 'NCAToAsset', 'NCFFromFAToAsset']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 按经营活动现金流量排序
            df_sorted = df.sort_values('CAToAsset', ascending=False)
            
            print(f"\n公司现金流量排名 (按经营活动现金流量):")
            print(df_sorted.to_string())
            
            # 筛选经营活动现金流量为正的公司
            positive_cf = df[df['CAToAsset'] > 0]
            print(f"\n经营活动现金流量为正的公司数量: {len(positive_cf)}/{len(df)}")
        else:
            print("未查询到数据")


def main():
    """主函数"""
    # 设置日志
    setup_logger(level=20)
    
    print("\n" + "="*60)
    print("BaoStock 现金流量查询 - 高级示例")
    print("="*60)
    
    # 运行示例
    example_export_to_csv()
    example_custom_analysis()
    example_filter_and_rank()
    
    print("\n" + "="*60)
    print("所有示例执行完成")
    print("="*60)


if __name__ == "__main__":
    main()
