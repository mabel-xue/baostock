"""
数据源对比示例
对比不同数据源的数据质量、速度和可用性
"""

import sys
from pathlib import Path
import time

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datasource import DataSourceFactory, DataSourceType
from utils.logger import setup_logger


def benchmark_datasource(source_type: DataSourceType, code: str, year: int):
    """
    测试单个数据源的性能
    
    Args:
        source_type: 数据源类型
        code: 股票代码
        year: 年份
    """
    print(f"\n测试数据源: {source_type.value}")
    print("-" * 40)
    
    try:
        # 创建数据源
        start_time = time.time()
        datasource = DataSourceFactory.create(source_type)
        connect_time = time.time() - start_time
        print(f"  连接耗时: {connect_time:.3f}秒")
        
        # 查询资产负债表
        start_time = time.time()
        df_balance = datasource.query_balance_sheet(code=code, year=year)
        balance_time = time.time() - start_time
        
        if df_balance is not None and not df_balance.empty:
            print(f"  资产负债表: 成功 ({len(df_balance)}条记录, {balance_time:.3f}秒)")
            print(f"    字段数: {len(df_balance.columns)}")
        else:
            print(f"  资产负债表: 无数据 ({balance_time:.3f}秒)")
        
        # 查询现金流量表
        start_time = time.time()
        df_cashflow = datasource.query_cash_flow(code=code, year=year)
        cashflow_time = time.time() - start_time
        
        if df_cashflow is not None and not df_cashflow.empty:
            print(f"  现金流量表: 成功 ({len(df_cashflow)}条记录, {cashflow_time:.3f}秒)")
            print(f"    字段数: {len(df_cashflow.columns)}")
        else:
            print(f"  现金流量表: 无数据 ({cashflow_time:.3f}秒)")
        
        # 查询利润表
        start_time = time.time()
        df_income = datasource.query_income_statement(code=code, year=year)
        income_time = time.time() - start_time
        
        if df_income is not None and not df_income.empty:
            print(f"  利润表: 成功 ({len(df_income)}条记录, {income_time:.3f}秒)")
            print(f"    字段数: {len(df_income.columns)}")
        else:
            print(f"  利润表: 无数据 ({income_time:.3f}秒)")
        
        # 总耗时
        total_time = connect_time + balance_time + cashflow_time + income_time
        print(f"  总耗时: {total_time:.3f}秒")
        
        # 断开连接
        datasource.disconnect()
        
        return {
            'source': source_type.value,
            'success': True,
            'connect_time': connect_time,
            'balance_time': balance_time,
            'cashflow_time': cashflow_time,
            'income_time': income_time,
            'total_time': total_time,
            'balance_records': len(df_balance) if df_balance is not None else 0,
            'cashflow_records': len(df_cashflow) if df_cashflow is not None else 0,
            'income_records': len(df_income) if df_income is not None else 0,
        }
        
    except Exception as e:
        print(f"  错误: {str(e)}")
        return {
            'source': source_type.value,
            'success': False,
            'error': str(e)
        }


def compare_all_datasources(code: str = "600000", year: int = 2023):
    """
    对比所有可用的数据源
    
    Args:
        code: 股票代码
        year: 年份
    """
    print("\n" + "="*60)
    print(f"数据源对比测试")
    print(f"股票代码: {code}, 年份: {year}")
    print("="*60)
    
    # 获取可用数据源
    available_sources = DataSourceFactory.get_available_sources()
    print(f"\n可用数据源: {[s.value for s in available_sources]}")
    
    # 测试每个数据源
    results = []
    for source_type in available_sources:
        result = benchmark_datasource(source_type, code, year)
        results.append(result)
    
    # 汇总结果
    print("\n" + "="*60)
    print("测试结果汇总")
    print("="*60)
    
    successful_sources = [r for r in results if r.get('success', False)]
    
    if successful_sources:
        print("\n成功的数据源:")
        for r in successful_sources:
            print(f"\n  {r['source']}:")
            print(f"    连接耗时: {r['connect_time']:.3f}秒")
            print(f"    查询耗时: {r['total_time'] - r['connect_time']:.3f}秒")
            print(f"    总耗时: {r['total_time']:.3f}秒")
            print(f"    数据记录: 资产负债表({r['balance_records']}), "
                  f"现金流量表({r['cashflow_records']}), "
                  f"利润表({r['income_records']})")
        
        # 找出最快的数据源
        fastest = min(successful_sources, key=lambda x: x['total_time'])
        print(f"\n最快的数据源: {fastest['source']} ({fastest['total_time']:.3f}秒)")
        
        # 找出数据最全的数据源
        most_complete = max(
            successful_sources,
            key=lambda x: x['balance_records'] + x['cashflow_records'] + x['income_records']
        )
        total_records = (most_complete['balance_records'] + 
                        most_complete['cashflow_records'] + 
                        most_complete['income_records'])
        print(f"数据最全的数据源: {most_complete['source']} ({total_records}条记录)")
    
    failed_sources = [r for r in results if not r.get('success', False)]
    if failed_sources:
        print("\n失败的数据源:")
        for r in failed_sources:
            print(f"  {r['source']}: {r.get('error', '未知错误')}")
    
    print("\n" + "="*60)
    print("建议:")
    if successful_sources:
        print(f"1. 推荐使用: {fastest['source']} (速度最快)")
        if len(successful_sources) > 1:
            print(f"2. 备用数据源: {[r['source'] for r in successful_sources if r['source'] != fastest['source']]}")
    else:
        print("1. 所有数据源都不可用，请检查网络连接和依赖安装")
    
    if DataSourceType.TUSHARE in [r['source'] for r in failed_sources if isinstance(r.get('source'), str)]:
        print("3. Tushare需要token，请访问 https://tushare.pro/register 注册获取")
    
    print("="*60 + "\n")


def test_data_consistency(code: str = "600000", year: int = 2023):
    """
    测试不同数据源的数据一致性
    
    Args:
        code: 股票代码
        year: 年份
    """
    print("\n" + "="*60)
    print("数据一致性测试")
    print("="*60)
    
    available_sources = DataSourceFactory.get_available_sources()
    
    # 收集各数据源的资产负债率数据
    liability_ratios = {}
    
    for source_type in available_sources:
        try:
            with DataSourceFactory.create(source_type) as datasource:
                df = datasource.query_balance_sheet(code=code, year=year)
                
                if df is not None and not df.empty:
                    # 尝试找到资产负债率字段
                    ratio_col = None
                    for col in df.columns:
                        if 'liability' in col.lower() and 'asset' in col.lower():
                            ratio_col = col
                            break
                    
                    if ratio_col and ratio_col in df.columns:
                        value = df[ratio_col].iloc[-1]
                        liability_ratios[source_type.value] = value
                        print(f"  {source_type.value}: 资产负债率 = {value}")
        
        except Exception as e:
            print(f"  {source_type.value}: 查询失败 - {str(e)}")
    
    if len(liability_ratios) > 1:
        print("\n数据对比:")
        values = list(liability_ratios.values())
        print(f"  数据源数量: {len(values)}")
        print(f"  数据一致: {'是' if len(set(values)) == 1 else '否'}")
    
    print("="*60 + "\n")


def main():
    """主函数"""
    # 设置日志（只显示WARNING及以上）
    setup_logger(level=30)
    
    # 对比所有数据源
    compare_all_datasources(code="600000", year=2023)
    
    # 测试数据一致性
    # test_data_consistency(code="600000", year=2023)


if __name__ == "__main__":
    main()
