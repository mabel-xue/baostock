"""
多数据源使用示例
演示如何使用不同的数据源进行查询
"""

import sys
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datasource import DataSourceFactory, DataSourceManager, DataSourceType
from queries.balance_query import BalanceQuery
from queries.cashflow_query import CashFlowQuery
from utils.logger import setup_logger


def example_single_datasource():
    """示例1: 使用单个数据源"""
    print("\n" + "="*60)
    print("示例1: 使用单个BaoStock数据源")
    print("="*60)
    
    # 创建BaoStock数据源
    datasource = DataSourceFactory.create(DataSourceType.BAOSTOCK)
    
    try:
        # 使用数据源创建查询对象
        balance_query = BalanceQuery(datasource=datasource)
        
        # 查询浦发银行的资产负债表
        df = balance_query.query(code="600000", year=2023)
        
        if df is not None and not df.empty:
            print(f"\n浦发银行2023年资产负债表数据:")
            print(df[['code', 'pubDate', 'statDate', 'liabilityToAsset']].head())
        else:
            print("未查询到数据")
    
    finally:
        datasource.disconnect()


def example_datasource_manager():
    """示例2: 使用数据源管理器"""
    print("\n" + "="*60)
    print("示例2: 使用数据源管理器（自动故障转移）")
    print("="*60)
    
    # 配置数据源管理器
    config = {
        'default_source': 'baostock',
        'fallback_sources': [],  # 可以添加 'tushare', 'akshare' 作为备用
        'sources_config': {
            'baostock': {},
            # 如果有Tushare token，可以添加：
            # 'tushare': {'token': 'your_token_here'}
        }
    }
    
    # 创建数据源管理器
    with DataSourceManager(config) as manager:
        # 使用管理器创建查询对象
        cashflow_query = CashFlowQuery(datasource=manager)
        
        # 查询现金流量数据
        df = cashflow_query.query(code="600000", year=2023)
        
        if df is not None and not df.empty:
            print(f"\n浦发银行2023年现金流量数据:")
            print(df[['code', 'pubDate', 'statDate', 'CAToAsset']].head())
        else:
            print("未查询到数据")


def example_compare_datasources():
    """示例3: 对比不同数据源的查询结果"""
    print("\n" + "="*60)
    print("示例3: 对比不同数据源")
    print("="*60)
    
    code = "600000"
    year = 2023
    
    # 获取可用的数据源
    available_sources = DataSourceFactory.get_available_sources()
    print(f"\n可用的数据源: {[s.value for s in available_sources]}")
    
    # 使用BaoStock查询
    print(f"\n使用BaoStock查询 {code}:")
    try:
        with DataSourceFactory.create(DataSourceType.BAOSTOCK) as datasource:
            query = BalanceQuery(datasource=datasource)
            df = query.query(code=code, year=year)
            if df is not None and not df.empty:
                print(f"  查询成功，共 {len(df)} 条记录")
                print(f"  数据源: {datasource.get_source_name()}")
            else:
                print("  未查询到数据")
    except Exception as e:
        print(f"  查询失败: {str(e)}")
    
    # 如果安装了Tushare，可以尝试
    if DataSourceType.TUSHARE in available_sources:
        print(f"\n使用Tushare查询 {code}:")
        try:
            config = {'token': ''}  # 需要提供token
            with DataSourceFactory.create(DataSourceType.TUSHARE, config) as datasource:
                query = BalanceQuery(datasource=datasource)
                df = query.query(code=code, year=year)
                if df is not None and not df.empty:
                    print(f"  查询成功，共 {len(df)} 条记录")
                    print(f"  数据源: {datasource.get_source_name()}")
                else:
                    print("  未查询到数据")
        except Exception as e:
            print(f"  查询失败: {str(e)}")


def example_switch_datasource():
    """示例4: 动态切换数据源"""
    print("\n" + "="*60)
    print("示例4: 动态切换数据源")
    print("="*60)
    
    config = {
        'default_source': 'baostock',
        'fallback_sources': [],
    }
    
    with DataSourceManager(config) as manager:
        query = CashFlowQuery(datasource=manager)
        
        # 使用默认数据源查询
        print(f"\n当前默认数据源: {manager.default_source_type.value}")
        df = query.query(code="600000", year=2023)
        if df is not None and not df.empty:
            print(f"查询成功，共 {len(df)} 条记录")
        
        # 如果有其他数据源，可以切换
        # manager.set_default_source(DataSourceType.TUSHARE)
        # print(f"\n切换后的数据源: {manager.default_source_type.value}")


def example_batch_query_with_manager():
    """示例5: 使用管理器批量查询"""
    print("\n" + "="*60)
    print("示例5: 批量查询多个公司")
    print("="*60)
    
    config = {
        'default_source': 'baostock',
        'fallback_sources': [],
    }
    
    with DataSourceManager(config) as manager:
        query = CashFlowQuery(datasource=manager)
        
        # 批量查询
        codes = ["600000", "601398", "600519"]
        results = query.query_multiple(codes=codes, year=2023)
        
        print(f"\n批量查询结果:")
        for code, df in results.items():
            if df is not None and not df.empty:
                print(f"  {code}: 成功，{len(df)} 条记录")
            else:
                print(f"  {code}: 未查询到数据")


def example_with_config_file():
    """示例6: 从配置文件加载"""
    print("\n" + "="*60)
    print("示例6: 从配置文件加载数据源配置")
    print("="*60)
    
    try:
        from config import ConfigLoader
        
        # 尝试加载配置文件
        config_file = Path(__file__).parent.parent / "config.json"
        
        if config_file.exists():
            config = ConfigLoader.load_from_file(str(config_file))
            print(f"\n从配置文件加载成功")
            print(f"默认数据源: {config.get('default_source')}")
            print(f"备用数据源: {config.get('fallback_sources')}")
        else:
            print(f"\n配置文件不存在: {config_file}")
            print("可以复制 config.example.json 为 config.json 并修改配置")
            
            # 使用默认配置
            from config import get_default_config
            config = get_default_config()
        
        # 使用配置创建管理器
        with DataSourceManager(config) as manager:
            query = BalanceQuery(datasource=manager)
            df = query.query(code="600000", year=2023)
            
            if df is not None and not df.empty:
                print(f"\n查询成功，共 {len(df)} 条记录")
    
    except ImportError:
        print("\n配置模块不可用")


def main():
    """主函数"""
    # 设置日志
    setup_logger(level=20)  # INFO级别
    
    print("\n" + "="*60)
    print("多数据源使用示例")
    print("="*60)
    
    # 运行各个示例
    example_single_datasource()
    example_datasource_manager()
    example_compare_datasources()
    example_switch_datasource()
    example_batch_query_with_manager()
    example_with_config_file()
    
    print("\n" + "="*60)
    print("所有示例执行完成")
    print("\n提示:")
    print("1. 默认使用BaoStock数据源（无需token）")
    print("2. 如需使用Tushare，请安装: pip install tushare")
    print("   并在配置中提供token: https://tushare.pro/register")
    print("3. 如需使用AkShare，请安装: pip install akshare")
    print("4. 可以通过配置文件或环境变量设置数据源")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
