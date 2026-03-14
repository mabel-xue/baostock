"""快速验证 AkShare API 接口"""
import akshare as ak

try:
    print("正在测试 stock_financial_analysis_indicator 接口...")
    print("测试股票: 600519 (贵州茅台)")
    print("start_year: 2025 (默认值)")
    
    # 测试接口 - 需要传入 start_year 参数
    df = ak.stock_financial_analysis_indicator(symbol="600519", start_year="2025")
    
    if df is not None and not df.empty:
        print(f"\n✓ 接口有效！")
        print(f"返回数据行数: {len(df)}")
        print(f"返回数据列数: {len(df.columns)}")
        
        # 显示前5条数据
        print(f"\n前5条数据:")
        print(df.head())
        
        # 取最新一期数据（最后一条）
        latest = df.iloc[-1]
        print(f"\n最新一期数据:")
        print(f"报告期: {latest.get('日期', 'N/A')}")
        print(f"摊薄每股收益: {latest.get('摊薄每股收益(元)', 'N/A')}")
        print(f"净资产收益率: {latest.get('净资产收益率(%)', 'N/A')}")
        
        print(f"\n列名前10个: {list(df.columns)[:10]}")
    else:
        print("✗ 接口返回空数据")
        
except AttributeError as e:
    print(f"✗ 接口不存在: {e}")
    print("提示: 可能需要升级 akshare: pip install --upgrade akshare")
except Exception as e:
    print(f"✗ 调用失败: {e}")
    import traceback
    traceback.print_exc()
