from monitor_fund_preferences import FundPreferenceMonitor

# 创建监测器实例
monitor = FundPreferenceMonitor()

# 测试按规模排序的方法
print("=" * 80)
print("测试 monitor_top_funds_by_scale 方法")
print("=" * 80)

top_holdings, fund_lists_dict = monitor.monitor_top_funds_by_scale(top_n=20)

print("\n" + "=" * 80)
print("测试完成")
print("=" * 80)