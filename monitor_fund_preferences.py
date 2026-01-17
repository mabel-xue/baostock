"""
定期监测基金资金动向和机构偏好
可以设置为定时任务（cron job）定期运行
"""

import sys
import os
from datetime import datetime
import pandas as pd
import logging
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.datasource import DataSourceManager
from src.queries.fund_holdings_query import FundHoldingsQuery

# 配置日志
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f"fund_monitoring_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class FundPreferenceMonitor:
    """基金偏好监测器"""
    
    def __init__(self, config=None):
        """
        初始化监测器
        
        Args:
            config: 数据源配置，默认使用AkShare
        """
        if config is None:
            config = {
                'default_source': 'akshare',
                'fallback_sources': [],
                'sources_config': {'akshare': {}}
            }
        self.config = config
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
    
    def get_current_period(self) -> str:
        """
        获取当前报告期
        
        Returns:
            str: 报告期字符串，格式如 "2024Q3"
        """
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        return f"{now.year}Q{quarter}"
    
    def monitor_top_funds(self, top_n: int = 20):
        """
        监测排名靠前的基金
        
        从近3年、近2年、近1年分别取前N个基金，剔除多收费类型基金（只保留A类），
        然后三组去重合并，用于分析机构偏好
        
        Args:
            top_n: 每个时间周期监测前N只基金，默认20
            
        Returns:
            tuple: (top_holdings_df, fund_lists_dict)
                - top_holdings_df: 机构偏好股票DataFrame
                - fund_lists_dict: 包含三组基金列表的字典，格式为 {period: [(序号, 基金代码, 基金名称, 收益率), ...]}
        """
        logger.info(f"开始监测近3年、近2年、近1年各前 {top_n} 只基金的资金动向...")
        
        with DataSourceManager(self.config) as manager:
            query = FundHoldingsQuery(datasource=manager)
            datasource = manager.get_datasource()
            
            all_fund_codes = []  # 存储所有去重后的基金代码
            fund_lists_dict = {}  # 存储三组基金列表（带序号）
            
            # 定义要查询的时间周期
            periods = ['近3年', '近2年', '近1年']
            
            for period in periods:
                logger.info(f"\n{'='*60}")
                logger.info(f"查询 {period} 收益率前 {top_n} 的股票型基金...")
                logger.info(f"{'='*60}")
                
                # 使用 query_fund_rank 获取基金排名，指定股票型基金
                fund_rank_df = datasource.query_fund_rank(top_n=top_n*2, sort_by=period, fund_type='股票型')  # 多取一些，以便过滤后还有足够数量
                
                if fund_rank_df is None or fund_rank_df.empty:
                    logger.warning(f"未查询到 {period} 的基金排名数据")
                    fund_lists_dict[period] = []
                    continue
                
                # 剔除多收费类型基金，只保留A类
                # 基金代码列可能是 '基金代码' 或 '代码'
                code_col = '基金代码' if '基金代码' in fund_rank_df.columns else '代码'
                name_col = '基金简称' if '基金简称' in fund_rank_df.columns else '基金名称'
                
                # 过滤：只保留A类基金，或者没有收费类型后缀的基金
                # 排除B类、C类、E类等
                filtered_df = fund_rank_df[
                    ~fund_rank_df[name_col].str.contains('B$|C$|E$|D$|H$', regex=True, na=False) |
                    fund_rank_df[name_col].str.contains('A$', regex=True, na=False)
                ].copy()
                
                # 取前top_n个
                filtered_df = filtered_df.head(top_n)
                
                # 提取基金代码和名称，并添加序号
                period_fund_list = []
                for idx, row in filtered_df.iterrows():
                    fund_code = row[code_col]
                    fund_name = row[name_col]
                    return_rate = row.get(period, 'N/A')
                    period_fund_list.append((len(period_fund_list) + 1, fund_code, fund_name, return_rate))
                
                fund_lists_dict[period] = period_fund_list
                period_fund_codes = [item[1] for item in period_fund_list]  # 提取基金代码
                
                # 打印当前周期的基金数据
                print(f"\n{'='*80}")
                print(f"{period} 收益率前 {len(period_fund_codes)} 只股票型基金（已过滤，只保留A类）")
                print(f"{'='*80}")
                
                # 显示关键列
                display_cols = [code_col, name_col, period]
                if all(col in filtered_df.columns for col in display_cols):
                    print(filtered_df[display_cols].to_string(index=False))
                else:
                    print(filtered_df.to_string(index=False))
                
                logger.info(f"{period} 筛选出 {len(period_fund_codes)} 只基金")
                
                # 添加到总列表
                all_fund_codes.extend(period_fund_codes)
            
            # 去重合并
            unique_fund_codes = list(dict.fromkeys(all_fund_codes))  # 保持顺序的去重
            
            print(f"\n{'='*80}")
            print(f"三组基金去重合并结果")
            print(f"{'='*80}")
            print(f"原始总数: {len(all_fund_codes)} 只")
            print(f"去重后: {len(unique_fund_codes)} 只")
            print(f"基金代码列表: {unique_fund_codes}")
            
            logger.info(f"去重后共 {len(unique_fund_codes)} 只基金用于分析")
            
            if not unique_fund_codes:
                logger.error("未获取到任何基金数据")
                return None, fund_lists_dict
            
            # 使用合并后的基金列表分析机构偏好
            logger.info(f"\n开始分析 {len(unique_fund_codes)} 只基金的机构偏好...")
            preference_result = query.analyze_institutional_preference(
                fund_codes=unique_fund_codes,
                period=None,  # 使用最新数据
                top_n=30
            )
            
            if 'top_holdings' in preference_result and not preference_result['top_holdings'].empty:
                # 保存结果
                output_file = self.output_dir / f"institutional_preference_{datetime.now().strftime('%Y%m%d')}.csv"
                preference_result['top_holdings'].to_csv(output_file, index=False, encoding='utf-8-sig')
                logger.info(f"机构偏好分析结果已保存至: {output_file}")
                
                # 打印Top 10
                print("\n" + "=" * 80)
                print("机构偏好股票 Top 10")
                print("=" * 80)
                print(preference_result['top_holdings'].head(10).to_string())
                
                return preference_result['top_holdings'], fund_lists_dict
            else:
                logger.warning("未获取到机构偏好数据")
                return None, fund_lists_dict
    
    def monitor_fund_holdings_changes(self, fund_codes: list, compare_period: str = None):
        """
        监测基金持仓变化
        
        Args:
            fund_codes: 基金代码列表
            compare_period: 对比的报告期（可选）
        """
        logger.info(f"监测 {len(fund_codes)} 只基金的持仓变化...")
        
        with DataSourceManager(self.config) as manager:
            query = FundHoldingsQuery(datasource=manager)
            
            # 查询当前持仓
            current_holdings = query.query_multiple_funds(fund_codes, period=None)
            
            # 如果有对比期，查询历史持仓
            if compare_period:
                historical_holdings = query.query_multiple_funds(fund_codes, period=compare_period)
                # 这里可以进一步分析持仓变化
                logger.info("持仓变化分析功能待完善...")
            
            return current_holdings
    
    def generate_report(self):
        """
        生成监测报告
        """
        logger.info("生成基金资金动向监测报告...")
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append(f"基金资金动向监测报告")
        report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # 监测机构偏好
        top_holdings, fund_lists_dict = self.monitor_top_funds(top_n=20)
        
        # 添加三组基金列表到报告
        report_lines.append("\n【监测基金列表】")
        report_lines.append("-" * 80)
        
        periods = ['近3年', '近2年', '近1年']
        for period in periods:
            if period in fund_lists_dict and fund_lists_dict[period]:
                report_lines.append(f"\n{period} 收益率前 {len(fund_lists_dict[period])} 只股票型基金（已过滤，只保留A类）:")
                report_lines.append("-" * 80)
                for seq, fund_code, fund_name, return_rate in fund_lists_dict[period]:
                    report_lines.append(f"  {seq}. {fund_code} {fund_name} ({return_rate})")
            else:
                report_lines.append(f"\n{period}: 未查询到数据")
        
        # 统计去重后的基金数量
        all_fund_codes = []
        for period_list in fund_lists_dict.values():
            all_fund_codes.extend([item[1] for item in period_list])
        unique_count = len(set(all_fund_codes))
        report_lines.append(f"\n三组基金去重合并: 原始总数 {len(all_fund_codes)} 只，去重后 {unique_count} 只")
        
        if top_holdings is not None and not top_holdings.empty:
            report_lines.append("\n【机构偏好分析】")
            report_lines.append("-" * 80)
            report_lines.append("最受机构偏好的前10只股票:")
            for idx, row in top_holdings.head(10).iterrows():
                stock_col = '股票代码' if '股票代码' in row else '代码'
                count_col = '持有基金数'
                stock_code = row.get(stock_col, 'N/A')
                fund_count = row.get(count_col, 0)
                stock_name = row.get('股票名称', 'N/A')
                report_lines.append(f"  {idx+1}. {stock_code} {stock_name}: 被 {fund_count} 只基金持有")
        
        # 保存报告
        report_file = self.output_dir / f"fund_report_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"报告已保存至: {report_file}")
        
        # 打印报告
        print('\n'.join(report_lines))
        
        return report_file


def main():
    """主函数"""
    print("=" * 80)
    print("基金资金动向定期监测系统")
    print("=" * 80)
    
    monitor = FundPreferenceMonitor()
    
    try:
        # 生成完整报告
        report_file = monitor.generate_report()
        
        print(f"\n监测完成！报告已保存至: {report_file}")
        
    except Exception as e:
        logger.error(f"监测过程出错: {str(e)}", exc_info=True)
        print(f"\n错误: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
