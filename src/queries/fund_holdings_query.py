"""
基金持仓查询模块
用于查询基金持仓和机构偏好分析
"""

import pandas as pd
import logging
from typing import Optional, List, Dict, Union
from datetime import datetime
from ..core.base_query import BaseQuery

logger = logging.getLogger(__name__)


class FundHoldingsQuery(BaseQuery):
    """基金持仓查询类，支持多数据源"""
    
    def __init__(self, datasource=None):
        """
        初始化基金持仓查询
        
        Args:
            datasource: 数据源实例或数据源管理器。如果为None，使用默认BaoStock
        """
        super().__init__(datasource)
    
    def query_fund_holdings(
        self,
        fund_code: str,
        period: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        查询单个基金的持仓明细
        
        Args:
            fund_code: 基金代码，如 "000001"
            period: 报告期，格式如 "2024Q3" 或 "20240331"，不指定则查询最新一期
            
        Returns:
            Optional[pd.DataFrame]: 基金持仓数据
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return None
        
        try:
            # 如果使用新的数据源系统
            if self.datasource is not None:
                if hasattr(self.datasource, 'query_fund_holdings'):
                    return self.datasource.query_fund_holdings(
                        fund_code=fund_code,
                        period=period
                    )
                elif hasattr(self.datasource, 'query_with_fallback'):
                    return self.datasource.query_with_fallback(
                        'query_fund_holdings',
                        fund_code=fund_code,
                        period=period
                    )
            
            logger.error("当前数据源不支持基金持仓查询，请使用AkShare数据源")
            return None
            
        except Exception as e:
            logger.error(f"查询基金持仓失败: {str(e)}")
            return None
    
    def query_multiple_funds(
        self,
        fund_codes: List[str],
        period: Optional[str] = None
    ) -> Dict[str, Optional[pd.DataFrame]]:
        """
        批量查询多个基金的持仓
        
        Args:
            fund_codes: 基金代码列表
            period: 报告期，格式如 "2024Q3"
            
        Returns:
            Dict[str, Optional[pd.DataFrame]]: {基金代码: 持仓数据} 的字典
        """
        return self.batch_query(
            items=fund_codes,
            query_func=self.query_fund_holdings,
            period=period
        )
    
    def query_fund_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        查询基金基本信息
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            Optional[pd.DataFrame]: 基金基本信息
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return None
        
        try:
            if self.datasource is not None:
                if hasattr(self.datasource, 'query_fund_basic'):
                    return self.datasource.query_fund_basic(**kwargs)
                elif hasattr(self.datasource, 'query_with_fallback'):
                    return self.datasource.query_with_fallback(
                        'query_fund_basic',
                        **kwargs
                    )
            
            logger.error("当前数据源不支持基金基本信息查询，请使用AkShare数据源")
            return None
            
        except Exception as e:
            logger.error(f"查询基金基本信息失败: {str(e)}")
            return None
    
    def query_fund_nav(
        self,
        fund_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        查询基金净值
        
        Args:
            fund_code: 基金代码
            start_date: 开始日期，格式 "YYYY-MM-DD" 或 "YYYYMMDD"
            end_date: 结束日期，格式 "YYYY-MM-DD" 或 "YYYYMMDD"
            
        Returns:
            Optional[pd.DataFrame]: 基金净值数据
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return None
        
        try:
            if self.datasource is not None:
                if hasattr(self.datasource, 'query_fund_nav'):
                    return self.datasource.query_fund_nav(
                        fund_code=fund_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                elif hasattr(self.datasource, 'query_with_fallback'):
                    return self.datasource.query_with_fallback(
                        'query_fund_nav',
                        fund_code=fund_code,
                        start_date=start_date,
                        end_date=end_date
                    )
            
            logger.error("当前数据源不支持基金净值查询，请使用AkShare数据源")
            return None
            
        except Exception as e:
            logger.error(f"查询基金净值失败: {str(e)}")
            return None
    
    def query_institutional_holdings(
        self,
        stock_code: str,
        period: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        查询机构持仓明细
        
        Args:
            stock_code: 股票代码
            period: 报告期，格式如 "2024Q3"
            
        Returns:
            Optional[pd.DataFrame]: 机构持仓数据
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return None
        
        try:
            if self.datasource is not None:
                if hasattr(self.datasource, 'query_institutional_holdings'):
                    return self.datasource.query_institutional_holdings(
                        stock_code=stock_code,
                        period=period
                    )
                elif hasattr(self.datasource, 'query_with_fallback'):
                    return self.datasource.query_with_fallback(
                        'query_institutional_holdings',
                        stock_code=stock_code,
                        period=period
                    )
            
            logger.error("当前数据源不支持机构持仓查询，请使用AkShare数据源")
            return None
            
        except Exception as e:
            logger.error(f"查询机构持仓失败: {str(e)}")
            return None
    
    def analyze_institutional_preference(
        self,
        fund_codes: Optional[List[str]] = None,
        period: Optional[str] = None,
        top_n: int = 20
    ) -> Dict[str, pd.DataFrame]:
        """
        分析机构偏好
        
        分析多个基金的重仓股，找出机构偏好的股票和行业
        
        Args:
            fund_codes: 基金代码列表，不指定则查询所有基金
            period: 报告期，格式如 "2024Q3"
            top_n: 返回前N只重仓股
            
        Returns:
            Dict[str, pd.DataFrame]: 包含以下键的字典
                - 'top_holdings': 最受机构偏好的股票（按持有基金数量排序）
                - 'holding_changes': 持仓变化（如果有历史数据）
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return {}
        
        try:
            # 如果没有指定基金列表，先获取基金基本信息
            if fund_codes is None:
                fund_basic = self.query_fund_basic()
                if fund_basic is None or fund_basic.empty:
                    logger.warning("无法获取基金列表")
                    return {}
                # 取前100只基金作为示例（实际使用时可以根据需要调整）
                fund_codes = fund_basic['基金代码'].head(100).tolist()
            
            # 批量查询基金持仓
            all_holdings = []
            holdings_dict = self.query_multiple_funds(fund_codes, period=period)
            
            for fund_code, holdings_df in holdings_dict.items():
                if holdings_df is not None and not holdings_df.empty:
                    # 添加基金代码列
                    holdings_df = holdings_df.copy()
                    holdings_df['基金代码'] = fund_code
                    # 记录列名用于调试
                    logger.debug(f"基金 {fund_code} 持仓数据列名: {list(holdings_df.columns)}")
                    all_holdings.append(holdings_df)
            
            if not all_holdings:
                logger.warning("未获取到任何基金持仓数据")
                return {}
            
            # 合并所有持仓数据
            try:
                combined_df = pd.concat(all_holdings, ignore_index=True)
                logger.debug(f"合并后数据列名: {list(combined_df.columns)}")
            except Exception as e:
                logger.error(f"合并持仓数据失败: {str(e)}")
                # 打印每个DataFrame的列名
                for i, h in enumerate(all_holdings):
                    logger.info(f"持仓数据 {i} 列名: {list(h.columns)}")
                return {}
            
            # 分析最受机构偏好的股票
            # 统计每只股票被多少基金持有
            if '股票代码' in combined_df.columns:
                stock_col = '股票代码'
            elif '代码' in combined_df.columns:
                stock_col = '代码'
            elif '股票名称' in combined_df.columns:
                stock_col = '股票名称'
            else:
                logger.warning("无法识别持仓数据中的股票代码列")
                return {}
            
            # 统计每只股票被持有的基金数量
            stock_counts = combined_df.groupby(stock_col).size().reset_index(name='持有基金数')
            stock_counts = stock_counts.sort_values('持有基金数', ascending=False)
            
            # 统一股票代码列名
            if stock_col != '股票代码':
                stock_counts = stock_counts.rename(columns={stock_col: '股票代码'})
            
            # 查找并添加股票名称列
            stock_name_col = None
            for col in ['股票名称', '名称', 'name', '股票简称']:
                if col in combined_df.columns:
                    stock_name_col = col
                    logger.info(f"找到股票名称列: {stock_name_col}")
                    break
            
            if stock_name_col:
                try:
                    # 获取每只股票的名称（取第一个出现的名称）
                    stock_names = combined_df.groupby(stock_col)[stock_name_col].first().reset_index()
                    if stock_col != '股票代码':
                        stock_names = stock_names.rename(columns={stock_col: '股票代码'})
                    if stock_name_col != '股票名称':
                        stock_names = stock_names.rename(columns={stock_name_col: '股票名称'})
                    stock_counts = stock_counts.merge(stock_names, on='股票代码', how='left')
                    logger.info(f"成功添加股票名称，共 {stock_counts['股票名称'].notna().sum()} 条有名称")
                except Exception as e:
                    logger.warning(f"添加股票名称失败: {str(e)}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    # 如果合并失败，添加空列
                    stock_counts['股票名称'] = 'N/A'
            else:
                logger.warning(f"未找到股票名称列，可用列: {list(combined_df.columns)}")
                # 如果没有股票名称列，添加一个空列
                stock_counts['股票名称'] = 'N/A'
            
            # 计算持仓市值总和（如果有市值列）
            # 尝试多种可能的列名
            market_value_col = None
            for col in ['持仓市值', '持仓市值(万元)', '市值', 'market_value']:
                if col in combined_df.columns:
                    market_value_col = col
                    break
            
            if market_value_col:
                try:
                    stock_market_value = combined_df.groupby(stock_col)[market_value_col].sum().reset_index()
                    if stock_col != '股票代码':
                        stock_market_value = stock_market_value.rename(columns={stock_col: '股票代码'})
                    stock_market_value = stock_market_value.rename(columns={market_value_col: '持仓市值'})
                    stock_counts = stock_counts.merge(stock_market_value, on='股票代码', how='left')
                except Exception as e:
                    logger.warning(f"计算持仓市值失败: {str(e)}")
            
            top_holdings = stock_counts.head(top_n)
            
            # 调整列的顺序：股票代码、股票名称、持有基金数、持仓市值（如果有）
            column_order = ['股票代码']
            if '股票名称' in top_holdings.columns:
                column_order.append('股票名称')
            column_order.append('持有基金数')
            if '持仓市值' in top_holdings.columns:
                column_order.append('持仓市值')
            
            # 只保留存在的列
            column_order = [col for col in column_order if col in top_holdings.columns]
            # 添加其他列（如果有）
            other_cols = [col for col in top_holdings.columns if col not in column_order]
            column_order.extend(other_cols)
            
            top_holdings = top_holdings[column_order]
            
            return {
                'top_holdings': top_holdings,
                'all_holdings': combined_df
            }
            
        except Exception as e:
            logger.error(f"分析机构偏好失败: {str(e)}")
            return {}
    
    def compare_fund_holdings(
        self,
        fund_codes: List[str],
        period: Optional[str] = None
    ) -> pd.DataFrame:
        """
        对比多个基金的持仓
        
        Args:
            fund_codes: 基金代码列表
            period: 报告期
            
        Returns:
            pd.DataFrame: 对比结果，包含共同持仓和差异持仓
        """
        holdings_dict = self.query_multiple_funds(fund_codes, period=period)
        
        # 提取每只基金的重仓股
        fund_stocks = {}
        for fund_code, holdings_df in holdings_dict.items():
            if holdings_df is not None and not holdings_df.empty:
                # 尝试识别股票代码列
                if '股票代码' in holdings_df.columns:
                    stocks = holdings_df['股票代码'].tolist()
                elif '代码' in holdings_df.columns:
                    stocks = holdings_df['代码'].tolist()
                else:
                    continue
                fund_stocks[fund_code] = set(stocks)
        
        # 找出共同持仓
        if len(fund_stocks) < 2:
            return pd.DataFrame()
        
        common_stocks = set.intersection(*fund_stocks.values())
        
        # 构建对比结果
        comparison_data = []
        for fund_code, stocks in fund_stocks.items():
            comparison_data.append({
                '基金代码': fund_code,
                '持仓股票数': len(stocks),
                '共同持仓数': len(common_stocks)
            })
        
        return pd.DataFrame(comparison_data)
    
    def query(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        默认查询方法，查询基金持仓
        
        Args:
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            Optional[pd.DataFrame]: 查询结果
        """
        fund_code = kwargs.get('fund_code') or (args[0] if args else None)
        period = kwargs.get('period')
        
        if fund_code:
            return self.query_fund_holdings(fund_code=fund_code, period=period)
        else:
            return self.query_fund_basic(**kwargs)
