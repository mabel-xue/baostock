"""
现金流量查询模块
"""

import pandas as pd
import baostock as bs
import logging
from typing import Optional, List, Union
from datetime import datetime
from core.base_query import BaseQuery

logger = logging.getLogger(__name__)


class CashFlowQuery(BaseQuery):
    """现金流量查询类，支持多数据源"""
    
    def __init__(self, datasource=None):
        """
        初始化现金流量查询
        
        Args:
            datasource: 数据源实例或数据源管理器。如果为None，使用默认BaoStock
        """
        super().__init__(datasource)
    
    def query(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        查询单个公司的现金流量数据
        
        Args:
            code: 股票代码，如 "sh.600000" 或 "sz.000001"
            year: 年份，如 2023。不指定则查询最近一期
            quarter: 季度，1-4。不指定则查询年度数据
            
        Returns:
            Optional[pd.DataFrame]: 现金流量数据
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return None
        
        try:
            # 如果使用新的数据源系统
            if self.datasource is not None:
                return self._query_with_datasource(code, year, quarter)
            
            # 使用传统的BaoStock查询
            return self._query_with_baostock(code, year, quarter)
            
        except Exception as e:
            logger.error(f"查询 {code} 现金流量数据时出错: {str(e)}")
            return None
    
    def _query_with_datasource(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """使用新数据源系统查询"""
        from datasource.datasource_manager import DataSourceManager
        
        # 如果是数据源管理器，使用故障转移查询
        if isinstance(self.datasource, DataSourceManager):
            df = self.datasource.query_with_fallback(
                'query_cash_flow',
                code=code,
                year=year,
                quarter=quarter
            )
        else:
            # 单个数据源
            df = self.datasource.query_cash_flow(
                code=code,
                year=year,
                quarter=quarter
            )
        
        if df is not None and not df.empty:
            logger.info(f"成功查询 {code} 的现金流量数据，共 {len(df)} 条记录")
            df = self._convert_dtypes(df)
        
        return df
    
    def _query_with_baostock(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """使用传统BaoStock查询"""
        # 标准化股票代码
        code = self._normalize_code(code)
        
        # 构建查询参数
        if year and quarter:
            rs = bs.query_cash_flow_data(code=code, year=year, quarter=quarter)
        elif year:
            rs = bs.query_cash_flow_data(code=code, year=year, quarter=4)
        else:
            current_year = datetime.now().year
            rs = bs.query_cash_flow_data(code=code, year=current_year, quarter=4)
        
        df = self._result_to_dataframe(rs)
        
        if df is not None and not df.empty:
            logger.info(f"成功查询 {code} 的现金流量数据，共 {len(df)} 条记录")
            df = self._convert_dtypes(df)
        
        return df
    
    def query_multiple(
        self,
        codes: List[str],
        year: Optional[int] = None,
        quarter: Optional[int] = None
    ) -> dict:
        """
        批量查询多个公司的现金流量数据
        
        Args:
            codes: 股票代码列表
            year: 年份
            quarter: 季度
            
        Returns:
            dict: {股票代码: DataFrame} 的字典
        """
        logger.info(f"开始批量查询 {len(codes)} 个公司的现金流量数据")
        
        results = self.batch_query(
            items=codes,
            query_func=self.query,
            year=year,
            quarter=quarter
        )
        
        success_count = sum(1 for v in results.values() if v is not None and not v.empty)
        logger.info(f"批量查询完成，成功 {success_count}/{len(codes)}")
        
        return results
    
    def query_history(
        self,
        code: str,
        start_year: int,
        end_year: Optional[int] = None,
        quarter: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        查询历史现金流量数据
        
        Args:
            code: 股票代码
            start_year: 起始年份
            end_year: 结束年份，不指定则到当前年份
            quarter: 季度，不指定则查询年度数据
            
        Returns:
            Optional[pd.DataFrame]: 合并后的历史数据
        """
        if end_year is None:
            end_year = datetime.now().year
        
        if start_year > end_year:
            logger.error("起始年份不能大于结束年份")
            return None
        
        logger.info(f"查询 {code} 从 {start_year} 到 {end_year} 的现金流量数据")
        
        all_data = []
        for year in range(start_year, end_year + 1):
            df = self.query(code, year=year, quarter=quarter)
            if df is not None and not df.empty:
                all_data.append(df)
        
        if not all_data:
            logger.warning(f"未查询到 {code} 的历史数据")
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"成功查询 {code} 的历史数据，共 {len(result)} 条记录")
        
        return result
    
    def compare_companies(
        self,
        codes: List[str],
        year: int,
        quarter: Optional[int] = None,
        metrics: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        对比多个公司的现金流量指标
        
        Args:
            codes: 股票代码列表
            year: 年份
            quarter: 季度
            metrics: 要对比的指标列表，不指定则使用默认指标
            
        Returns:
            Optional[pd.DataFrame]: 对比结果
        """
        # 默认对比指标
        if metrics is None:
            metrics = [
                'CAToAsset',  # 经营活动产生的现金流量净额
                'NCAToAsset',  # 投资活动产生的现金流量净额
                'NCFFromFAToAsset',  # 筹资活动产生的现金流量净额
            ]
        
        results = self.query_multiple(codes, year=year, quarter=quarter)
        
        comparison_data = []
        for code, df in results.items():
            if df is not None and not df.empty:
                row = {'code': code}
                for metric in metrics:
                    if metric in df.columns:
                        # 取最新一条记录
                        row[metric] = df.iloc[-1][metric]
                    else:
                        row[metric] = None
                comparison_data.append(row)
        
        if not comparison_data:
            logger.warning("没有可对比的数据")
            return pd.DataFrame()
        
        result = pd.DataFrame(comparison_data)
        logger.info(f"成功对比 {len(result)} 个公司的现金流量数据")
        
        return result
    
    @staticmethod
    def _normalize_code(code: str) -> str:
        """
        标准化股票代码
        
        Args:
            code: 股票代码
            
        Returns:
            str: 标准化后的代码
        """
        code = code.strip().lower()
        
        # 如果已经包含市场前缀，直接返回
        if code.startswith('sh.') or code.startswith('sz.'):
            return code
        
        # 根据代码判断市场
        if code.startswith('6'):
            return f'sh.{code}'
        elif code.startswith(('0', '3')):
            return f'sz.{code}'
        else:
            logger.warning(f"无法识别股票代码 {code} 的市场，默认使用上海市场")
            return f'sh.{code}'
    
    @staticmethod
    def _convert_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """
        转换数据类型
        
        Args:
            df: 原始DataFrame
            
        Returns:
            pd.DataFrame: 转换后的DataFrame
        """
        # 数值列转换为float
        numeric_columns = [
            'CAToAsset', 'NCAToAsset', 'NCFFromFAToAsset',
            'CCE', 'CCEAdd', 'FCFF', 'FCFE'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
