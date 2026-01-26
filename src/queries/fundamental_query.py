"""
股票基本面查询模块
"""

import pandas as pd
import logging
from typing import Optional, List, Dict, Union
from ..core.base_query import BaseQuery

logger = logging.getLogger(__name__)


class FundamentalQuery(BaseQuery):
    """股票基本面查询类，支持多数据源"""
    
    def __init__(self, datasource=None):
        """
        初始化基本面查询
        
        Args:
            datasource: 数据源实例或数据源管理器。如果为None，使用默认BaoStock
        """
        super().__init__(datasource)
    
    def query(
        self,
        code: str,
        year: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询单个公司的基本面数据
        
        Args:
            code: 股票代码，如 "600519" 或 "sh.600519"
            year: 年份，如 2023。不指定则查询全部数据
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 基本面数据
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return None
        
        try:
            # 如果使用新的数据源系统
            if self.datasource is not None:
                return self._query_with_datasource(code, year, **kwargs)
            
            # BaoStock不直接支持基本面查询，需要使用其他数据源
            logger.warning("BaoStock不支持基本面查询，请使用AkShare或Tushare数据源")
            return None
            
        except Exception as e:
            logger.error(f"查询 {code} 基本面数据时出错: {str(e)}")
            return None
    
    def _query_with_datasource(
        self,
        code: str,
        year: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """使用新数据源系统查询"""
        from ..datasource.datasource_manager import DataSourceManager
        
        # 如果是数据源管理器，使用故障转移查询
        if isinstance(self.datasource, DataSourceManager):
            df = self.datasource.query_with_fallback(
                'query_stock_fundamental',
                code=code,
                year=year,
                **kwargs
            )
        else:
            # 单个数据源
            if hasattr(self.datasource, 'query_stock_fundamental'):
                df = self.datasource.query_stock_fundamental(
                    code=code,
                    year=year,
                    **kwargs
                )
            else:
                logger.error(f"数据源 {self.datasource.get_source_name()} 不支持基本面查询")
                return None
        
        if df is not None and not df.empty:
            logger.info(f"成功查询 {code} 的基本面数据，共 {len(df)} 条记录")
        
        return df
    
    def query_multiple(
        self,
        codes: List[str],
        year: Optional[int] = None,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        批量查询多个公司的基本面数据
        
        Args:
            codes: 股票代码列表
            year: 年份
            **kwargs: 其他参数
            
        Returns:
            dict: {股票代码: DataFrame} 的字典
        """
        logger.info(f"开始批量查询 {len(codes)} 个公司的基本面数据")
        
        results = {}
        for code in codes:
            try:
                result = self.query(code=code, year=year, **kwargs)
                results[code] = result
            except Exception as e:
                logger.error(f"查询 {code} 时出错: {str(e)}")
                results[code] = None
        
        success_count = sum(1 for v in results.values() if v is not None and not v.empty)
        logger.info(f"批量查询完成，成功 {success_count}/{len(codes)}")
        
        return results
    
    def query_by_names(
        self,
        names: List[str],
        year: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        根据公司名称批量查询基本面数据，并汇总为一个表格
        
        Args:
            names: 公司名称列表，如 ["贵州茅台", "五粮液", "泸州老窖"]
            year: 年份，如 2023。不指定则查询最近一期
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 汇总的基本面信息表，包含所有公司的关键指标
        """
        logger.info(f"根据 {len(names)} 个公司名称查询基本面数据")
        
        # 第一步：获取股票基本信息，建立名称到代码的映射
        name_to_code = self._get_name_to_code_mapping(names)
        
        if not name_to_code:
            logger.error("无法获取股票代码映射")
            return pd.DataFrame()
        
        logger.info(f"成功匹配 {len(name_to_code)} 个公司代码")
        
        # 第二步：批量查询基本面数据
        codes = list(name_to_code.values())
        results = self.query_multiple(codes=codes, year=year, **kwargs)
        
        # 第三步：提取关键指标并汇总
        summary_data = []
        
        for name, code in name_to_code.items():
            df = results.get(code)
            
            if df is None or df.empty:
                logger.warning(f"{name}({code}): 无数据")
                continue
            
            # 获取最近一期数据
            latest = df.iloc[-1] if not df.empty else None
            
            if latest is not None:
                # 提取关键指标
                row_data = {
                    '股票代码': code,
                    '公司名称': name,
                    '报告期': latest.get('报告期', 'N/A'),
                }
                
                # 添加常见的基本面指标
                indicators = [
                    '净利润', '净利润(亿元)', '营业总收入', '营业总收入(亿元)',
                    '净资产收益率', '净资产收益率(%)', 'ROE', 
                    '毛利率', '毛利率(%)',
                    '总资产收益率', '总资产收益率(%)', 'ROA',
                    '资产负债率', '资产负债率(%)',
                    '每股收益', '每股收益(元)',
                    '每股净资产', '每股净资产(元)',
                    '市盈率', 'PE',
                    '市净率', 'PB',
                ]
                
                for indicator in indicators:
                    if indicator in latest.index:
                        row_data[indicator] = latest[indicator]
                
                summary_data.append(row_data)
        
        if not summary_data:
            logger.warning("未能提取到任何基本面数据")
            return pd.DataFrame()
        
        # 创建汇总表
        summary_df = pd.DataFrame(summary_data)
        logger.info(f"成功创建基本面汇总表，共 {len(summary_df)} 家公司")
        
        return summary_df
    
    def _get_name_to_code_mapping(self, names: List[str]) -> Dict[str, str]:
        """
        获取公司名称到股票代码的映射
        
        Args:
            names: 公司名称列表
            
        Returns:
            dict: {公司名称: 股票代码} 的字典
        """
        if not self.ensure_connection():
            logger.error("无法连接到数据源")
            return {}
        
        try:
            # 获取股票基本信息
            if self.datasource is not None:
                from ..datasource.datasource_manager import DataSourceManager
                
                if isinstance(self.datasource, DataSourceManager):
                    stock_info = self.datasource.query_with_fallback('query_stock_basic')
                else:
                    stock_info = self.datasource.query_stock_basic()
            else:
                logger.error("需要数据源支持才能查询股票基本信息")
                return {}
            
            if stock_info is None or stock_info.empty:
                logger.error("无法获取股票基本信息")
                return {}
            
            # 建立名称到代码的映射
            name_to_code = {}
            
            # 尝试不同的列名
            code_col = None
            name_col = None
            
            for col in ['code', '代码', 'symbol', '股票代码']:
                if col in stock_info.columns:
                    code_col = col
                    break
            
            for col in ['name', '名称', 'stock_name', '股票名称', '简称']:
                if col in stock_info.columns:
                    name_col = col
                    break
            
            if not code_col or not name_col:
                logger.error(f"无法识别股票信息表的列名，可用列: {list(stock_info.columns)}")
                return {}
            
            # 匹配公司名称
            for name in names:
                # 精确匹配
                matched = stock_info[stock_info[name_col] == name]
                
                # 如果精确匹配失败，尝试模糊匹配
                if matched.empty:
                    matched = stock_info[stock_info[name_col].str.contains(name, na=False)]
                
                if not matched.empty:
                    code = str(matched.iloc[0][code_col])
                    name_to_code[name] = code
                    logger.info(f"匹配成功: {name} -> {code}")
                else:
                    logger.warning(f"未找到公司: {name}")
            
            return name_to_code
            
        except Exception as e:
            logger.error(f"获取股票代码映射时出错: {str(e)}")
            return {}
