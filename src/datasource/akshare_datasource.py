"""
AkShare数据源适配器
需要安装: pip install akshare
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .base_datasource import BaseDataSource, DataSourceType

logger = logging.getLogger(__name__)

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    logger.warning("AkShare未安装，请运行: pip install akshare")


class AkShareDataSource(BaseDataSource):
    """AkShare数据源实现"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.source_type = DataSourceType.AKSHARE
        
        if not AKSHARE_AVAILABLE:
            raise ImportError("AkShare未安装，请运行: pip install akshare")
    
    def connect(self) -> bool:
        """连接到AkShare（AkShare无需登录）"""
        if self._is_connected:
            logger.info("已经连接到AkShare")
            return True
        
        try:
            # AkShare不需要登录，直接标记为已连接
            self._is_connected = True
            logger.info("连接AkShare成功")
            return True
            
        except Exception as e:
            logger.error(f"连接AkShare失败: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """断开AkShare连接"""
        if not self._is_connected:
            logger.info("未连接AkShare，无需断开")
            return True
        
        self._is_connected = False
        logger.info("断开AkShare成功")
        return True
    
    def query_balance_sheet(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询资产负债表"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # AkShare的财务报表接口
            df = ak.stock_financial_report_sina(stock=code, symbol="资产负债表")
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询资产负债表失败: {str(e)}")
            logger.info("提示: AkShare的财务数据接口可能需要特定格式，请参考文档")
            return None
    
    def query_cash_flow(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询现金流量表"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # AkShare的财务报表接口
            df = ak.stock_financial_report_sina(stock=code, symbol="现金流量表")
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询现金流量表失败: {str(e)}")
            return None
    
    def query_income_statement(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询利润表"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # AkShare的财务报表接口
            df = ak.stock_financial_report_sina(stock=code, symbol="利润表")
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询利润表失败: {str(e)}")
            return None
    
    def query_stock_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """查询股票基本信息"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 获取A股股票列表
            df = ak.stock_info_a_code_name()
            return df
            
        except Exception as e:
            logger.error(f"查询股票基本信息失败: {str(e)}")
            return None
    
    def query_daily_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询日线行情数据"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # 默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            if not start_date:
                start_date = '19900101'  # AkShare可以获取全部历史数据
            else:
                start_date = start_date.replace('-', '')
            
            # 获取历史行情数据
            adjust = kwargs.get('adjust', 'qfq')  # qfq=前复权, hfq=后复权, ''=不复权
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            return df
            
        except Exception as e:
            logger.error(f"查询日线数据失败: {str(e)}")
            return None
    
    def normalize_code(self, code: str) -> str:
        """
        标准化股票代码为AkShare格式
        AkShare格式: 纯数字代码，如 600000 或 000001
        """
        return self._extract_code(code)
    
    def _extract_code(self, code: str) -> str:
        """
        提取纯数字代码
        
        Args:
            code: 原始代码（可能包含市场前缀或后缀）
            
        Returns:
            str: 纯数字代码
        """
        code = code.strip()
        
        # 移除各种前缀和后缀
        code = code.replace('sh.', '').replace('sz.', '')
        code = code.replace('SH.', '').replace('SZ.', '')
        code = code.replace('.SH', '').replace('.SZ', '')
        code = code.replace('.sh', '').replace('.sz', '')
        
        return code
    
    def query_financial_indicator(
        self,
        code: str,
        year: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询财务指标
        这是AkShare特有的接口
        
        Args:
            code: 股票代码
            year: 年份
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 财务指标数据
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            code = self._extract_code(code)
            
            # 获取财务指标
            df = ak.stock_financial_analysis_indicator(symbol=code)
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询财务指标失败: {str(e)}")
            return None
