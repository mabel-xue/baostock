"""
BaoStock数据源适配器
"""

import pandas as pd
import baostock as bs
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .base_datasource import BaseDataSource, DataSourceType

logger = logging.getLogger(__name__)


class BaoStockDataSource(BaseDataSource):
    """BaoStock数据源实现"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.source_type = DataSourceType.BAOSTOCK
    
    def connect(self) -> bool:
        """连接到BaoStock"""
        if self._is_connected:
            logger.info("已经连接到BaoStock")
            return True
        
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self._is_connected = True
                logger.info(f"连接BaoStock成功: {lg.error_msg}")
                return True
            else:
                logger.error(f"连接BaoStock失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"连接BaoStock异常: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """断开BaoStock连接"""
        if not self._is_connected:
            logger.info("未连接BaoStock，无需断开")
            return True
        
        try:
            lg = bs.logout()
            if lg.error_code == '0':
                self._is_connected = False
                logger.info(f"断开BaoStock成功: {lg.error_msg}")
                return True
            else:
                logger.error(f"断开BaoStock失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"断开BaoStock异常: {str(e)}")
            return False
    
    def query_balance_sheet(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询资产负债表"""
        if not self._is_connected:
            logger.error("未连接到BaoStock")
            return None
        
        try:
            code = self.normalize_code(code)
            
            if year and quarter:
                rs = bs.query_balance_data(code=code, year=year, quarter=quarter)
            elif year:
                rs = bs.query_balance_data(code=code, year=year, quarter=4)
            else:
                current_year = datetime.now().year
                rs = bs.query_balance_data(code=code, year=current_year, quarter=4)
            
            return self._result_to_dataframe(rs)
            
        except Exception as e:
            logger.error(f"查询资产负债表失败: {str(e)}")
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
            logger.error("未连接到BaoStock")
            return None
        
        try:
            code = self.normalize_code(code)
            
            if year and quarter:
                rs = bs.query_cash_flow_data(code=code, year=year, quarter=quarter)
            elif year:
                rs = bs.query_cash_flow_data(code=code, year=year, quarter=4)
            else:
                current_year = datetime.now().year
                rs = bs.query_cash_flow_data(code=code, year=current_year, quarter=4)
            
            return self._result_to_dataframe(rs)
            
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
            logger.error("未连接到BaoStock")
            return None
        
        try:
            code = self.normalize_code(code)
            
            if year and quarter:
                rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
            elif year:
                rs = bs.query_profit_data(code=code, year=year, quarter=4)
            else:
                current_year = datetime.now().year
                rs = bs.query_profit_data(code=code, year=current_year, quarter=4)
            
            return self._result_to_dataframe(rs)
            
        except Exception as e:
            logger.error(f"查询利润表失败: {str(e)}")
            return None
    
    def query_stock_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """查询股票基本信息"""
        if not self._is_connected:
            logger.error("未连接到BaoStock")
            return None
        
        try:
            # 查询所有股票
            rs = bs.query_stock_basic()
            return self._result_to_dataframe(rs)
            
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
            logger.error("未连接到BaoStock")
            return None
        
        try:
            code = self.normalize_code(code)
            
            # 默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now().replace(year=datetime.now().year-1)).strftime('%Y-%m-%d')
            
            # 获取字段列表
            fields = kwargs.get('fields', 'date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,isST')
            frequency = kwargs.get('frequency', 'd')  # d=日k线, w=周, m=月
            adjustflag = kwargs.get('adjustflag', '3')  # 1=后复权, 2=前复权, 3=不复权
            
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )
            
            return self._result_to_dataframe(rs)
            
        except Exception as e:
            logger.error(f"查询日线数据失败: {str(e)}")
            return None
    
    def normalize_code(self, code: str) -> str:
        """
        标准化股票代码为BaoStock格式
        BaoStock格式: sh.600000 或 sz.000001
        """
        code = code.strip().lower()
        
        # 如果已经包含市场前缀，直接返回
        if code.startswith('sh.') or code.startswith('sz.'):
            return code
        
        # 移除可能的其他前缀
        code = code.replace('.sh', '').replace('.sz', '')
        
        # 根据代码判断市场
        if code.startswith('6'):
            return f'sh.{code}'
        elif code.startswith(('0', '3')):
            return f'sz.{code}'
        else:
            logger.warning(f"无法识别股票代码 {code} 的市场，默认使用上海市场")
            return f'sh.{code}'
    
    def _result_to_dataframe(self, rs) -> Optional[pd.DataFrame]:
        """
        将BaoStock查询结果转换为DataFrame
        
        Args:
            rs: BaoStock查询结果对象
            
        Returns:
            Optional[pd.DataFrame]: 转换后的DataFrame
        """
        if rs.error_code != '0':
            logger.error(f"查询失败: {rs.error_msg}")
            return None
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning("查询结果为空")
            return pd.DataFrame()
        
        result = pd.DataFrame(data_list, columns=rs.fields)
        return result
