"""
Tushare数据源适配器
需要安装: pip install tushare
需要token: https://tushare.pro/register
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .base_datasource import BaseDataSource, DataSourceType

logger = logging.getLogger(__name__)

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    logger.warning("Tushare未安装，请运行: pip install tushare")


class TushareDataSource(BaseDataSource):
    """Tushare数据源实现"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.source_type = DataSourceType.TUSHARE
        self.api = None
        self.token = self.config.get('token', '')
        
        if not TUSHARE_AVAILABLE:
            raise ImportError("Tushare未安装，请运行: pip install tushare")
        
        if not self.token:
            logger.warning("未提供Tushare token，部分功能可能受限")
    
    def connect(self) -> bool:
        """连接到Tushare"""
        if self._is_connected:
            logger.info("已经连接到Tushare")
            return True
        
        try:
            if self.token:
                ts.set_token(self.token)
                self.api = ts.pro_api()
            else:
                # 使用免费接口
                self.api = ts
            
            self._is_connected = True
            logger.info("连接Tushare成功")
            return True
            
        except Exception as e:
            logger.error(f"连接Tushare失败: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """断开Tushare连接"""
        if not self._is_connected:
            logger.info("未连接Tushare，无需断开")
            return True
        
        self.api = None
        self._is_connected = False
        logger.info("断开Tushare成功")
        return True
    
    def query_balance_sheet(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询资产负债表"""
        if not self._is_connected or not self.api:
            logger.error("未连接到Tushare")
            return None
        
        try:
            code = self.normalize_code(code)
            
            # 构建期间参数
            period = None
            if year and quarter:
                period = f"{year}{quarter:02d}31" if quarter == 4 else f"{year}{quarter*3:02d}31"
            elif year:
                period = f"{year}1231"
            
            # 使用pro接口
            if hasattr(self.api, 'balancesheet'):
                df = self.api.balancesheet(
                    ts_code=code,
                    period=period,
                    fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_assets,total_liab,total_hldr_eqy_exc_min_int'
                )
            else:
                logger.error("Tushare API不支持资产负债表查询，请升级到pro版本")
                return None
            
            return df
            
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
        if not self._is_connected or not self.api:
            logger.error("未连接到Tushare")
            return None
        
        try:
            code = self.normalize_code(code)
            
            # 构建期间参数
            period = None
            if year and quarter:
                period = f"{year}{quarter:02d}31" if quarter == 4 else f"{year}{quarter*3:02d}31"
            elif year:
                period = f"{year}1231"
            
            # 使用pro接口
            if hasattr(self.api, 'cashflow'):
                df = self.api.cashflow(
                    ts_code=code,
                    period=period,
                    fields='ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,n_cashflow_act,n_cashflow_inv_act,n_cash_flows_fnc_act'
                )
            else:
                logger.error("Tushare API不支持现金流量表查询，请升级到pro版本")
                return None
            
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
        if not self._is_connected or not self.api:
            logger.error("未连接到Tushare")
            return None
        
        try:
            code = self.normalize_code(code)
            
            # 构建期间参数
            period = None
            if year and quarter:
                period = f"{year}{quarter:02d}31" if quarter == 4 else f"{year}{quarter*3:02d}31"
            elif year:
                period = f"{year}1231"
            
            # 使用pro接口
            if hasattr(self.api, 'income'):
                df = self.api.income(
                    ts_code=code,
                    period=period,
                    fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,operate_profit,total_profit,n_income,n_income_attr_p'
                )
            else:
                logger.error("Tushare API不支持利润表查询，请升级到pro版本")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"查询利润表失败: {str(e)}")
            return None
    
    def query_stock_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """查询股票基本信息"""
        if not self._is_connected or not self.api:
            logger.error("未连接到Tushare")
            return None
        
        try:
            # 使用pro接口
            if hasattr(self.api, 'stock_basic'):
                df = self.api.stock_basic(
                    exchange='',
                    list_status='L',
                    fields='ts_code,symbol,name,area,industry,market,list_date'
                )
            else:
                logger.error("Tushare API不支持股票基本信息查询，请升级到pro版本")
                return None
            
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
        if not self._is_connected or not self.api:
            logger.error("未连接到Tushare")
            return None
        
        try:
            code = self.normalize_code(code)
            
            # 默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            if not start_date:
                start_date = (datetime.now().replace(year=datetime.now().year-1)).strftime('%Y%m%d')
            else:
                start_date = start_date.replace('-', '')
            
            # 使用pro接口
            if hasattr(self.api, 'daily'):
                df = self.api.daily(
                    ts_code=code,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                # 使用免费接口
                df = self.api.get_k_data(
                    code=code.split('.')[0],
                    start=start_date,
                    end=end_date
                )
            
            return df
            
        except Exception as e:
            logger.error(f"查询日线数据失败: {str(e)}")
            return None
    
    def normalize_code(self, code: str) -> str:
        """
        标准化股票代码为Tushare格式
        Tushare格式: 600000.SH 或 000001.SZ
        """
        code = code.strip().upper()
        
        # 如果已经包含市场后缀，直接返回
        if code.endswith('.SH') or code.endswith('.SZ'):
            return code
        
        # 移除可能的前缀
        code = code.replace('SH.', '').replace('SZ.', '').replace('sh.', '').replace('sz.', '')
        
        # 根据代码判断市场
        if code.startswith('6'):
            return f'{code}.SH'
        elif code.startswith(('0', '3')):
            return f'{code}.SZ'
        else:
            logger.warning(f"无法识别股票代码 {code} 的市场，默认使用上海市场")
            return f'{code}.SH'
