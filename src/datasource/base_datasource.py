"""
数据源基类模块
定义统一的数据源接口
"""

import pandas as pd
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from enum import Enum

logger = logging.getLogger(__name__)


class DataSourceType(Enum):
    """数据源类型枚举"""
    BAOSTOCK = "baostock"
    TUSHARE = "tushare"
    AKSHARE = "akshare"
    WIND = "wind"


class BaseDataSource(ABC):
    """数据源基类，定义统一的数据接口"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化数据源
        
        Args:
            config: 数据源配置字典
        """
        self.config = config or {}
        self._is_connected = False
        self.source_type = None
    
    @abstractmethod
    def connect(self) -> bool:
        """
        连接到数据源
        
        Returns:
            bool: 连接是否成功
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        断开数据源连接
        
        Returns:
            bool: 断开是否成功
        """
        pass
    
    def is_connected(self) -> bool:
        """
        检查是否已连接
        
        Returns:
            bool: 是否已连接
        """
        return self._is_connected
    
    @abstractmethod
    def query_balance_sheet(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询资产负债表
        
        Args:
            code: 股票代码
            year: 年份
            quarter: 季度
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 资产负债表数据
        """
        pass
    
    @abstractmethod
    def query_cash_flow(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询现金流量表
        
        Args:
            code: 股票代码
            year: 年份
            quarter: 季度
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 现金流量表数据
        """
        pass
    
    @abstractmethod
    def query_income_statement(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询利润表
        
        Args:
            code: 股票代码
            year: 年份
            quarter: 季度
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 利润表数据
        """
        pass
    
    @abstractmethod
    def query_stock_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        查询股票基本信息
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            Optional[pd.DataFrame]: 股票基本信息
        """
        pass
    
    @abstractmethod
    def query_daily_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询日线行情数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 日线数据
        """
        pass
    
    def normalize_code(self, code: str) -> str:
        """
        标准化股票代码（不同数据源格式可能不同）
        
        Args:
            code: 原始股票代码
            
        Returns:
            str: 标准化后的代码
        """
        # 默认实现，子类可以覆盖
        return code.strip()
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
    
    def get_source_name(self) -> str:
        """
        获取数据源名称
        
        Returns:
            str: 数据源名称
        """
        return self.source_type.value if self.source_type else "unknown"
