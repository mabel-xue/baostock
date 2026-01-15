"""
查询基类模块
"""

import pandas as pd
import baostock as bs
import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Union
from core.connection import BaoStockConnection

logger = logging.getLogger(__name__)

# 延迟导入数据源模块，避免循环依赖
try:
    from datasource.base_datasource import BaseDataSource
    from datasource.datasource_manager import DataSourceManager
    DATASOURCE_AVAILABLE = True
except ImportError:
    DATASOURCE_AVAILABLE = False
    logger.debug("数据源模块不可用，使用传统BaoStock连接")


class BaseQuery(ABC):
    """查询基类，定义通用查询接口，支持多数据源"""
    
    def __init__(self, datasource: Optional[Union['BaseDataSource', 'DataSourceManager']] = None):
        """
        初始化查询类
        
        Args:
            datasource: 数据源实例或数据源管理器。
                       如果为None，则使用传统的BaoStock连接
        """
        self.datasource = datasource
        self.connection = None
        
        # 如果没有提供数据源，使用传统的BaoStock连接
        if self.datasource is None:
            self.connection = BaoStockConnection()
    
    def ensure_connection(self) -> bool:
        """
        确保已连接到数据源
        
        Returns:
            bool: 是否已连接
        """
        # 如果使用新的数据源系统
        if self.datasource is not None:
            if hasattr(self.datasource, 'is_connected'):
                # 单个数据源
                if not self.datasource.is_connected():
                    return self.datasource.connect()
                return True
            else:
                # 数据源管理器，总是返回True
                return True
        
        # 使用传统的BaoStock连接
        if self.connection is not None:
            if not self.connection.is_connected():
                return self.connection.login()
            return True
        
        return False
    
    @abstractmethod
    def query(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        执行查询的抽象方法，子类必须实现
        
        Returns:
            Optional[pd.DataFrame]: 查询结果
        """
        pass
    
    def _result_to_dataframe(self, rs) -> Optional[pd.DataFrame]:
        """
        将BaoStock查询结果转换为DataFrame
        
        Args:
            rs: BaoStock查询结果对象
            
        Returns:
            Optional[pd.DataFrame]: 转换后的DataFrame，失败返回None
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
    
    def batch_query(self, items: List, query_func, **kwargs) -> dict:
        """
        批量查询通用方法
        
        Args:
            items: 要查询的项目列表（如股票代码列表）
            query_func: 单个查询函数
            **kwargs: 传递给查询函数的其他参数
            
        Returns:
            dict: {item: DataFrame} 的字典
        """
        results = {}
        for item in items:
            try:
                result = query_func(item, **kwargs)
                results[item] = result
            except Exception as e:
                logger.error(f"查询 {item} 时出错: {str(e)}")
                results[item] = None
        
        return results
