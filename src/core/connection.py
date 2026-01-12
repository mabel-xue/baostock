"""
BaoStock连接管理模块
"""

import baostock as bs
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BaoStockConnection:
    """BaoStock连接管理器，使用单例模式"""
    
    _instance: Optional['BaoStockConnection'] = None
    _is_logged_in: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def login(self) -> bool:
        """
        登录BaoStock
        
        Returns:
            bool: 登录是否成功
        """
        if self._is_logged_in:
            logger.info("已经登录BaoStock")
            return True
        
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self._is_logged_in = True
                logger.info(f"登录BaoStock成功: {lg.error_msg}")
                return True
            else:
                logger.error(f"登录BaoStock失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"登录BaoStock异常: {str(e)}")
            return False
    
    def logout(self) -> bool:
        """
        登出BaoStock
        
        Returns:
            bool: 登出是否成功
        """
        if not self._is_logged_in:
            logger.info("未登录BaoStock，无需登出")
            return True
        
        try:
            lg = bs.logout()
            if lg.error_code == '0':
                self._is_logged_in = False
                logger.info(f"登出BaoStock成功: {lg.error_msg}")
                return True
            else:
                logger.error(f"登出BaoStock失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"登出BaoStock异常: {str(e)}")
            return False
    
    def is_connected(self) -> bool:
        """
        检查是否已连接
        
        Returns:
            bool: 是否已连接
        """
        return self._is_logged_in
    
    def __enter__(self):
        """上下文管理器入口"""
        self.login()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.logout()
