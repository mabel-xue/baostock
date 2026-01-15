"""
数据源工厂模块
使用工厂模式创建不同的数据源实例
"""

import logging
from typing import Optional, Dict, Any
from .base_datasource import BaseDataSource, DataSourceType
from .baostock_datasource import BaoStockDataSource

logger = logging.getLogger(__name__)

# 延迟导入可选数据源
_TUSHARE_AVAILABLE = False
_AKSHARE_AVAILABLE = False

try:
    from .tushare_datasource import TushareDataSource
    _TUSHARE_AVAILABLE = True
except ImportError:
    logger.debug("Tushare数据源不可用")

try:
    from .akshare_datasource import AkShareDataSource
    _AKSHARE_AVAILABLE = True
except ImportError:
    logger.debug("AkShare数据源不可用")


class DataSourceFactory:
    """数据源工厂类"""
    
    # 数据源类映射
    _datasource_classes = {
        DataSourceType.BAOSTOCK: BaoStockDataSource,
    }
    
    # 动态注册可选数据源
    if _TUSHARE_AVAILABLE:
        _datasource_classes[DataSourceType.TUSHARE] = TushareDataSource
    
    if _AKSHARE_AVAILABLE:
        _datasource_classes[DataSourceType.AKSHARE] = AkShareDataSource
    
    @classmethod
    def create(
        cls,
        source_type: DataSourceType,
        config: Optional[Dict[str, Any]] = None,
        auto_connect: bool = True
    ) -> BaseDataSource:
        """
        创建数据源实例
        
        Args:
            source_type: 数据源类型
            config: 数据源配置
            auto_connect: 是否自动连接
            
        Returns:
            BaseDataSource: 数据源实例
            
        Raises:
            ValueError: 不支持的数据源类型
            ImportError: 数据源依赖未安装
        """
        if source_type not in cls._datasource_classes:
            available = ', '.join([t.value for t in cls._datasource_classes.keys()])
            raise ValueError(
                f"不支持的数据源类型: {source_type.value}。"
                f"可用的数据源: {available}"
            )
        
        datasource_class = cls._datasource_classes[source_type]
        
        try:
            datasource = datasource_class(config)
            
            if auto_connect:
                if not datasource.connect():
                    logger.warning(f"自动连接 {source_type.value} 失败")
            
            logger.info(f"成功创建 {source_type.value} 数据源")
            return datasource
            
        except ImportError as e:
            raise ImportError(
                f"创建 {source_type.value} 数据源失败: {str(e)}。"
                f"请安装相应的依赖包。"
            )
        except Exception as e:
            logger.error(f"创建 {source_type.value} 数据源时出错: {str(e)}")
            raise
    
    @classmethod
    def create_from_string(
        cls,
        source_name: str,
        config: Optional[Dict[str, Any]] = None,
        auto_connect: bool = True
    ) -> BaseDataSource:
        """
        从字符串创建数据源实例
        
        Args:
            source_name: 数据源名称字符串（如 "baostock", "tushare"）
            config: 数据源配置
            auto_connect: 是否自动连接
            
        Returns:
            BaseDataSource: 数据源实例
            
        Raises:
            ValueError: 无效的数据源名称
        """
        source_name = source_name.lower().strip()
        
        # 查找匹配的数据源类型
        for source_type in DataSourceType:
            if source_type.value == source_name:
                return cls.create(source_type, config, auto_connect)
        
        # 未找到匹配的数据源
        available = ', '.join([t.value for t in cls._datasource_classes.keys()])
        raise ValueError(
            f"无效的数据源名称: {source_name}。"
            f"可用的数据源: {available}"
        )
    
    @classmethod
    def get_available_sources(cls) -> list:
        """
        获取可用的数据源列表
        
        Returns:
            list: 可用数据源类型列表
        """
        return list(cls._datasource_classes.keys())
    
    @classmethod
    def is_available(cls, source_type: DataSourceType) -> bool:
        """
        检查数据源是否可用
        
        Args:
            source_type: 数据源类型
            
        Returns:
            bool: 是否可用
        """
        return source_type in cls._datasource_classes
