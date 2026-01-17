"""
数据源管理器模块
管理多个数据源实例，支持数据源切换和故障转移
"""

import logging
from typing import Optional, Dict, Any, List
import pandas as pd
from .base_datasource import BaseDataSource, DataSourceType
from .datasource_factory import DataSourceFactory

logger = logging.getLogger(__name__)


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化数据源管理器
        
        Args:
            config: 配置字典，格式如下:
                {
                    'default_source': 'baostock',
                    'fallback_sources': ['tushare', 'akshare'],
                    'sources_config': {
                        'tushare': {'token': 'your_token'},
                        'akshare': {}
                    }
                }
        """
        self.config = config or {}
        self.datasources: Dict[DataSourceType, BaseDataSource] = {}
        self.default_source_type = None
        self.fallback_sources: List[DataSourceType] = []
        
        # 解析配置
        self._parse_config()
    
    def _parse_config(self):
        """解析配置"""
        # 设置默认数据源
        default_source_name = self.config.get('default_source', 'baostock')
        try:
            self.default_source_type = DataSourceType(default_source_name)
        except ValueError:
            logger.warning(f"无效的默认数据源: {default_source_name}，使用baostock")
            self.default_source_type = DataSourceType.BAOSTOCK
        
        # 设置备用数据源
        fallback_names = self.config.get('fallback_sources', [])
        for name in fallback_names:
            try:
                source_type = DataSourceType(name)
                if DataSourceFactory.is_available(source_type):
                    self.fallback_sources.append(source_type)
                else:
                    logger.warning(f"备用数据源 {name} 不可用，已跳过")
            except ValueError:
                logger.warning(f"无效的备用数据源: {name}")
    
    def get_datasource(
        self,
        source_type: Optional[DataSourceType] = None,
        auto_connect: bool = True
    ) -> Optional[BaseDataSource]:
        """
        获取数据源实例
        
        Args:
            source_type: 数据源类型，不指定则使用默认数据源
            auto_connect: 是否自动连接
            
        Returns:
            Optional[BaseDataSource]: 数据源实例
        """
        if source_type is None:
            source_type = self.default_source_type
        
        # 如果已经创建过，直接返回
        if source_type in self.datasources:
            datasource = self.datasources[source_type]
            if auto_connect and not datasource.is_connected():
                datasource.connect()
            return datasource
        
        # 创建新的数据源实例
        try:
            source_config = self.config.get('sources_config', {}).get(source_type.value, {})
            datasource = DataSourceFactory.create(
                source_type=source_type,
                config=source_config,
                auto_connect=auto_connect
            )
            self.datasources[source_type] = datasource
            return datasource
        except Exception as e:
            logger.error(f"获取数据源 {source_type.value} 失败: {str(e)}")
            return None
    
    def query_with_fallback(
        self,
        query_method: str,
        *args,
        source_type: Optional[DataSourceType] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        使用故障转移机制查询数据
        如果主数据源失败，自动尝试备用数据源
        
        Args:
            query_method: 查询方法名（如 'query_balance_sheet'）
            *args: 位置参数
            source_type: 指定数据源类型
            **kwargs: 关键字参数
            
        Returns:
            Optional[pd.DataFrame]: 查询结果
        """
        # 构建数据源尝试列表
        sources_to_try = []
        if source_type:
            sources_to_try.append(source_type)
        else:
            sources_to_try.append(self.default_source_type)
            sources_to_try.extend(self.fallback_sources)
        
        # 依次尝试每个数据源
        for src_type in sources_to_try:
            try:
                datasource = self.get_datasource(src_type)
                if datasource is None:
                    continue
                
                # 检查数据源是否有该方法
                if not hasattr(datasource, query_method):
                    logger.warning(f"数据源 {src_type.value} 不支持方法 {query_method}")
                    continue
                
                # 执行查询
                method = getattr(datasource, query_method)
                result = method(*args, **kwargs)
                
                if result is not None and not result.empty:
                    logger.info(f"使用数据源 {src_type.value} 查询成功")
                    return result
                elif result is not None and result.empty:
                    # 返回空DataFrame是正常情况（如基金没有持仓数据）
                    logger.debug(f"数据源 {src_type.value} 返回空结果（数据不存在）")
                    return result
                else:
                    logger.warning(f"数据源 {src_type.value} 返回None")
                    
            except Exception as e:
                logger.error(f"数据源 {src_type.value} 查询失败: {str(e)}")
                continue
        
        logger.warning(f"所有数据源都无法查询: {query_method}")
        return None
    
    def set_default_source(self, source_type: DataSourceType):
        """
        设置默认数据源
        
        Args:
            source_type: 数据源类型
        """
        if DataSourceFactory.is_available(source_type):
            self.default_source_type = source_type
            logger.info(f"默认数据源已设置为: {source_type.value}")
        else:
            logger.error(f"数据源 {source_type.value} 不可用")
    
    def add_fallback_source(self, source_type: DataSourceType):
        """
        添加备用数据源
        
        Args:
            source_type: 数据源类型
        """
        if DataSourceFactory.is_available(source_type):
            if source_type not in self.fallback_sources:
                self.fallback_sources.append(source_type)
                logger.info(f"已添加备用数据源: {source_type.value}")
        else:
            logger.error(f"数据源 {source_type.value} 不可用")
    
    def disconnect_all(self):
        """断开所有数据源连接"""
        for source_type, datasource in self.datasources.items():
            try:
                datasource.disconnect()
                logger.info(f"已断开数据源: {source_type.value}")
            except Exception as e:
                logger.error(f"断开数据源 {source_type.value} 失败: {str(e)}")
    
    def get_available_sources(self) -> List[str]:
        """
        获取可用的数据源列表
        
        Returns:
            List[str]: 数据源名称列表
        """
        return [t.value for t in DataSourceFactory.get_available_sources()]
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect_all()
