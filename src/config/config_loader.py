"""
配置加载模块
支持从文件、环境变量等加载配置
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置加载器"""
    
    @staticmethod
    def load_from_file(config_path: str) -> Dict[str, Any]:
        """
        从文件加载配置
        
        Args:
            config_path: 配置文件路径，支持.json和.py文件
            
        Returns:
            Dict[str, Any]: 配置字典
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            logger.error(f"配置文件不存在: {config_path}")
            return {}
        
        try:
            if config_path.suffix == '.json':
                return ConfigLoader._load_json(config_path)
            elif config_path.suffix == '.py':
                return ConfigLoader._load_python(config_path)
            else:
                logger.error(f"不支持的配置文件格式: {config_path.suffix}")
                return {}
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}
    
    @staticmethod
    def _load_json(config_path: Path) -> Dict[str, Any]:
        """加载JSON配置文件"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def _load_python(config_path: Path) -> Dict[str, Any]:
        """加载Python配置文件"""
        import importlib.util
        
        spec = importlib.util.spec_from_file_location("config", config_path)
        if spec and spec.loader:
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            
            # 提取配置变量
            config = {}
            for key in dir(config_module):
                if not key.startswith('_'):
                    config[key] = getattr(config_module, key)
            
            return config
        
        return {}
    
    @staticmethod
    def load_from_env(prefix: str = "BAOSTOCK_") -> Dict[str, Any]:
        """
        从环境变量加载配置
        
        Args:
            prefix: 环境变量前缀
            
        Returns:
            Dict[str, Any]: 配置字典
        """
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # 移除前缀
                config_key = key[len(prefix):].lower()
                
                # 尝试解析JSON值
                try:
                    config[config_key] = json.loads(value)
                except json.JSONDecodeError:
                    config[config_key] = value
        
        return config
    
    @staticmethod
    def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
        """
        合并多个配置字典
        后面的配置会覆盖前面的配置
        
        Args:
            *configs: 配置字典列表
            
        Returns:
            Dict[str, Any]: 合并后的配置
        """
        merged = {}
        
        for config in configs:
            merged.update(config)
        
        return merged
    
    @staticmethod
    def load_config(
        config_file: Optional[str] = None,
        use_env: bool = True,
        env_prefix: str = "BAOSTOCK_"
    ) -> Dict[str, Any]:
        """
        加载配置（综合方法）
        
        优先级: 环境变量 > 配置文件 > 默认配置
        
        Args:
            config_file: 配置文件路径
            use_env: 是否使用环境变量
            env_prefix: 环境变量前缀
            
        Returns:
            Dict[str, Any]: 配置字典
        """
        # 默认配置
        config = get_default_config()
        
        # 从文件加载
        if config_file:
            file_config = ConfigLoader.load_from_file(config_file)
            config = ConfigLoader.merge_configs(config, file_config)
        
        # 从环境变量加载
        if use_env:
            env_config = ConfigLoader.load_from_env(env_prefix)
            config = ConfigLoader.merge_configs(config, env_config)
        
        logger.info(f"配置加载完成，使用数据源: {config.get('default_source', 'baostock')}")
        
        return config


def get_default_config() -> Dict[str, Any]:
    """
    获取默认配置
    
    Returns:
        Dict[str, Any]: 默认配置字典
    """
    return {
        # 默认数据源
        'default_source': 'baostock',
        
        # 备用数据源列表
        'fallback_sources': [],
        
        # 各数据源的配置
        'sources_config': {
            'baostock': {},
            'tushare': {
                'token': os.getenv('TUSHARE_TOKEN', '')
            },
            'akshare': {}
        },
        
        # 日志配置
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        
        # 缓存配置
        'cache': {
            'enabled': False,
            'cache_dir': '.cache',
            'expire_hours': 24
        },
        
        # 查询配置
        'query': {
            'retry_times': 3,
            'retry_delay': 1,
            'timeout': 30
        }
    }
