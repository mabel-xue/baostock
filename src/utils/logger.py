"""
日志配置模块
"""

import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


def get_log_dir() -> Path:
    """
    获取日志目录路径，如果不存在则创建
    
    Returns:
        Path: 日志目录路径
    """
    # 获取项目根目录（假设logger.py在src/utils/下）
    current_file = Path(__file__)
    project_root = current_file.parent.parent.parent
    log_dir = project_root / "logs"
    
    # 确保日志目录存在
    log_dir.mkdir(exist_ok=True)
    
    return log_dir


def get_default_log_file(name: Optional[str] = None) -> str:
    """
    获取默认日志文件路径
    
    Args:
        name: 日志记录器名称，用于生成日志文件名
        
    Returns:
        str: 日志文件路径
    """
    log_dir = get_log_dir()
    
    # 生成日志文件名：日期_名称.log 或 日期_app.log
    date_str = datetime.now().strftime('%Y%m%d')
    log_name = name if name else 'app'
    log_file = log_dir / f"{date_str}_{log_name}.log"
    
    return str(log_file)


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    enable_file_logging: bool = True
) -> logging.Logger:
    """
    配置日志记录器
    
    Args:
        name: 日志记录器名称，默认为根记录器
        level: 日志级别
        log_file: 日志文件路径，不指定则使用默认路径（logs目录下）
        enable_file_logging: 是否启用文件日志，默认为True
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加处理器
    if logger.handlers:
        return logger
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器
    if enable_file_logging:
        if log_file is None:
            log_file = get_default_log_file(name)
        
        # 确保日志文件所在目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"日志文件已创建: {log_file}")
    
    return logger
