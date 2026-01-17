# 日志系统使用指南

本项目已配置完整的日志系统，支持控制台输出和文件记录。

## 目录结构

```
baostock/
├── logs/                    # 日志文件目录（自动创建）
│   ├── README.md           # 日志目录说明
│   ├── 20260116_app.log    # 应用日志文件（按日期命名）
│   └── 20260116_fund_monitoring.log  # 特定模块日志
└── src/
    └── utils/
        └── logger.py       # 日志配置模块
```

## 快速开始

### 1. 在应用程序入口使用（推荐）

在主程序或脚本中使用 `setup_logger` 函数：

```python
from src.utils.logger import setup_logger

# 创建日志记录器（自动保存到 logs/YYYYMMDD_my_app.log）
logger = setup_logger('my_app')

logger.info("应用程序启动")
logger.warning("这是一个警告")
logger.error("这是一个错误")
```

### 2. 在库模块中使用（推荐）

在库模块中使用标准的 `logging.getLogger(__name__)`：

```python
import logging

logger = logging.getLogger(__name__)

def my_function():
    logger.info("执行某个操作")
```

这样可以继承主程序配置的日志设置。

## 配置选项

### 基本配置

```python
from src.utils.logger import setup_logger
import logging

# 默认配置（INFO级别，保存到logs目录）
logger = setup_logger('my_module')

# 设置DEBUG级别
logger = setup_logger('my_module', level=logging.DEBUG)

# 指定自定义日志文件
logger = setup_logger('my_module', log_file='logs/custom.log')

# 仅输出到控制台，不保存文件
logger = setup_logger('my_module', enable_file_logging=False)
```

### 日志级别

- `logging.DEBUG` - 详细的调试信息
- `logging.INFO` - 一般信息（默认）
- `logging.WARNING` - 警告信息
- `logging.ERROR` - 错误信息
- `logging.CRITICAL` - 严重错误

## 日志文件命名规则

日志文件自动按以下规则命名：

- 格式：`YYYYMMDD_<logger_name>.log`
- 示例：
  - `20260116_app.log` - 默认应用日志
  - `20260116_fund_monitoring.log` - 基金监测模块日志
  - `20260116_data_query.log` - 数据查询模块日志

每天会自动创建新的日志文件，便于管理和归档。

## 实际使用示例

### 示例1：基金监测脚本

```python
from src.utils.logger import setup_logger
from src.datasource import DataSourceManager

# 配置日志
logger = setup_logger('fund_monitoring')

def main():
    logger.info("开始基金监测任务")
    
    try:
        config = {'default_source': 'akshare'}
        with DataSourceManager(config) as manager:
            logger.info("数据源连接成功")
            # 执行查询...
            
    except Exception as e:
        logger.error(f"任务执行失败: {e}", exc_info=True)
    
    logger.info("基金监测任务完成")

if __name__ == '__main__':
    main()
```

### 示例2：数据查询脚本

```python
from src.utils.logger import setup_logger
import logging

# 配置详细的DEBUG日志
logger = setup_logger('data_query', level=logging.DEBUG)

def query_data():
    logger.debug("开始数据查询")
    logger.info("查询参数: stock_code=sh.600000")
    
    try:
        # 执行查询
        result = perform_query()
        logger.info(f"查询成功，返回 {len(result)} 条记录")
        return result
    except Exception as e:
        logger.error(f"查询失败: {e}", exc_info=True)
        raise
```

### 示例3：仅控制台输出

```python
from src.utils.logger import setup_logger

# 临时调试时不需要保存日志文件
logger = setup_logger('debug_script', enable_file_logging=False)

logger.info("这条日志只会显示在控制台")
```

## 日志格式

日志输出格式为：

```
2026-01-16 10:30:45 - fund_monitoring - INFO - 开始基金监测任务
2026-01-16 10:30:46 - fund_monitoring - INFO - 数据源连接成功
2026-01-16 10:30:50 - fund_monitoring - ERROR - 查询失败: 连接超时
```

格式说明：
- 时间戳（精确到秒）
- 日志记录器名称
- 日志级别
- 日志消息

## 日志文件管理

### 查看日志

```bash
# 查看最新日志
tail -f logs/20260116_app.log

# 查看特定模块日志
tail -f logs/20260116_fund_monitoring.log

# 搜索错误日志
grep "ERROR" logs/*.log
```

### 清理旧日志

日志文件会随时间累积，建议定期清理：

```bash
# 删除30天前的日志
find logs/ -name "*.log" -mtime +30 -delete

# 或者手动删除旧日志
rm logs/202601*.log
```

## 注意事项

1. **日志文件不会提交到Git**：`.gitignore` 已配置忽略 `logs/` 目录
2. **自动创建目录**：首次运行时会自动创建 `logs` 目录
3. **UTF-8编码**：日志文件使用UTF-8编码，支持中文
4. **线程安全**：日志系统是线程安全的，可在多线程环境使用
5. **避免重复配置**：在同一程序中对同一logger多次调用 `setup_logger` 不会重复添加处理器

## 高级用法

### 配置根日志记录器

```python
from src.utils.logger import setup_logger
import logging

# 配置根日志记录器，影响所有模块
root_logger = setup_logger(name=None, level=logging.INFO)

# 现在所有模块的日志都会输出到文件
```

### 异常追踪

```python
logger = setup_logger('my_app')

try:
    risky_operation()
except Exception as e:
    # exc_info=True 会记录完整的异常堆栈
    logger.error("操作失败", exc_info=True)
```

### 多个日志文件

```python
# 为不同模块创建不同的日志文件
data_logger = setup_logger('data_module', log_file='logs/data.log')
api_logger = setup_logger('api_module', log_file='logs/api.log')

data_logger.info("数据处理日志")
api_logger.info("API调用日志")
```

## 迁移指南

如果你的代码使用了旧的日志配置方式，可以按以下方式迁移：

### 迁移前

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

### 迁移后

```python
from src.utils.logger import setup_logger

logger = setup_logger(__name__)
```

或者在主程序中：

```python
from src.utils.logger import setup_logger

logger = setup_logger('my_app')  # 使用有意义的应用名称
```

## 参考资料

- [Python logging 官方文档](https://docs.python.org/3/library/logging.html)
- 日志配置源码：`src/utils/logger.py`
- 使用示例：`examples/fund_monitoring_example.py`
