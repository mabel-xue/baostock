# 多数据源支持文档

## 概述

本项目现已支持多种金融数据源，可以灵活切换和整合不同的数据源，提供更可靠的数据查询服务。

## 支持的数据源

### 1. BaoStock（默认）
- **优点**: 免费、无需注册、数据稳定
- **缺点**: 数据更新可能有延迟
- **安装**: `pip install baostock`
- **配置**: 无需配置

### 2. Tushare
- **优点**: 数据全面、更新及时、接口丰富
- **缺点**: 需要注册获取token，部分接口有积分要求
- **安装**: `pip install tushare`
- **配置**: 需要提供token
- **注册**: https://tushare.pro/register

### 3. AkShare
- **优点**: 免费、无需注册、数据源多样
- **缺点**: 接口可能不稳定
- **安装**: `pip install akshare`
- **配置**: 无需配置

## 快速开始

### 方式1: 使用单个数据源

```python
from datasource import DataSourceFactory, DataSourceType
from queries.balance_query import BalanceQuery

# 创建数据源
datasource = DataSourceFactory.create(DataSourceType.BAOSTOCK)

# 使用数据源创建查询对象
query = BalanceQuery(datasource=datasource)

# 查询数据
df = query.query(code="600000", year=2023)

# 断开连接
datasource.disconnect()
```

### 方式2: 使用数据源管理器（推荐）

```python
from datasource import DataSourceManager
from queries.cashflow_query import CashFlowQuery

# 配置数据源
config = {
    'default_source': 'baostock',
    'fallback_sources': ['tushare', 'akshare'],  # 备用数据源
    'sources_config': {
        'tushare': {'token': 'your_token_here'}
    }
}

# 使用上下文管理器
with DataSourceManager(config) as manager:
    query = CashFlowQuery(datasource=manager)
    df = query.query(code="600000", year=2023)
```

### 方式3: 使用配置文件

1. 复制配置文件模板：
```bash
cp config.example.json config.json
```

2. 编辑 `config.json`，填入你的配置

3. 在代码中加载配置：
```python
from config import ConfigLoader
from datasource import DataSourceManager
from queries.balance_query import BalanceQuery

# 加载配置
config = ConfigLoader.load_from_file('config.json')

# 创建管理器
with DataSourceManager(config) as manager:
    query = BalanceQuery(datasource=manager)
    df = query.query(code="600000", year=2023)
```

### 方式4: 使用环境变量

```bash
# 设置环境变量
export BAOSTOCK_DEFAULT_SOURCE=baostock
export BAOSTOCK_TUSHARE_TOKEN=your_token_here
```

```python
from config import ConfigLoader
from datasource import DataSourceManager

# 从环境变量加载
config = ConfigLoader.load_config(use_env=True)

with DataSourceManager(config) as manager:
    # 使用管理器...
    pass
```

## 核心功能

### 1. 自动故障转移

当主数据源查询失败时，自动尝试备用数据源：

```python
config = {
    'default_source': 'baostock',
    'fallback_sources': ['tushare', 'akshare'],
}

with DataSourceManager(config) as manager:
    # 如果baostock失败，会自动尝试tushare，再失败会尝试akshare
    df = manager.query_with_fallback(
        'query_balance_sheet',
        code='600000',
        year=2023
    )
```

### 2. 动态切换数据源

```python
with DataSourceManager(config) as manager:
    # 使用默认数据源
    query = BalanceQuery(datasource=manager)
    df1 = query.query(code="600000", year=2023)
    
    # 切换数据源
    manager.set_default_source(DataSourceType.TUSHARE)
    df2 = query.query(code="600000", year=2023)
```

### 3. 数据源对比

运行对比脚本，测试各数据源的性能和数据质量：

```bash
python examples/datasource_comparison.py
```

## 配置说明

### 配置文件格式 (config.json)

```json
{
  "default_source": "baostock",
  "fallback_sources": ["tushare", "akshare"],
  "sources_config": {
    "baostock": {},
    "tushare": {
      "token": "your_tushare_token"
    },
    "akshare": {}
  },
  "logging": {
    "level": "INFO"
  },
  "cache": {
    "enabled": false,
    "cache_dir": ".cache",
    "expire_hours": 24
  }
}
```

### 配置项说明

- `default_source`: 默认使用的数据源
- `fallback_sources`: 备用数据源列表，按优先级排序
- `sources_config`: 各数据源的具体配置
  - `tushare.token`: Tushare的API token
- `logging`: 日志配置
- `cache`: 缓存配置（未来功能）

## 统一接口

所有数据源都实现了统一的接口：

```python
# 查询资产负债表
df = datasource.query_balance_sheet(code, year, quarter)

# 查询现金流量表
df = datasource.query_cash_flow(code, year, quarter)

# 查询利润表
df = datasource.query_income_statement(code, year, quarter)

# 查询股票基本信息
df = datasource.query_stock_basic()

# 查询日线数据
df = datasource.query_daily_data(code, start_date, end_date)
```

## 股票代码格式

不同数据源使用不同的股票代码格式，系统会自动转换：

| 数据源 | 格式 | 示例 |
|--------|------|------|
| BaoStock | sh.600000 | sh.600000, sz.000001 |
| Tushare | 600000.SH | 600000.SH, 000001.SZ |
| AkShare | 600000 | 600000, 000001 |

你可以使用任意格式，系统会自动标准化：

```python
# 以下代码都可以正常工作
df = query.query(code="600000", year=2023)
df = query.query(code="sh.600000", year=2023)
df = query.query(code="600000.SH", year=2023)
```

## 示例代码

### 完整示例

查看 `examples/multi_datasource_usage.py` 获取完整的使用示例。

运行示例：
```bash
python examples/multi_datasource_usage.py
```

### 性能对比

查看 `examples/datasource_comparison.py` 对比不同数据源的性能。

运行对比：
```bash
python examples/datasource_comparison.py
```

## 兼容性

### 向后兼容

现有代码无需修改，仍然可以正常工作：

```python
# 旧代码仍然可用
from queries.balance_query import BalanceQuery

query = BalanceQuery()  # 默认使用BaoStock
df = query.query(code="600000", year=2023)
```

### 渐进式迁移

你可以逐步迁移到新的数据源系统：

1. 先在部分代码中使用新数据源
2. 测试稳定性和性能
3. 逐步迁移其他代码
4. 最终统一使用数据源管理器

## 最佳实践

### 1. 使用上下文管理器

```python
# 推荐：自动管理连接
with DataSourceManager(config) as manager:
    query = BalanceQuery(datasource=manager)
    df = query.query(code="600000", year=2023)

# 不推荐：手动管理连接
manager = DataSourceManager(config)
query = BalanceQuery(datasource=manager)
df = query.query(code="600000", year=2023)
manager.disconnect_all()  # 容易忘记
```

### 2. 配置备用数据源

```python
# 推荐：配置多个备用数据源
config = {
    'default_source': 'baostock',
    'fallback_sources': ['tushare', 'akshare'],
}

# 不推荐：只使用单一数据源
config = {
    'default_source': 'baostock',
    'fallback_sources': [],
}
```

### 3. 使用配置文件

```python
# 推荐：使用配置文件，便于管理
config = ConfigLoader.load_from_file('config.json')

# 不推荐：硬编码配置
config = {
    'default_source': 'tushare',
    'sources_config': {
        'tushare': {'token': 'hardcoded_token'}  # 不安全
    }
}
```

### 4. 敏感信息使用环境变量

```bash
# 推荐：使用环境变量
export TUSHARE_TOKEN=your_token_here
```

```python
import os
config = {
    'sources_config': {
        'tushare': {'token': os.getenv('TUSHARE_TOKEN')}
    }
}
```

## 故障排查

### 问题1: 导入错误

```
ImportError: cannot import name 'DataSourceFactory'
```

**解决**: 确保已创建所有必要的文件，检查 `src/datasource/__init__.py`

### 问题2: Tushare token错误

```
Error: Tushare token is required
```

**解决**: 
1. 访问 https://tushare.pro/register 注册
2. 获取token
3. 在配置中添加token

### 问题3: 数据源不可用

```
ValueError: 不支持的数据源类型
```

**解决**: 
1. 检查是否安装了相应的依赖包
2. 运行 `pip install tushare akshare`

### 问题4: 查询返回空数据

**可能原因**:
1. 股票代码格式错误
2. 查询的时间范围没有数据
3. 数据源暂时不可用

**解决**:
1. 检查股票代码格式
2. 尝试其他时间范围
3. 切换到备用数据源

## 扩展开发

### 添加新的数据源

1. 创建新的数据源适配器：

```python
# src/datasource/your_datasource.py
from .base_datasource import BaseDataSource, DataSourceType

class YourDataSource(BaseDataSource):
    def __init__(self, config=None):
        super().__init__(config)
        self.source_type = DataSourceType.YOUR_SOURCE
    
    def connect(self):
        # 实现连接逻辑
        pass
    
    def query_balance_sheet(self, code, year, quarter):
        # 实现查询逻辑
        pass
    
    # 实现其他必需的方法...
```

2. 在 `DataSourceType` 枚举中添加新类型
3. 在 `DataSourceFactory` 中注册新数据源

## 更新日志

### v0.2.0 (2026-01-15)
- ✅ 添加多数据源支持
- ✅ 实现BaoStock、Tushare、AkShare适配器
- ✅ 添加数据源管理器和工厂模式
- ✅ 支持自动故障转移
- ✅ 添加配置文件支持
- ✅ 保持向后兼容性

## 贡献

欢迎贡献新的数据源适配器或改进现有功能！

## 许可证

MIT License
