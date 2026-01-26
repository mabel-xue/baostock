# BaoStock 财务分析工具

<div align="center">

![Python Version](https://img.shields.io/badge/python-3.7+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-active-success.svg)

**一个强大、灵活、易用的 A股财务数据查询与分析工具**

支持多数据源 | 自动故障转移 | 简洁API | 丰富示例

[快速开始](#快速开始) • [功能特性](#功能特性) • [文档](#文档) • [示例](#示例)

</div>

---

## 📖 简介

BaoStock 财务分析工具是一个基于 Python 的 A股财务数据查询与分析框架。它提供了统一、简洁的 API 来查询上市公司的财务报表数据，支持多个数据源，并具有自动故障转移功能，确保数据查询的稳定性和可靠性。

### 为什么选择这个工具？

- 🎯 **统一接口** - 一套API，多个数据源，无缝切换
- 🔄 **自动故障转移** - 主数据源失败时自动切换备用源
- 🆓 **免费使用** - 默认使用免费的 BaoStock 数据源
- 📊 **数据全面** - 支持资产负债表、现金流量表、利润表等
- 🚀 **简单易用** - 清晰的 API 设计，丰富的示例代码
- 🔧 **灵活配置** - 支持配置文件、环境变量等多种配置方式
- 🏗️ **架构优雅** - 采用工厂、适配器等设计模式，易于扩展
- 📚 **文档完善** - 详细的使用文档和最佳实践指南

---

## ✨ 功能特性

### 核心功能

- ✅ **财务报表查询**
  - 资产负债表
  - 现金流量表
  - 利润表（即将支持）
  - **基本面信息查询（新增）**
  
- ✅ **多数据源支持**
  - BaoStock（默认，免费）
  - Tushare（需token）
  - AkShare（免费）
  
- ✅ **高级功能**
  - 单个公司查询
  - 批量公司查询
  - 历史数据查询
  - 公司对比分析
  - **根据公司名称批量查询基本面（新增）**
  - 自动故障转移
  - 动态数据源切换

- ✅ **灵活配置**
  - JSON 配置文件
  - Python 配置文件
  - 环境变量
  - 代码配置

### 数据源对比

| 数据源 | 费用 | 注册 | 数据质量 | 更新速度 | 推荐场景 |
|--------|------|------|----------|----------|----------|
| **BaoStock** | 免费 | 不需要 | ⭐⭐⭐⭐ | 中等 | 日常使用、学习研究 |
| **Tushare** | 部分免费 | 需要token | ⭐⭐⭐⭐⭐ | 快速 | 专业分析、生产环境 |
| **AkShare** | 免费 | 不需要 | ⭐⭐⭐ | 中等 | 备用数据源 |

---

## 🚀 快速开始

### 安装

#### 1. 克隆项目

```bash
git clone https://github.com/yourusername/baostock.git
cd baostock
```

#### 2. 安装依赖

**基础安装（仅 BaoStock）：**
```bash
pip install -r requirements.txt
```

**完整安装（包含所有数据源）：**
```bash
pip install baostock pandas python-dateutil
pip install tushare  # 可选，需要token
pip install akshare  # 可选，免费
```

### 快速示例

#### 示例 1: 基础使用

```python
from queries.balance_query import BalanceQuery

# 创建查询对象
query = BalanceQuery()

# 查询浦发银行2023年资产负债表
df = query.query(code="600000", year=2023)

if df is not None and not df.empty:
    print(df[['code', 'pubDate', 'liabilityToAsset']])
```

#### 示例 2: 使用多数据源

```python
from datasource import DataSourceManager
from queries.cashflow_query import CashFlowQuery

# 配置数据源（主数据源 + 备用数据源）
config = {
    'default_source': 'baostock',
    'fallback_sources': ['akshare'],  # 备用数据源
}

# 使用数据源管理器
with DataSourceManager(config) as manager:
    query = CashFlowQuery(datasource=manager)
    
    # 查询现金流量数据
    df = query.query(code="600000", year=2023)
    print(df)
```

#### 示例 3: 批量查询

```python
from queries.balance_query import BalanceQuery

query = BalanceQuery()

# 批量查询多家公司
codes = ["600000", "601398", "600519"]
results = query.query_multiple(codes=codes, year=2023)

for code, df in results.items():
    if df is not None and not df.empty:
        print(f"{code}: {len(df)} 条记录")
```

#### 示例 4: 基本面信息查询（新增）

```python
from datasource.akshare_datasource import AkShareDataSource
from queries.fundamental_query import FundamentalQuery

# 使用AkShare数据源
with AkShareDataSource() as datasource:
    query = FundamentalQuery(datasource=datasource)
    
    # 根据公司名称批量查询基本面
    company_names = ["贵州茅台", "五粮液", "泸州老窖"]
    df = query.query_by_names(names=company_names, year=2024)
    
    # 显示汇总表
    print(df)
    
    # 保存到CSV
    df.to_csv("fundamental_data.csv", index=False, encoding='utf-8-sig')
```

或使用简化版本：

```bash
# 运行预置示例
python app/query_fundamental_simple.py
```

详细使用说明请参考：[基本面查询使用指南.md](基本面查询使用指南.md)

#### 示例 5: 运行主程序

```bash
python main.py
```

输出示例：
```
============================================================
BaoStock 公司分析工具 - 现金流量查询
============================================================

正在查询 3 家公司的2023年现金流量数据...

浦发银行 (600000):
------------------------------------------------------------
  统计日期: 2023-12-31
  发布日期: 2024-03-29
  经营活动现金流: 0.0234
  投资活动现金流: -0.0156
  筹资活动现金流: -0.0078
...
```

---

## 📚 文档

### 核心文档

- 📘 [多数据源使用文档](DATASOURCE_README.md) - 详细的多数据源使用指南
- 📗 [实施总结](MULTI_DATASOURCE_SUMMARY.md) - 多数据源功能实施总结
- 📙 [配置示例](config.example.json) - 配置文件模板
- 📕 [日志系统使用指南](LOGGING_GUIDE.md) - 日志配置和使用说明

### API 文档

#### 查询类

##### BalanceQuery - 资产负债表查询

```python
from queries.balance_query import BalanceQuery

query = BalanceQuery(datasource=None)  # datasource可选

# 单个查询
df = query.query(code="600000", year=2023, quarter=None)

# 批量查询
results = query.query_multiple(codes=["600000", "601398"], year=2023)
```

##### CashFlowQuery - 现金流量表查询

```python
from queries.cashflow_query import CashFlowQuery

query = CashFlowQuery(datasource=None)

# 单个查询
df = query.query(code="600000", year=2023, quarter=None)

# 批量查询
results = query.query_multiple(codes=["600000", "601398"], year=2023)

# 历史数据查询
df = query.query_history(code="600000", start_year=2020, end_year=2023)

# 公司对比
df = query.compare_companies(
    codes=["600000", "601398"], 
    year=2023,
    metrics=['CAToAsset', 'NCAToAsset']
)
```

#### 数据源管理

##### DataSourceFactory - 数据源工厂

```python
from datasource import DataSourceFactory, DataSourceType

# 创建数据源
datasource = DataSourceFactory.create(DataSourceType.BAOSTOCK)

# 查看可用数据源
available = DataSourceFactory.get_available_sources()

# 从字符串创建
datasource = DataSourceFactory.create_from_string('baostock')
```

##### DataSourceManager - 数据源管理器

```python
from datasource import DataSourceManager

config = {
    'default_source': 'baostock',
    'fallback_sources': ['akshare'],
    'sources_config': {
        'tushare': {'token': 'your_token'}
    }
}

with DataSourceManager(config) as manager:
    # 获取数据源
    datasource = manager.get_datasource()
    
    # 使用故障转移查询
    df = manager.query_with_fallback(
        'query_balance_sheet',
        code='600000',
        year=2023
    )
    
    # 切换默认数据源
    manager.set_default_source(DataSourceType.AKSHARE)
```

---

## 💡 示例

项目包含丰富的示例代码，位于 `examples/` 目录：

### 基础示例

```bash
# 基础使用示例
python examples/basic_usage.py

# 高级使用示例
python examples/advanced_usage.py
```

### 多数据源示例

```bash
# 多数据源使用示例
python examples/multi_datasource_usage.py

# 数据源性能对比
python examples/datasource_comparison.py
```

### 行业分析示例

```bash
# 高速公路行业资产负债率分析
python query_highway_balance.py

# 高速公路行业现金流分析
python query_highway_companies.py
```

---

## 🔧 配置

### 方式 1: 配置文件

1. 复制配置模板：
```bash
cp config.example.json config.json
```

2. 编辑 `config.json`：
```json
{
  "default_source": "baostock",
  "fallback_sources": ["akshare"],
  "sources_config": {
    "tushare": {
      "token": "your_tushare_token_here"
    }
  }
}
```

3. 使用配置：
```python
from config import ConfigLoader
from datasource import DataSourceManager

config = ConfigLoader.load_from_file('config.json')
manager = DataSourceManager(config)
```

### 方式 2: 环境变量

```bash
export BAOSTOCK_DEFAULT_SOURCE=baostock
export BAOSTOCK_TUSHARE_TOKEN=your_token_here
```

```python
from config import ConfigLoader

config = ConfigLoader.load_config(use_env=True)
```

### 方式 3: 代码配置

```python
config = {
    'default_source': 'baostock',
    'fallback_sources': ['akshare'],
    'sources_config': {
        'tushare': {'token': 'your_token'}
    }
}

manager = DataSourceManager(config)
```

---

## 📦 项目结构

```
baostock/
├── src/                          # 源代码目录
│   ├── core/                     # 核心模块
│   │   ├── base_query.py         # 查询基类
│   │   └── connection.py         # BaoStock连接管理
│   ├── queries/                  # 查询模块
│   │   ├── balance_query.py      # 资产负债表查询
│   │   └── cashflow_query.py     # 现金流量表查询
│   ├── datasource/               # 数据源模块
│   │   ├── base_datasource.py    # 数据源基类
│   │   ├── baostock_datasource.py # BaoStock适配器
│   │   ├── tushare_datasource.py  # Tushare适配器
│   │   ├── akshare_datasource.py  # AkShare适配器
│   │   ├── datasource_factory.py  # 数据源工厂
│   │   └── datasource_manager.py  # 数据源管理器
│   ├── config/                   # 配置模块
│   │   └── config_loader.py      # 配置加载器
│   └── utils/                    # 工具模块
│       └── logger.py             # 日志工具
├── examples/                     # 示例代码
│   ├── basic_usage.py            # 基础使用示例
│   ├── advanced_usage.py         # 高级使用示例
│   ├── multi_datasource_usage.py # 多数据源示例
│   └── datasource_comparison.py  # 数据源对比
├── logs/                         # 日志文件目录（自动生成）
│   └── README.md                 # 日志目录说明
├── main.py                       # 主程序入口
├── setup.py                      # 安装配置
├── requirements.txt              # 依赖列表
├── config.example.json           # 配置文件模板
├── README.md                     # 项目文档（本文件）
├── DATASOURCE_README.md          # 多数据源文档
├── MULTI_DATASOURCE_SUMMARY.md   # 实施总结
└── LOGGING_GUIDE.md              # 日志系统使用指南
```

---

## 🎯 使用场景

### 1. 个人投资分析

```python
# 分析公司财务健康状况
from queries.balance_query import BalanceQuery
from queries.cashflow_query import CashFlowQuery

balance_query = BalanceQuery()
cashflow_query = CashFlowQuery()

# 查询资产负债率
balance_df = balance_query.query(code="600000", year=2023)
liability_ratio = balance_df['liabilityToAsset'].iloc[-1]

# 查询现金流
cashflow_df = cashflow_query.query(code="600000", year=2023)
operating_cashflow = cashflow_df['CAToAsset'].iloc[-1]

print(f"资产负债率: {liability_ratio}")
print(f"经营现金流: {operating_cashflow}")
```

### 2. 行业研究

```python
# 对比同行业公司
from queries.cashflow_query import CashFlowQuery

query = CashFlowQuery()

# 银行业对比
bank_codes = ["600000", "601398", "601939", "601288"]
comparison = query.compare_companies(
    codes=bank_codes,
    year=2023,
    metrics=['CAToAsset', 'NCAToAsset']
)

print(comparison.sort_values('CAToAsset', ascending=False))
```

### 3. 量化研究

```python
# 批量获取历史数据用于回测
from queries.cashflow_query import CashFlowQuery

query = CashFlowQuery()

# 获取5年历史数据
df = query.query_history(
    code="600000",
    start_year=2019,
    end_year=2023
)

# 导出为CSV供其他工具使用
df.to_csv("cashflow_history.csv", index=False)
```

### 4. 数据采集

```python
# 批量采集多家公司数据
from datasource import DataSourceManager
from queries.balance_query import BalanceQuery
import pandas as pd

config = {
    'default_source': 'baostock',
    'fallback_sources': ['akshare'],
}

with DataSourceManager(config) as manager:
    query = BalanceQuery(datasource=manager)
    
    # 批量查询
    codes = [f"60{i:04d}" for i in range(1, 100)]
    results = query.query_multiple(codes=codes, year=2023)
    
    # 合并数据
    all_data = pd.concat([df for df in results.values() if df is not None])
    all_data.to_csv("all_companies_balance.csv", index=False)
```

---

## 🔍 常见问题

### Q1: 如何获取 Tushare token？

访问 [Tushare官网](https://tushare.pro/register) 注册账号，在个人中心获取 token。

### Q2: 数据源查询失败怎么办？

1. 检查网络连接
2. 确认股票代码格式正确
3. 配置备用数据源实现自动故障转移
4. 查看日志获取详细错误信息

### Q3: 支持哪些股票代码格式？

系统自动识别并转换以下格式：
- `600000` - 通用格式
- `sh.600000` - BaoStock格式
- `600000.SH` - Tushare格式

### Q4: 如何添加新的数据源？

参考 [多数据源文档](DATASOURCE_README.md) 的"扩展开发"章节。

### Q5: 旧代码需要修改吗？

不需要！项目完全向后兼容，旧代码可以继续使用。

### Q6: 如何启用日志记录？

使用 `setup_logger` 函数即可自动启用日志记录到 `logs/` 目录：

```python
from src.utils.logger import setup_logger

logger = setup_logger('my_app')
logger.info("应用启动")
```

详见 [日志系统使用指南](LOGGING_GUIDE.md)。

---

## 🛠️ 开发

### 运行测试

```bash
# 快速测试
python test_datasource.py

# 完整测试
python examples/multi_datasource_usage.py
```

### 代码风格

项目遵循 PEP 8 代码规范。

---

## 🤝 贡献

欢迎贡献代码！你可以：

1. 🐛 报告 Bug
2. 💡 提出新功能建议
3. 📝 改进文档
4. 🔧 提交代码

### 贡献步骤

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

---

## 🙏 致谢

- [BaoStock](http://baostock.com/) - 提供免费的证券数据
- [Tushare](https://tushare.pro/) - 提供专业的金融数据接口
- [AkShare](https://github.com/akfamily/akshare) - 提供开源的金融数据接口

---

## 📮 联系方式

- 项目主页: [GitHub](https://github.com/yourusername/baostock)
- 问题反馈: [Issues](https://github.com/yourusername/baostock/issues)

---

## 📊 更新日志

### v0.2.1 (2026-01-16)

- ✨ 新增日志系统，自动保存到 `logs/` 目录
- ✨ 日志文件按日期和模块名称自动命名
- 📚 添加完整的日志使用指南
- 🔧 优化日志配置，支持多种使用场景

### v0.2.0 (2026-01-15)

- ✨ 新增多数据源支持
- ✨ 添加 Tushare 和 AkShare 数据源
- ✨ 实现自动故障转移机制
- ✨ 添加配置文件支持
- 📚 完善文档和示例
- 🐛 修复已知问题

### v0.1.0 (初始版本)

- ✨ 基础查询功能
- ✨ BaoStock 数据源支持
- ✨ 资产负债表和现金流量表查询

---

<div align="center">

**⭐ 如果这个项目对你有帮助，请给一个 Star！⭐**

Made with ❤️ by BaoStock Team

</div>
