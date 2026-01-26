# 基本面查询功能 - 更新日志

## 版本: v1.1.0
## 日期: 2026-01-26

---

## 📦 新增文件

### 核心代码

1. **src/queries/fundamental_query.py** ⭐
   - 基本面查询核心类 `FundamentalQuery`
   - 支持单个查询、批量查询、按名称查询
   - 自动匹配公司名称到股票代码
   - 自动提取和汇总关键财务指标

### 应用示例

2. **app/query_fundamental.py**
   - 预置示例：查询白酒行业基本面
   - 查询5家白酒公司（茅台、五粮液等）
   - 自动保存CSV文件

3. **app/query_fundamental_custom.py**
   - 自定义查询示例
   - 演示两种查询方式（按名称、按代码）
   - 包含白酒和银行两个行业示例

4. **app/query_fundamental_simple.py** ⭐ 推荐
   - 简化版本，不依赖复杂类结构
   - 直接使用akshare接口
   - 适合快速使用和学习

### 测试文件

5. **test_fundamental.py**
   - 测试基本面查询功能
   - 包含3个测试用例
   - 用于验证功能正确性

### 文档

6. **基本面查询使用指南.md** 📖
   - 完整的使用指南
   - 包含安装、使用、示例、FAQ
   - 约400行详细文档

7. **新增功能说明.md** 📋
   - 本次更新的完整说明
   - 技术架构和设计模式
   - 包含类图和数据流图

8. **基本面查询快速参考.md** 🚀
   - 快速参考卡片
   - 常用代码示例
   - 故障排除指南

9. **CHANGELOG_基本面查询.md**
   - 本文件
   - 更新日志和文件清单

---

## 🔧 修改文件

### 数据源层

1. **src/datasource/akshare_datasource.py**
   - 新增方法: `query_stock_fundamental()`
   - 使用akshare的 `stock_financial_analysis_indicator` 接口
   - 支持按年份过滤数据
   - 约70行新增代码

### 查询模块

2. **src/queries/__init__.py**
   - 新增导出: `FundamentalQuery`
   - 更新 `__all__` 列表

### 项目文档

3. **README.md**
   - 在"功能特性"部分添加基本面查询
   - 在"快速开始"部分添加使用示例
   - 添加文档链接
   - 约30行新增内容

---

## ✨ 新增功能

### 1. 基本面信息查询

- ✅ 查询单个公司基本面数据
- ✅ 批量查询多个公司
- ✅ 根据公司名称自动查询
- ✅ 自动匹配股票代码
- ✅ 提取关键财务指标
- ✅ 生成汇总表
- ✅ 导出CSV文件

### 2. 支持的指标

- 净利润 / 净利润(亿元)
- 营业总收入 / 营业总收入(亿元)
- 净资产收益率 / 净资产收益率(%) (ROE)
- 毛利率 / 毛利率(%)
- 总资产收益率 / 总资产收益率(%) (ROA)
- 资产负债率 / 资产负债率(%)
- 每股收益 / 每股收益(元) (EPS)
- 每股净资产 / 每股净资产(元)
- 市盈率 / PE
- 市净率 / PB

### 3. 使用方式

#### 方式1: 完整版（使用项目架构）
```python
from src.datasource.akshare_datasource import AkShareDataSource
from src.queries.fundamental_query import FundamentalQuery

with AkShareDataSource() as datasource:
    query = FundamentalQuery(datasource=datasource)
    df = query.query_by_names(names=["贵州茅台", "五粮液"], year=2024)
```

#### 方式2: 简化版（直接使用）
```bash
python app/query_fundamental_simple.py
```

#### 方式3: 自定义（灵活使用）
```python
import akshare as ak
df = ak.stock_financial_analysis_indicator(symbol="600519")
```

---

## 📊 代码统计

### 新增代码量

| 类型 | 文件数 | 代码行数 |
|------|--------|---------|
| 核心代码 | 1 | ~250行 |
| 应用示例 | 3 | ~350行 |
| 测试代码 | 1 | ~100行 |
| 文档 | 4 | ~1000行 |
| **总计** | **9** | **~1700行** |

### 修改代码量

| 文件 | 修改行数 |
|------|---------|
| akshare_datasource.py | +70行 |
| __init__.py | +2行 |
| README.md | +30行 |
| **总计** | **+102行** |

---

## 🎯 技术亮点

### 1. 设计模式

- **适配器模式**: AkShareDataSource适配akshare接口
- **模板方法模式**: BaseQuery定义查询流程
- **工厂模式**: 支持多数据源创建
- **策略模式**: 不同数据源实现统一接口

### 2. 架构特点

- **松耦合**: 查询层与数据源层分离
- **易扩展**: 可轻松添加其他数据源支持
- **高内聚**: 每个类职责单一明确
- **可测试**: 提供完整的测试用例

### 3. 用户体验

- **简单易用**: 3种使用方式，满足不同需求
- **文档完善**: 4份文档，覆盖各种场景
- **示例丰富**: 3个示例程序，开箱即用
- **错误处理**: 完善的异常处理和日志记录

---

## 🔄 数据流

```
用户输入
    ↓
公司名称 / 股票代码
    ↓
FundamentalQuery
    ↓
AkShareDataSource
    ↓
akshare.stock_financial_analysis_indicator()
    ↓
数据处理和汇总
    ↓
返回DataFrame
    ↓
显示 / 保存CSV
```

---

## 📋 依赖更新

### 新增依赖

- akshare >= 1.10.0 (已在requirements.txt中标注为可选)

### 现有依赖

- baostock >= 0.8.8
- pandas >= 1.3.0
- python-dateutil >= 2.8.0

---

## 🧪 测试覆盖

### 测试用例

1. **test_single_query**: 测试单个股票查询
2. **test_multiple_query**: 测试批量查询
3. **test_query_by_names**: 测试根据名称查询

### 测试文件

- test_fundamental.py

---

## 📚 文档结构

```
baostock/
├── 基本面查询使用指南.md        (详细使用指南)
├── 新增功能说明.md              (功能说明)
├── 基本面查询快速参考.md        (快速参考)
├── CHANGELOG_基本面查询.md      (本文件)
└── README.md                    (已更新)
```

---

## 🚀 快速开始

### 最简单的方式

```bash
cd /Users/mabelxue/web+/baostock
pip install akshare
python app/query_fundamental_simple.py
```

### 查看文档

```bash
# 快速参考
cat 基本面查询快速参考.md

# 详细指南
cat 基本面查询使用指南.md

# 功能说明
cat 新增功能说明.md
```

---

## ⚠️ 注意事项

1. **数据源要求**
   - 必须使用AkShare数据源
   - 免费无需token
   - 需要网络连接

2. **使用建议**
   - 公司名称使用简称
   - 建议查询上一年度数据
   - 一次查询不超过20家公司

3. **环境要求**
   - Python >= 3.7
   - pandas >= 1.3.0
   - akshare >= 1.10.0

---

## 🔮 未来计划

### 待实现功能

- [ ] 支持Tushare数据源
- [ ] 添加更多财务指标
- [ ] 支持行业对比分析
- [ ] 添加数据可视化
- [ ] 支持历史数据对比
- [ ] 添加财务健康度评分

### 优化方向

- [ ] 提升查询性能
- [ ] 添加缓存机制
- [ ] 支持异步查询
- [ ] 改进错误处理
- [ ] 添加更多测试用例

---

## 👥 贡献者

- [@mabelxue] - 基本面查询功能开发

---

## 📄 许可证

MIT License

---

## 📞 技术支持

如有问题，请：

1. 查看快速参考: `基本面查询快速参考.md`
2. 查看使用指南: `基本面查询使用指南.md`
3. 查看功能说明: `新增功能说明.md`
4. 运行示例程序: `python app/query_fundamental_simple.py`
5. 查看AkShare文档: https://akshare.akfamily.xyz/

---

## 📝 更新记录

### v1.1.0 (2026-01-26)

**新增**:
- 基本面查询功能
- 根据公司名称批量查询
- 3个示例程序
- 4份完整文档

**修改**:
- 更新AkShareDataSource
- 更新README
- 更新模块导出

**文件**:
- 新增: 9个文件
- 修改: 3个文件
- 总计: 约1800行代码和文档

---

*本更新日志记录了基本面查询功能的完整开发过程和所有相关文件。*
