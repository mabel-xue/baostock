"""
场内证券监控包（分层架构）

- infrastructure/  基础设施：第三方通知适配器等
- equity/            场内股票/ETF 盯盘（腾讯行情）
- bonds/             可转债规则监控（AkShare）
- facade_cli.py      门面：统一子命令 equity | bond | all

启动（在 app 目录下，或将 app 加入 PYTHONPATH）：
  cd app && python -m monitoring.facade_cli equity
  cd app && python -m monitoring.facade_cli bond
  cd app && python -m monitoring.facade_cli all
  cd app && python -m monitoring.equity.cli
  cd app && python -m monitoring.bonds.cli
"""

from __future__ import annotations

__all__ = [
    "equity",
    "bonds",
    "infrastructure",
]
