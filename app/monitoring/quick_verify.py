"""
快速验证 monitoring 包能否导入、子 CLI 能否启动。

用法（必须在 app 目录下，或将 app 加入 PYTHONPATH）：
  cd app && python -m monitoring.quick_verify
  cd app && python -m monitoring.quick_verify --fetch   # 额外拉一轮转债（需网络）
"""

from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="monitoring 包烟测")
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="执行 bonds --once --dry-run（访问 AkShare，较慢）",
    )
    args = parser.parse_args()

    import monitoring.facade_cli  # noqa: F401
    import monitoring.bonds.service  # noqa: F401
    import monitoring.equity.service  # noqa: F401
    from monitoring.infrastructure import notifications  # noqa: F401

    print("imports: ok")

    exe = sys.executable
    for mod in ("monitoring.equity.cli", "monitoring.bonds.cli", "monitoring.facade_cli"):
        r = subprocess.run([exe, "-m", mod, "--help"], capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            print(f"FAIL {mod} --help rc={r.returncode}", file=sys.stderr)
            if r.stderr:
                print(r.stderr, file=sys.stderr)
            sys.exit(1)
    print("equity/bonds/facade --help: ok")

    if args.fetch:
        r = subprocess.run(
            [exe, "-m", "monitoring.bonds.cli", "--once", "--dry-run"],
            timeout=120,
        )
        if r.returncode != 0:
            sys.exit(r.returncode)
        print("bonds --once --dry-run: ok")

    print("quick_verify: 全部通过")


if __name__ == "__main__":
    main()
