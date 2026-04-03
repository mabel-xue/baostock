"""
门面（Facade）：统一 CLI，组合 equity / bonds 两个子域。

不改变各子域内部逻辑，仅负责 argv 分发与「同时运行」的线程编排。
"""

from __future__ import annotations

import argparse
import sys
import threading


def _split_argv() -> tuple[str, list[str]]:
    av = sys.argv[1:]
    if not av:
        return "equity", []
    if av[0] in ("equity", "bond", "all"):
        return av[0], av[1:]
    return "equity", av


def main() -> None:
    mode, rest = _split_argv()
    prog = sys.argv[0]

    if mode == "equity":
        sys.argv = [prog] + rest
        from .equity.cli import main as equity_main

        equity_main()
        return

    if mode == "bond":
        sys.argv = [prog] + rest
        from .bonds.cli import main as bond_main

        bond_main()
        return

    ap = argparse.ArgumentParser(description="同时：可转债后台轮询 + 场内前台盯盘")
    ap.add_argument("--cb-interval", type=int, default=600, help="可转债轮询间隔（秒）")
    args, unknown = ap.parse_known_args(rest)
    sys.argv = [prog] + unknown

    from .bonds.service import run_cb_forever

    threading.Thread(
        target=lambda: run_cb_forever(
            interval=args.cb_interval,
            once=False,
            dry_run=False,
        ),
        name="monitor-cb",
        daemon=True,
    ).start()

    from .equity.cli import main as equity_main

    equity_main()


if __name__ == "__main__":
    main()
