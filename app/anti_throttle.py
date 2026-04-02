"""
反限制工具类 —— 统一封装 UA 轮换、自适应限速、指数退避重试等策略。

所有需要频繁调用外部数据接口的脚本均可引用本模块，避免重复实现。

典型用法::

    from anti_throttle import throttle, random_ua

    # 启动时初始化
    throttle.patch_akshare()

    # 带重试地调用
    df = throttle.retry(ak.stock_zh_b_spot, "B股行情")
    df = throttle.retry_df(lambda: ak.some_api(code=code), "财务数据")

    # 批量循环中使用自适应等待
    for item in items:
        result = do_request(item)
        if result:
            throttle.on_success()
        else:
            throttle.on_fail()
        throttle.wait()

    # 只需要随机 UA（给 requests.get 用）
    headers = {"User-Agent": random_ua()}
"""

from __future__ import annotations

import random
import time

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]


def random_ua() -> str:
    """返回一个随机 User-Agent 字符串。"""
    return random.choice(USER_AGENTS)


def random_headers() -> dict[str, str]:
    """返回一组模拟浏览器的完整 HTTP 请求头。"""
    return {
        "User-Agent": random_ua(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://finance.sina.com.cn/",
    }


def _is_throttled(msg: str) -> bool:
    """判断异常消息是否属于限流/反爬。"""
    low = msg.lower()
    return any(k in low for k in ("456", "403", "limit", "频繁", "请求过多", "too many"))


class AntiThrottle:
    """
    反限制核心类，集成以下策略：

    1. **随机 UA 轮换**  — 每次重试前自动切换 User-Agent
    2. **akshare headers 注入** — 向 akshare 底层 session 写入随机 UA + Referer
    3. **指数退避重试**  — delay = base * 2^(attempt-1) + jitter
    4. **自适应限速**    — 连续失败时自动加大请求间隔，恢复后逐步降回
    5. **冷却暂停**      — 连续失败超过阈值时暂停较长时间再继续
    """

    def __init__(
        self,
        base_delay: float = 0.3,
        max_delay: float = 8.0,
        retry_base: float = 1.5,
        max_retries: int = 3,
        cooldown_threshold: int = 10,
        cooldown_seconds: float = 15.0,
    ):
        self._base = base_delay
        self._max = max_delay
        self._delay = base_delay
        self._retry_base = retry_base
        self._max_retries = max_retries
        self._cooldown_threshold = cooldown_threshold
        self._cooldown_seconds = cooldown_seconds
        self._consecutive_fails = 0

    # ── 限速状态管理 ──

    def on_success(self):
        """请求成功时调用，重置失败计数并逐步降低延时。"""
        self._consecutive_fails = 0
        self._delay = max(self._base, self._delay * 0.7)

    def on_fail(self):
        """请求失败时调用，累加计数并在连续失败时翻倍延时。"""
        self._consecutive_fails += 1
        if self._consecutive_fails >= 3:
            self._delay = min(self._max, self._delay * 2)
            if self._consecutive_fails == 3:
                print(f"  ⚠ 连续失败 {self._consecutive_fails} 次，请求间隔升至 {self._delay:.1f}s")

    def wait(self):
        """自适应等待：当前延时 + 随机抖动。"""
        jitter = random.uniform(0, self._delay * 0.5)
        time.sleep(self._delay + jitter)

    def check_cooldown(self):
        """连续失败过多时执行冷却暂停，并刷新 headers。"""
        if self._consecutive_fails >= self._cooldown_threshold:
            print(f"  ⚠ 连续失败过多({self._consecutive_fails}次)，暂停 {self._cooldown_seconds:.0f}s 后继续 ...")
            time.sleep(self._cooldown_seconds)
            self.patch_akshare()

    @property
    def consecutive_fails(self) -> int:
        return self._consecutive_fails

    @property
    def current_delay(self) -> float:
        return self._delay

    # ── akshare headers 注入 ──

    def patch_akshare(self):
        """向 akshare 底层注入随机 User-Agent 与 Referer。"""
        try:
            import akshare.utils.cons as _cons
            if hasattr(_cons, "headers"):
                _cons.headers["User-Agent"] = random_ua()
        except Exception:
            pass
        try:
            import akshare as _ak
            sess = getattr(_ak, "_session", None) or getattr(_ak, "session", None)
            if sess and hasattr(sess, "headers"):
                sess.headers.update(random_headers())
        except Exception:
            pass
        try:
            import requests.utils
            requests.utils.default_headers = lambda: {
                "User-Agent": random_ua(),
                "Accept-Encoding": "gzip, deflate",
                "Accept": "*/*",
                "Connection": "keep-alive",
            }
        except Exception:
            pass

    # ── 指数退避 ──

    def _backoff_sleep(self, attempt: int):
        delay = self._retry_base * (2 ** (attempt - 1))
        jitter = random.uniform(0, delay * 0.5)
        time.sleep(delay + jitter)

    # ── 通用重试 ──

    def retry(self, fn, label: str, retries: int | None = None):
        """
        带反限制的重试调用（返回任意非 None 即成功）。

        :param fn:      无参可调用对象
        :param label:   日志标识
        :param retries: 重试次数，默认使用实例配置
        """
        retries = retries or self._max_retries
        for attempt in range(1, retries + 1):
            self.patch_akshare()
            try:
                result = fn()
                if result is not None:
                    self.on_success()
                    return result
            except Exception as e:
                self.on_fail()
                msg = str(e)[:120]
                tag = "被限流" if _is_throttled(msg) else "失败"
                print(f"  {label}: 第{attempt}次{tag} — {msg}")
            if attempt < retries:
                self._backoff_sleep(attempt)
        return None

    def retry_df(self, fn, label: str, retries: int = 2):
        """
        带反限制的重试调用（返回非空 DataFrame 才算成功）。

        :param fn:      无参可调用对象，应返回 DataFrame
        :param label:   日志标识
        :param retries: 重试次数
        """
        for attempt in range(1, retries + 1):
            self.patch_akshare()
            try:
                d = fn()
                if d is not None and not getattr(d, "empty", True):
                    self.on_success()
                    return d
            except Exception as e:
                self.on_fail()
                msg = str(e)[:120]
                tag = "被限流" if _is_throttled(msg) else "失败"
                print(f"  {label}: 第{attempt}次{tag} — {msg}")
            if attempt < retries:
                self._backoff_sleep(attempt)
        return None


# 全局默认实例，大多数场景直接 from anti_throttle import throttle 即可
throttle = AntiThrottle()
