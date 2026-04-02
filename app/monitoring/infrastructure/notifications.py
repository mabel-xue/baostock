"""
飞书机器人适配器（Adapter）：封装 Webhook post/text 与签名校验。
业务层只依赖本模块函数或 FeishuNotifier，不直接 requests 飞书。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


def get_webhook() -> str:
    return (os.environ.get("FEISHU_WEBHOOK_URL") or "").strip()


def get_secret() -> str | None:
    s = (os.environ.get("FEISHU_SECRET") or "").strip()
    return s or None


def _sign_payload(secret: str) -> dict[str, str]:
    ts = str(int(time.time()))
    string_to_sign = f"{ts}\n{secret}".encode("utf-8")
    sign = base64.b64encode(
        hmac.new(secret.encode("utf-8"), string_to_sign, hashlib.sha256).digest()
    ).decode()
    return {"timestamp": ts, "sign": sign}


def send_feishu_post(
    webhook_url: str,
    title: str,
    lines: list[str],
    *,
    secret: str | None = None,
) -> None:
    if not webhook_url:
        return
    secret = secret if secret is not None else get_secret()
    content_elements = [[{"tag": "text", "text": line}] for line in lines]
    msg: dict[str, Any] = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": content_elements,
                }
            }
        },
    }
    if secret:
        msg.update(_sign_payload(secret))
    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("飞书 post 失败: %s %s", resp.status_code, resp.text)
            return
        body = resp.json()
        sc = body.get("StatusCode")
        if sc is not None and sc != 0:
            logger.warning("飞书 post 返回异常: %s", body)
    except Exception as e:
        logger.warning("飞书 post 异常: %s", e)


def send_feishu_text(
    webhook_url: str,
    text: str,
    *,
    secret: str | None = None,
) -> None:
    if not webhook_url:
        raise ValueError("webhook 为空")
    secret = secret if secret is not None else get_secret()
    payload: dict[str, Any] = {
        "msg_type": "text",
        "content": {"text": text},
    }
    if secret:
        payload.update(_sign_payload(secret))
    r = requests.post(webhook_url, json=payload, timeout=15)
    r.raise_for_status()
    body = r.json()
    sc = body.get("StatusCode")
    if sc is not None and sc != 0:
        raise RuntimeError(f"飞书接口返回异常: {body}")
    if body.get("code") not in (0, None) and body.get("code") != 0:
        raise RuntimeError(f"飞书接口返回异常: {body}")


class FeishuNotifier:
    """
    可选 OOP 封装：同一 Webhook 上多次发送时可复用实例。
    """

    def __init__(self, webhook_url: str | None = None, *, secret: str | None = None) -> None:
        self._webhook = (webhook_url or get_webhook() or "").strip()
        self._secret = secret if secret is not None else get_secret()

    @property
    def webhook(self) -> str:
        return self._webhook

    def send_post(self, title: str, lines: list[str]) -> None:
        send_feishu_post(self._webhook, title, lines, secret=self._secret)

    def send_text(self, text: str) -> None:
        send_feishu_text(self._webhook, text, secret=self._secret)
