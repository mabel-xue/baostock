"""飞书 webhook 通知（供 monitor_big_orders、日程提醒等复用）"""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

FEISHU_WEBHOOK_URL = os.environ.get(
    "FEISHU_WEBHOOK_URL",
    "https://open.feishu.cn/open-apis/bot/v2/hook/44073acd-feb1-4da9-828d-d3d3a77e9a53",
)


def send_feishu(webhook_url: str, title: str, lines: list[str]) -> None:
    """通过飞书 webhook 发送富文本 post 消息"""
    if not webhook_url:
        return
    content_elements = [[{"tag": "text", "text": line}] for line in lines]
    msg = {
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
    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=5,
        )
        if resp.status_code != 200:
            logger.warning("飞书通知发送失败: %s %s", resp.status_code, resp.text)
    except Exception as e:
        logger.warning("飞书通知异常: %s", e)
