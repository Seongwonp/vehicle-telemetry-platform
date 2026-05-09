"""
이상 감지 알림 발송 — Webhook
"""
import os
import logging
import requests

logger = logging.getLogger("notifier")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
WEBHOOK_TIMEOUT = int(os.getenv("WEBHOOK_TIMEOUT", "5"))


def send_webhook(anomaly_payload: dict) -> None:
    """이상 감지 이벤트를 Webhook으로 전송."""
    if not WEBHOOK_URL:
        logger.debug("WEBHOOK_URL 미설정 — 알림 스킵")
        return

    try:
        response = requests.post(
            WEBHOOK_URL,
            json=anomaly_payload,
            timeout=WEBHOOK_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        logger.info(f"웹훅 전송 성공 (status={response.status_code})")
    except requests.exceptions.Timeout:
        logger.error(f"웹훅 타임아웃 ({WEBHOOK_TIMEOUT}s): {WEBHOOK_URL}")
    except requests.exceptions.RequestException as e:
        logger.error(f"웹훅 전송 실패: {e}")
