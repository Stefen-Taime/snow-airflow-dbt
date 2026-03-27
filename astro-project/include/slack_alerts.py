"""
Slack alert utilities for Airflow DAG callbacks.

Uses Block Kit (nested inside attachments for color bar) via requests.post
to the Slack Incoming Webhook.
No dependency on apache-airflow-providers-slack (avoids Airflow 3 notifier bugs).

Webhook URL is read from Airflow Variable `slack_webhook_url`
(set via env var AIRFLOW_VAR_SLACK_WEBHOOK_URL in .env).
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _get_webhook_url() -> str | None:
    """Retrieve Slack webhook URL from Airflow variables."""
    try:
        return Variable.get("slack_webhook_url", default_var=None)
    except Exception:
        logger.warning("Could not retrieve slack_webhook_url variable")
        return None


def _build_blocks(
    header_emoji: str,
    header_text: str,
    fields: list[tuple[str, str]],
    error: str | None = None,
) -> list[dict]:
    """Build Block Kit blocks for a Slack notification.

    Args:
        header_emoji: Slack emoji for the header (e.g. ":white_check_mark:").
        header_text: Main header text (e.g. "DAG Success").
        fields: List of (label, value) tuples shown as columns.
        error: Optional error message shown in a code block.
    """
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{header_emoji}  {header_text}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{label}:*\n`{value}`"}
                for label, value in fields
            ],
        },
    ]

    if error:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{str(error)[:500]}```",
                },
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "TLC Pipeline | Airflow 3 | Snowflake",
                }
            ],
        }
    )

    return blocks


def _send_slack_message(blocks: list[dict], color: str, fallback: str) -> None:
    """Send a Block Kit message to Slack via incoming webhook.

    Blocks are nested inside an attachment to preserve the color sidebar.
    """
    webhook_url = _get_webhook_url()
    if not webhook_url:
        logger.warning("Slack webhook URL not configured, skipping notification")
        return

    payload = {
        "attachments": [
            {
                "color": color,
                "fallback": fallback,
                "blocks": blocks,
            }
        ]
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code != 200:
            logger.warning("Slack returned %s: %s", resp.status_code, resp.text)
        else:
            logger.info("Slack notification sent successfully")
    except Exception as e:
        logger.warning("Failed to send Slack notification: %s", e)


# ---------------------------------------------------------------------------
# Airflow callbacks
# ---------------------------------------------------------------------------
def on_dag_success(context: dict[str, Any]) -> None:
    """Callback for DAG success — sends a green Slack notification."""
    dag_id = context.get("dag", {})
    dag_id = getattr(dag_id, "dag_id", str(dag_id))
    run_id = context.get("run_id", "unknown")
    logical_date = context.get("logical_date", "unknown")

    blocks = _build_blocks(
        header_emoji=":white_check_mark:",
        header_text="DAG Success",
        fields=[
            ("DAG", dag_id),
            ("Run", run_id),
            ("Date", str(logical_date)),
        ],
    )
    _send_slack_message(blocks, color="#36a64f", fallback=f"DAG Success: {dag_id}")


def on_dag_failure(context: dict[str, Any]) -> None:
    """Callback for DAG failure — sends a red Slack notification."""
    dag_id = context.get("dag", {})
    dag_id = getattr(dag_id, "dag_id", str(dag_id))
    run_id = context.get("run_id", "unknown")
    logical_date = context.get("logical_date", "unknown")
    exception = context.get("exception", "No exception info")

    blocks = _build_blocks(
        header_emoji=":x:",
        header_text="DAG Failed",
        fields=[
            ("DAG", dag_id),
            ("Run", run_id),
            ("Date", str(logical_date)),
        ],
        error=str(exception),
    )
    _send_slack_message(blocks, color="#ff0000", fallback=f"DAG Failed: {dag_id}")


def on_task_failure(context: dict[str, Any]) -> None:
    """Callback for task failure — sends an orange Slack notification."""
    dag_id = context.get("dag", {})
    dag_id = getattr(dag_id, "dag_id", str(dag_id))
    task_id = context.get("task_instance", {})
    task_id = getattr(task_id, "task_id", str(task_id))
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception", "No exception info")

    blocks = _build_blocks(
        header_emoji=":warning:",
        header_text="Task Failed",
        fields=[
            ("DAG", dag_id),
            ("Task", task_id),
            ("Run", run_id),
        ],
        error=str(exception),
    )
    _send_slack_message(blocks, color="#ff9900", fallback=f"Task Failed: {dag_id}.{task_id}")


def send_custom_alert(message: str, color: str = "#439FE0") -> None:
    """Send a custom Slack message (for use in PythonOperator tasks)."""
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": "TLC Pipeline | Airflow 3 | Snowflake"}
            ],
        },
    ]
    _send_slack_message(blocks, color=color, fallback=message[:150])
