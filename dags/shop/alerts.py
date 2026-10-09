"""Failure alerts to Slack through an incoming webhook.

The webhook URL comes from the Airflow Variable `slack_webhook_url`. Without it the
alert is only logged, so local runs and tests never need Slack.
"""

from __future__ import annotations

import json
import logging
import urllib.request

log = logging.getLogger(__name__)


def _webhook_url() -> str | None:
    try:
        from airflow.sdk import Variable

        return Variable.get("slack_webhook_url", default=None)
    except Exception:  # no metadata DB, e.g. in unit tests
        return None


def format_failure(context: dict) -> str:
    ti = context["task_instance"]
    interval_end = context.get("data_interval_end")
    return (
        f":red_circle: *{ti.dag_id}.{ti.task_id}* failed "
        f"(try {ti.try_number}) for interval ending {interval_end}.\n"
        f"Logs: {getattr(ti, 'log_url', 'n/a')}"
    )


def notify_failure(context: dict, post=None) -> None:
    text = format_failure(context)
    url = _webhook_url()
    if not url:
        log.warning("slack_webhook_url not set, alert not sent: %s", text)
        return
    send = post or _post
    send(url, {"text": text})


def _post(url: str, payload: dict) -> None:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10):
        pass
