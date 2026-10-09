from types import SimpleNamespace

from shop import alerts


def context():
    ti = SimpleNamespace(dag_id="shop_daily", task_id="dbt_build", try_number=3, log_url="http://airflow/log")
    return {"task_instance": ti, "data_interval_end": "2026-01-03T06:00:00+00:00"}


def test_message_names_the_task_try_and_interval():
    text = alerts.format_failure(context())
    assert "shop_daily.dbt_build" in text and "try 3" in text and "2026-01-03" in text


def test_posts_to_slack_when_webhook_is_set(monkeypatch):
    sent = []
    monkeypatch.setattr(alerts, "_webhook_url", lambda: "https://hooks.slack.test/x")
    alerts.notify_failure(context(), post=lambda url, payload: sent.append((url, payload)))
    assert sent and sent[0][0].startswith("https://hooks.slack.test")


def test_only_logs_without_webhook(monkeypatch, caplog):
    monkeypatch.setattr(alerts, "_webhook_url", lambda: None)
    alerts.notify_failure(context(), post=lambda *a: (_ for _ in ()).throw(AssertionError("posted")))
    assert "not sent" in caplog.text
