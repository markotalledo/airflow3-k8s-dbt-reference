"""Manual full rebuild of the incremental models. Never scheduled.

Kept separate from the daily DAG so a full refresh is a deliberate, visible action
in the UI, and it competes for the same warehouse pool as everything else.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from shop.alerts import notify_failure
from shop.pods import pod

with DAG(
    dag_id="shop_full_refresh",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["shop", "manual"],
    doc_md=__doc__,
) as dag:
    pod(
        task_id="dbt_full_refresh",
        image="dbt-shop:0.1.0",
        arguments=["build", "--full-refresh"],
        pool="warehouse",
    )
