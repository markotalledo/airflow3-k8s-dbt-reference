"""Daily build of the shop data platform.

clean_events (PySpark) rebuilds the last few event dates from raw events, then
dbt_build rebuilds the warehouse and runs its tests. Both steps rewrite a window
instead of appending, so retries, reruns and backfills give the same result.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from shop.alerts import notify_failure
from shop.pods import pod

# The date being processed is the end of the data interval: a run scheduled for
# 2026-01-03 06:00 covers 2026-01-02 06:00 to 2026-01-03 06:00. execution_date no
# longer exists in Airflow 3.
RUN_DATE = "{{ data_interval_end | ds }}"
LOOKBACK_DAYS = "3"

with DAG(
    dag_id="shop_daily",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
    },
    tags=["shop", "daily"],
    doc_md=__doc__,
) as dag:
    clean_events = pod(
        task_id="clean_events",
        image="clean-events:0.1.0",
        arguments=[
            "--raw_path",
            "s3a://shop-events-prod-raw/events",
            "--clean_path",
            "s3a://shop-events-prod-clean",
            "--run_date",
            RUN_DATE,
            "--lookback_days",
            LOOKBACK_DAYS,
        ],
        pool="spark",
        cpu="2",
        memory="4Gi",
    )

    dbt_build = pod(
        task_id="dbt_build",
        image="dbt-shop:0.1.0",
        arguments=["build", "--vars", "{funnel_lookback_days: " + LOOKBACK_DAYS + "}"],
        pool="warehouse",
    )

    clean_events >> dbt_build
