"""One factory for every pod, so resources, pools and cleanup are set in one place."""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = "airflow"
REGISTRY = "ghcr.io/markotalledo"

# Pools are the concurrency limit on shared systems. `warehouse` caps how many
# queries hit the warehouse at once across every DAG, no matter how many DAGs
# are scheduled at the same minute.
POOLS = {
    "warehouse": {"slots": 2, "description": "Concurrent dbt runs against the warehouse"},
    "spark": {"slots": 1, "description": "Concurrent Spark jobs"},
}


def pod(
    task_id: str,
    image: str,
    arguments: list[str],
    pool: str,
    cpu: str = "500m",
    memory: str = "1Gi",
    timeout: timedelta = timedelta(minutes=30),
    env: dict[str, str] | None = None,
) -> KubernetesPodOperator:
    if pool not in POOLS:
        raise ValueError(f"unknown pool {pool!r}")
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=f"{REGISTRY}/{image}",
        arguments=arguments,
        env_vars=[k8s.V1EnvVar(name=k, value=v) for k, v in (env or {}).items()],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu, "memory": memory},
            limits={"memory": memory},  # no CPU limit: throttling hurts more than it protects
        ),
        pool=pool,
        execution_timeout=timeout,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",  # keep failed pods for debugging
        startup_timeout_seconds=300,
        service_account_name="shop-pipelines",
    )
