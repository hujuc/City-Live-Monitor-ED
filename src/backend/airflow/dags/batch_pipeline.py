# pyright: reportMissingImports=false

"""Airflow DAG for the batch processing pipeline.

Pipeline:
0) Download TLC files into Bronze (incremental)
1) Process TLC Bronze -> Silver
2) Process Events Bronze -> Silver
3) Process Noise311 Bronze -> Silver
4) Build all Gold tables (writes to local FS + MinIO)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

_log = logging.getLogger("city-live-monitor.dag")


class _AirflowStub:
    """Fallback object used when airflow isn't installed in local environments."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        pass

    def __enter__(self) -> _AirflowStub:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def __rshift__(self, other):  # noqa: ANN001
        return other


try:
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # pragma: no cover - airflow is available in the Airflow container
    DAG = _AirflowStub
    BashOperator = _AirflowStub


def _on_failure(context: dict) -> None:  # type: ignore[type-arg]
    ti = context.get("task_instance")
    exc = context.get("exception")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    _log.error(
        "[dag:%s] Task '%s' FAILED on run %s — %s: %s",
        dag_id,
        ti.task_id if ti else "?",
        context.get("run_id", "?"),
        type(exc).__name__ if exc else "unknown error",
        exc,
    )


DEFAULT_ARGS = {
    "owner": "city-live-monitor",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="city_live_monitor_batch_pipeline",
    description="Bronze -> Silver -> Gold batch pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["city-live-monitor", "batch", "medallion"],
) as dag:
    ingest_tlc_bronze = BashOperator(
        task_id="ingest_tlc_to_bronze",
        bash_command=(
            "cd /opt/airflow/src && "
            "python -m backend.ingestion.batch.download_tlc "
            "--taxi-types yellow --years 2023 2024 2025 --workers 4"
        ),
    )

    process_tlc = BashOperator(
        task_id="process_tlc_bronze_to_silver",
        bash_command=(
            "cd /opt/airflow/src && "
            "python -m backend.cli.main silver-process-tlc --taxi-type yellow"
        ),
    )

    process_events = BashOperator(
        task_id="process_events_bronze_to_silver",
        bash_command=("cd /opt/airflow/src && python -m backend.cli.main silver-process-events"),
    )

    process_noise = BashOperator(
        task_id="process_noise311_bronze_to_silver",
        bash_command=("cd /opt/airflow/src && python -m backend.cli.main silver-process-noise311"),
    )

    build_gold = BashOperator(
        task_id="build_gold_tables",
        bash_command="cd /opt/airflow/src && python -m backend.cli.main gold-build-all",
    )

    ingest_tlc_bronze >> process_tlc
    _ = [process_tlc, process_events, process_noise] >> build_gold  # type: ignore[operator]
