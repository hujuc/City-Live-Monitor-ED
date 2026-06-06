# pyright: reportMissingImports=false

"""Airflow DAG for daily NYC nightlife activity prediction refresh.

Pipeline:
1) Fetch latest noise311 daily counts from Socrata (yesterday → today)
2) Write Gold prediction parquet using the current trained model

Retraining runs separately on a weekly schedule (see weekly_retrain.py).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

_log = logging.getLogger("city-live-monitor.dag")


class _AirflowStub:
    """Fallback used when airflow isn't installed in local environments."""

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
except ImportError:  # pragma: no cover
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
    dag_id="city_live_monitor_daily_prediction",
    description="Refresh noise counts → generate activity predictions (no retrain)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # 06:00 UTC — Socrata 311 data typically stabilises overnight
    catchup=False,
    max_active_runs=1,
    tags=["city-live-monitor", "ml", "prediction"],
) as dag:
    refresh_noise = BashOperator(
        task_id="refresh_noise_counts",
        bash_command=(
            "cd /opt/airflow/src && python -m backend.cli.main backfill-noise311-daily-counts "
            "--start-date {{ macros.ds_add(ds, -1) }} --end-date {{ ds }}"
        ),
    )

    predict = BashOperator(
        task_id="generate_predictions",
        bash_command="cd /opt/airflow/src && python -m backend.cli.main predict-activity",
    )

    refresh_noise >> predict
