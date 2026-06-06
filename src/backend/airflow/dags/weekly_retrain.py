# pyright: reportMissingImports=false

"""Airflow DAG for weekly ML classifier retraining.

Pipeline:
1) Backfill the last 7 days of noise311 counts from Socrata
2) Retrain the activity classifier on the extended dataset
3) Write fresh Gold prediction parquet

Runs weekly (Sunday 05:00 UTC) so the daily prediction DAG always has
an up-to-date model without paying retrain cost every day.
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
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="city_live_monitor_weekly_retrain",
    description="Weekly: backfill noise counts → retrain classifier → generate predictions",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * 0",  # 05:00 UTC every Sunday — runs before daily prediction at 06:00
    catchup=False,
    max_active_runs=1,
    tags=["city-live-monitor", "ml", "retrain"],
) as dag:
    backfill_noise = BashOperator(
        task_id="backfill_noise_last_7_days",
        bash_command=(
            "cd /opt/airflow/src && python -m backend.cli.main backfill-noise311-daily-counts "
            "--start-date {{ macros.ds_add(ds, -7) }} --end-date {{ ds }}"
        ),
    )

    retrain = BashOperator(
        task_id="retrain_classifier",
        bash_command="cd /opt/airflow/src && python -m backend.cli.main train-model",
    )

    predict = BashOperator(
        task_id="generate_predictions",
        bash_command="cd /opt/airflow/src && python -m backend.cli.main predict-activity",
    )

    backfill_noise >> retrain >> predict
