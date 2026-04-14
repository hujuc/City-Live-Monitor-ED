"""
CLI Entry Point

Command-line interface for the City Live Monitor platform.
"""

import time
from pathlib import Path

import click
import uvicorn
from loguru import logger

from backend.ingestion.streaming import Noise311Poller, PermittedEventsPoller
from backend.processing.batch import (
    GoldEventScheduleProcessor,
    GoldNoiseAggregationProcessor,
    GoldTaxiDemandProcessor,
    Noise311SilverProcessor,
    PermittedEventsSilverProcessor,
    TLCBatchProcessor,
)
from backend.utils.logging import setup_logging
from config.settings import Config


@click.group()
@click.option(
    "--log-level",
    default=Config.LOG_LEVEL,
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
def cli(log_level: str) -> None:
    """City Live Monitor — data pipeline CLI."""
    setup_logging(log_level=log_level, log_file=Config.LOGS_DIR / "pipeline.log")


@cli.command("api")
@click.option("--host", default=Config.API_HOST, show_default=True)
@click.option("--port", default=Config.API_PORT, show_default=True)
def api(host: str, port: int) -> None:
    """Start the FastAPI REST server."""
    uvicorn.run("backend.api.main:app", host=host, port=port, reload=True)


@cli.command("stream-events")
@click.option("--once/--daemon", default=True, show_default=True)
def stream_events(once: bool) -> None:
    """Run Permitted Events poller one time or in continuous mode."""
    try:
        poller = PermittedEventsPoller()
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        if once:
            poller.ingest()
            return

        logger.info("[cli] Starting events poller in daemon mode")
        while True:
            poller.ingest()
            time.sleep(Config.EVENTS_POLL_SECONDS)
    finally:
        poller.close()


@cli.command("stream-noise311")
@click.option("--once/--daemon", default=True, show_default=True)
def stream_noise311(once: bool) -> None:
    """Run 311 Noise poller one time or in continuous mode."""
    try:
        poller = Noise311Poller()
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        if once:
            poller.ingest()
            return

        logger.info("[cli] Starting 311 noise poller in daemon mode")
        while True:
            poller.ingest()
            time.sleep(Config.NOISE311_POLL_SECONDS)
    finally:
        poller.close()


@cli.command("batch-process-tlc")
@click.option("--taxi-type", default="yellow", show_default=True)
@click.option("--max-files", type=int, default=None)
def batch_process_tlc(taxi_type: str, max_files: int | None) -> None:
    """Process TLC Bronze parquet files into Silver partitions."""
    bronze_root = Config.BRONZE_DIR / "tlc" / taxi_type
    destination = Path(f"tlc/{taxi_type}")
    processor = TLCBatchProcessor()

    stats = processor.process_bronze_tree(
        bronze_root=bronze_root,
        destination=destination,
        max_files=max_files,
    )
    logger.info(
        "[cli] batch-process-tlc finished: files={} rows_in={} rows_out={} dropped={}",
        stats.files_processed,
        stats.rows_read,
        stats.rows_written,
        stats.rows_dropped,
    )


@cli.command("silver-process-events")
@click.option("--max-files", type=int, default=None)
def silver_process_events(max_files: int | None) -> None:
    """Process Permitted Events Bronze NDJSON snapshots into Silver partitions."""
    bronze_root = Config.BRONZE_DIR / "nyc_permitted_events"
    destination = Path("events")
    processor = PermittedEventsSilverProcessor()

    stats = processor.process_bronze_tree(
        bronze_root=bronze_root,
        destination=destination,
        max_files=max_files,
    )
    logger.info(
        "[cli] silver-process-events finished: files={} rows_in={} rows_out={} dropped={}",
        stats.files_processed,
        stats.rows_read,
        stats.rows_written,
        stats.rows_dropped,
    )


@cli.command("silver-process-noise311")
@click.option("--max-files", type=int, default=None)
def silver_process_noise311(max_files: int | None) -> None:
    """Process 311 Noise Bronze NDJSON snapshots into Silver partitions."""
    bronze_root = Config.BRONZE_DIR / "nyc_311_noise"
    destination = Path("noise311")
    processor = Noise311SilverProcessor()

    stats = processor.process_bronze_tree(
        bronze_root=bronze_root,
        destination=destination,
        max_files=max_files,
    )
    logger.info(
        "[cli] silver-process-noise311 finished: files={} rows_in={} rows_out={} dropped={}",
        stats.files_processed,
        stats.rows_read,
        stats.rows_written,
        stats.rows_dropped,
    )


@cli.command("gold-build-taxi-demand")
def gold_build_taxi_demand() -> None:
    """Aggregate Silver TLC trips into Gold taxi demand table."""
    rows = GoldTaxiDemandProcessor().build()
    logger.info("[cli] gold-build-taxi-demand finished: {} rows written", rows)


@cli.command("gold-build-event-schedule")
def gold_build_event_schedule() -> None:
    """Build Gold event schedule table from Silver events."""
    rows = GoldEventScheduleProcessor().build()
    logger.info("[cli] gold-build-event-schedule finished: {} rows written", rows)


@cli.command("gold-build-noise")
def gold_build_noise() -> None:
    """Aggregate Silver noise311 complaints into Gold noise table."""
    rows = GoldNoiseAggregationProcessor().build()
    logger.info("[cli] gold-build-noise finished: {} rows written", rows)


@cli.command("gold-build-all")
def gold_build_all() -> None:
    """Build all three Gold tables in sequence."""
    taxi_rows = GoldTaxiDemandProcessor().build()
    event_rows = GoldEventScheduleProcessor().build()
    noise_rows = GoldNoiseAggregationProcessor().build()
    logger.info(
        "[cli] gold-build-all finished: taxi={} events={} noise={} rows",
        taxi_rows,
        event_rows,
        noise_rows,
    )


if __name__ == "__main__":
    cli()
