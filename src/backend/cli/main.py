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
from backend.processing.batch import TLCBatchProcessor
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


if __name__ == "__main__":
    cli()
