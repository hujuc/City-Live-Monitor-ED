"""
CLI Entry Point

Command-line interface for the City Live Monitor platform.
"""

import time

import click
import uvicorn
from loguru import logger

from config.settings import Config
from backend.ingestion.streaming import Noise311Poller, PermittedEventsPoller
from backend.utils.logging import setup_logging


@click.group()
@click.option("--log-level", default=Config.LOG_LEVEL, show_default=True,
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False))
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


if __name__ == "__main__":
    cli()
