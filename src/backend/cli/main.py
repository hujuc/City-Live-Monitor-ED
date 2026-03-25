"""
CLI Entry Point

Command-line interface for the City Live Monitor platform.
"""

import click
import uvicorn

from config.settings import Config
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


if __name__ == "__main__":
    cli()
