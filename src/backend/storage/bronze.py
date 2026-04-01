"""
Bronze Layer Storage

Manages the raw data landing zone. Files are organised as:
  data/bronze/<source>/<year>/<filename>
"""

from pathlib import Path

from loguru import logger

from config.settings import Config


class BronzeStorage:
    """File-system helper for the Bronze (raw) data layer."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Config.BRONZE_DIR

    def destination(self, source: str, year: int, filename: str) -> Path:
        """Return the canonical path for a raw file."""
        return self.root / source / str(year) / filename

    def exists(self, source: str, year: int, filename: str) -> bool:
        return self.destination(source, year, filename).exists()

    def list_files(self, source: str | None = None, pattern: str = "*.parquet") -> list[Path]:
        """List all files in the bronze layer, optionally filtered by source."""
        search_root = self.root / source if source else self.root
        return sorted(search_root.rglob(pattern))

    def record(self, source: str, year: int, filename: str, size_bytes: int) -> None:
        """Log that a file was written to the bronze layer."""
        logger.info(
            "[bronze] Recorded {}/{}/{} ({:.1f} MB)", source, year, filename, size_bytes / 1e6
        )
