"""
Silver Layer Storage

Manages the cleaned/normalized data zone. Files are written as Parquet,
partitioned by source and date for efficient querying.
  data/silver/<source>/year=<Y>/month=<M>/<filename>
"""

from pathlib import Path

from loguru import logger

from config.settings import Config


class SilverStorage:
    """File-system helper for the Silver (cleaned) data layer."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Config.SILVER_DIR

    def destination(self, source: str, year: int, month: int, filename: str) -> Path:
        """Return the canonical path for a processed file."""
        return self.root / source / f"year={year}" / f"month={month:02d}" / filename

    def list_files(self, source: str | None = None, pattern: str = "*.parquet") -> list[Path]:
        """List all files in the silver layer, optionally filtered by source."""
        search_root = self.root / source if source else self.root
        return sorted(search_root.rglob(pattern))

    def write(self, df, source: str, year: int, month: int, filename: str) -> Path:
        """Write a DataFrame to the silver layer as Parquet."""
        dest = self.destination(source, year, month, filename)
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest, index=False)
        logger.info("[silver] Written {}", dest.relative_to(self.root))
        return dest
