"""Gold Layer Storage — analytics-ready aggregated tables."""

from pathlib import Path

import pandas as pd
from loguru import logger

from config.settings import Config


class GoldStorage:
    """File-system helper for the Gold (aggregated) data layer.

    Gold tables are single Parquet files (no date partitioning) because they
    are fully rebuilt aggregates consumed directly by the API and dashboards.

    Layout: data/gold/<table_name>/<table_name>.parquet
    """

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Config.GOLD_DIR

    def destination(self, table_name: str) -> Path:
        """Return the canonical path for a Gold table."""
        return self.root / table_name / f"{table_name}.parquet"

    def write(self, df: pd.DataFrame, table_name: str) -> Path:
        """Overwrite the Gold table with the given DataFrame."""
        dest = self.destination(table_name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest, index=False)
        logger.info("[gold] Written {} ({} rows)", dest.relative_to(self.root), len(df))
        return dest

    def read(self, table_name: str) -> pd.DataFrame:
        """Read a Gold table into a DataFrame."""
        dest = self.destination(table_name)
        if not dest.exists():
            raise FileNotFoundError(f"Gold table not found: {dest}")
        return pd.read_parquet(dest)

    def exists(self, table_name: str) -> bool:
        return self.destination(table_name).exists()
