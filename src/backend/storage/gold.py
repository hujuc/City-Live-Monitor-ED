"""Gold Layer Storage — analytics-ready aggregated tables.

Each Gold table is written locally and mirrored to MinIO (fail-soft) so the
lakehouse has a consistent copy regardless of whether the local FS is mounted.

Local layout : data/gold/<table_name>/<table_name>.parquet
MinIO layout : s3://gold-layer/<table_name>/<table_name>.parquet
"""

from pathlib import Path

import pandas as pd
from loguru import logger

from backend.utils.minio_client import upload_file
from config.settings import Config


class GoldStorage:
    """File-system + MinIO helper for the Gold (aggregated) data layer."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Config.GOLD_DIR

    def destination(self, table_name: str) -> Path:
        """Return the canonical local path for a Gold table."""
        return self.root / table_name / f"{table_name}.parquet"

    def write(self, df: pd.DataFrame, table_name: str) -> Path:
        """Overwrite the Gold table locally and mirror to MinIO."""
        dest = self.destination(table_name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest, index=False)
        logger.info("[gold] Written {} ({} rows)", dest.relative_to(self.root), len(df))

        object_key = f"{table_name}/{table_name}.parquet"
        upload_file(dest, Config.MINIO_GOLD_BUCKET, object_key)

        return dest

    def read(self, table_name: str) -> pd.DataFrame:
        """Read a Gold table into a DataFrame."""
        dest = self.destination(table_name)
        if not dest.exists():
            raise FileNotFoundError(f"Gold table not found: {dest}")
        return pd.read_parquet(dest)

    def exists(self, table_name: str) -> bool:
        return self.destination(table_name).exists()
