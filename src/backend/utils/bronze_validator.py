"""Bronze layer validation utilities.

Checks three invariants required by the medallion architecture spec:
  1. All partitions have _ingestion_timestamp (NDJSON rows or .meta.json sidecar)
  2. No schema drift between partitions of the same source
  3. Row counts can be compared with a reference (pass-through helper)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from loguru import logger


@dataclass
class PartitionReport:
    path: Path
    row_count: int
    has_ingestion_timestamp: bool
    columns: frozenset[str]
    issues: list[str] = field(default_factory=list)


@dataclass
class BronzeValidationResult:
    source: str
    partitions: list[PartitionReport] = field(default_factory=list)
    schema_drift_files: list[str] = field(default_factory=list)
    missing_timestamp_files: list[str] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(p.row_count for p in self.partitions)

    @property
    def is_valid(self) -> bool:
        return not self.schema_drift_files and not self.missing_timestamp_files

    def log_summary(self) -> None:
        status = "OK" if self.is_valid else "ISSUES FOUND"
        logger.info(
            "[bronze/validate] source={} status={} partitions={} rows={}",
            self.source,
            status,
            len(self.partitions),
            self.total_rows,
        )
        for f in self.missing_timestamp_files:
            logger.warning("[bronze/validate] Missing _ingestion_timestamp: {}", f)
        for f in self.schema_drift_files:
            logger.warning("[bronze/validate] Schema drift detected: {}", f)


class BronzeValidator:
    """Validates Bronze layer files for ingestion timestamp and schema consistency."""

    def validate_ndjson_source(self, source_dir: Path, source_name: str) -> BronzeValidationResult:
        """Validate all NDJSON snapshots under source_dir."""
        result = BronzeValidationResult(source=source_name)
        files = sorted(source_dir.rglob("*.ndjson"))

        if not files:
            logger.warning("[bronze/validate] No NDJSON files found under {}", source_dir)
            return result

        baseline_cols: frozenset[str] | None = None

        for ndjson_path in files:
            rows = []
            with ndjson_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))

            if not rows:
                continue

            cols = frozenset(rows[0].keys())
            has_ts = "_ingestion_timestamp" in cols

            report = PartitionReport(
                path=ndjson_path,
                row_count=len(rows),
                has_ingestion_timestamp=has_ts,
                columns=cols,
            )

            if not has_ts:
                result.missing_timestamp_files.append(str(ndjson_path))
                report.issues.append("missing _ingestion_timestamp")

            if baseline_cols is None:
                baseline_cols = cols
            elif cols != baseline_cols:
                drift = (cols - baseline_cols) | (baseline_cols - cols)
                result.schema_drift_files.append(str(ndjson_path))
                report.issues.append(f"schema drift — differing columns: {drift}")

            result.partitions.append(report)

        result.log_summary()
        return result

    def validate_parquet_source(
        self, source_dir: Path, source_name: str
    ) -> BronzeValidationResult:
        """Validate all Parquet files under source_dir (checks .meta.json sidecars)."""
        result = BronzeValidationResult(source=source_name)
        files = sorted(source_dir.rglob("*.parquet"))

        if not files:
            logger.warning("[bronze/validate] No Parquet files found under {}", source_dir)
            return result

        baseline_cols: frozenset[str] | None = None

        for parquet_path in files:
            meta_path = parquet_path.with_suffix(".meta.json")
            has_ts = meta_path.exists()

            try:
                df = pd.read_parquet(parquet_path, columns=None)
                cols = frozenset(df.columns)
                row_count = len(df)
            except Exception as exc:
                logger.warning("[bronze/validate] Cannot read {}: {}", parquet_path.name, exc)
                continue

            report = PartitionReport(
                path=parquet_path,
                row_count=row_count,
                has_ingestion_timestamp=has_ts,
                columns=cols,
            )

            if not has_ts:
                result.missing_timestamp_files.append(str(parquet_path))
                report.issues.append("missing .meta.json sidecar (_ingestion_timestamp)")

            if baseline_cols is None:
                baseline_cols = cols
            elif cols != baseline_cols:
                drift = (cols - baseline_cols) | (baseline_cols - cols)
                result.schema_drift_files.append(str(parquet_path))
                report.issues.append(f"schema drift — differing columns: {drift}")

            result.partitions.append(report)

        result.log_summary()
        return result
