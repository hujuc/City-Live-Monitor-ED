"""
NYC TLC Yellow Taxi — Batch Ingester

Downloads Parquet files from the TLC public dataset.
Features:
  - Incremental: skips already-downloaded files
  - Parallel: concurrent downloads via ThreadPoolExecutor
  - Retry: exponential backoff on transient failures
  - Validation: checks file size and Parquet magic bytes
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime as dt
from pathlib import Path

import requests
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Repo root is 4 levels up from this file:
# src/backend/ingestion/batch/download_tlc.py
_REPO_ROOT = Path(__file__).parents[4]

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BRONZE_TLC = _REPO_ROOT / "data" / "bronze" / "tlc"
CHUNK_SIZE = 1024 * 1024  # 1 MB
INITIAL_BACKOFF = 2.0
BACKOFF_FACTOR = 2.0
MAX_RETRIES = 5
MIN_FILE_BYTES = 100_000  # 100 KB sanity floor
MAX_WORKERS = 4


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class DownloadTask:
    taxi_type: str
    year: int
    month: int

    @property
    def filename(self) -> str:
        return f"{self.taxi_type}_tripdata_{self.year}-{self.month:02d}.parquet"

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.filename}"

    @property
    def dest_path(self) -> Path:
        return BRONZE_TLC / self.taxi_type / str(self.year) / self.filename


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _write_meta(parquet_path: Path) -> None:
    """Write a .meta.json sidecar with ingestion timestamp alongside a Parquet file."""
    meta = {
        "_ingestion_timestamp": dt.now(tz=UTC).isoformat(),
        "source_file": parquet_path.name,
        "size_bytes": parquet_path.stat().st_size,
    }
    meta_path = parquet_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _validate(path: Path) -> bool:
    if path.stat().st_size < MIN_FILE_BYTES:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _download(task: DownloadTask) -> tuple[DownloadTask, bool, str]:
    dest = task.dest_path
    tmp = dest.with_suffix(".tmp")

    if dest.exists() and _validate(dest):
        return task, True, "already exists, skipped"
    elif dest.exists():
        logger.warning("Validation failed on existing file, re-downloading: {}", dest.name)
        dest.unlink()

    dest.parent.mkdir(parents=True, exist_ok=True)

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("[{}/{}] Downloading {} ...", attempt, MAX_RETRIES, task.filename)
            with requests.get(task.url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)

            if not _validate(tmp):
                tmp.unlink(missing_ok=True)
                return task, False, "downloaded file failed validation"

            tmp.rename(dest)
            _write_meta(dest)
            return task, True, f"downloaded ({dest.stat().st_size / 1e6:.1f} MB)"

        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (403, 404):
                tmp.unlink(missing_ok=True)
                return task, False, f"{exc.response.status_code} — file not available"
            msg = f"HTTP error on attempt {attempt}: {exc}"
        except requests.RequestException as exc:
            msg = f"Network error on attempt {attempt}: {exc}"

        logger.warning("{} — retrying in {:.0f}s ...", msg, backoff)
        time.sleep(backoff)
        backoff = min(backoff * BACKOFF_FACTOR, 60.0)

    tmp.unlink(missing_ok=True)
    return task, False, f"failed after {MAX_RETRIES} attempts"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def build_tasks(taxi_types: list[str], year_months: list[tuple[int, int]]) -> list[DownloadTask]:
    return [DownloadTask(taxi_type=t, year=y, month=m) for t in taxi_types for y, m in year_months]


def run(tasks: list[DownloadTask], workers: int = MAX_WORKERS) -> None:
    total = len(tasks)
    logger.info("Downloading {} file(s) with {} workers ...", total, workers)

    ok, failed = [], []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_download, t): t for t in tasks}
        for i, future in enumerate(as_completed(futures), 1):
            task, success, msg = future.result()
            logger.info(
                "[{}/{}] [{}] {} — {}", i, total, "OK" if success else "FAIL", task.filename, msg
            )
            (ok if success else failed).append(task)

    logger.info("")
    logger.info("=== Summary: {} ok, {} failed ===", len(ok), len(failed))
    for t in failed:
        logger.warning("  FAILED: {}", t.filename)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NYC TLC Parquet files")
    parser.add_argument(
        "--taxi-types", nargs="+", default=["yellow"], choices=["yellow", "green", "fhv", "fhvhv"]
    )
    parser.add_argument("--years", nargs="+", type=int, default=[2023, 2024, 2025])
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    # Build year/month pairs — skip months we know don't exist yet
    import datetime

    cutoff = datetime.date(2025, 11, 1)  # last confirmed available month
    year_months = [
        (y, m) for y in args.years for m in range(1, 13) if datetime.date(y, m, 1) <= cutoff
    ]

    tasks = build_tasks(args.taxi_types, year_months)
    run(tasks, workers=args.workers)
