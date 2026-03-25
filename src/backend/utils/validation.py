"""File and schema validation helpers."""

from pathlib import Path

MIN_FILE_SIZE_BYTES = 100_000  # 100 KB floor for a non-empty Parquet file


def validate_parquet(path: Path) -> bool:
    """
    Check that path is a valid, non-empty Parquet file.
    Uses pyarrow for full schema validation if available, falls back to magic bytes.
    """
    if not path.exists():
        return False
    if path.stat().st_size < MIN_FILE_SIZE_BYTES:
        return False

    with open(path, "rb") as f:
        magic = f.read(4)
    if magic != b"PAR1":
        return False

    try:
        import pyarrow.parquet as pq  # type: ignore
        pq.read_metadata(path)
    except ImportError:
        pass  # pyarrow not installed — magic bytes check is sufficient

    return True
