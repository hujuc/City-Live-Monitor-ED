"""Tests for src/backend/utils/validation.py"""

from pathlib import Path
import pandas as pd
import pytest
from backend.utils.validation import validate_parquet


def test_valid_parquet(sample_parquet: Path) -> None:
    assert validate_parquet(sample_parquet) is True


def test_missing_file(tmp_path: Path) -> None:
    assert validate_parquet(tmp_path / "nonexistent.parquet") is False


def test_wrong_magic_bytes(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    path.write_bytes(b"NOTPAR1" + b"\x00" * 200_000)
    assert validate_parquet(path) is False


def test_file_too_small(tmp_path: Path) -> None:
    path = tmp_path / "tiny.parquet"
    # Real Parquet but below the 100 KB size floor
    df = pd.DataFrame({"x": [1]})
    df.to_parquet(path, index=False)
    assert validate_parquet(path) is False
