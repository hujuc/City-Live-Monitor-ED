"""Tests for src/backend/storage/bronze.py and silver.py"""

from pathlib import Path

import pandas as pd

from backend.storage.bronze import BronzeStorage
from backend.storage.silver import SilverStorage


class TestBronzeStorage:
    def test_destination_path(self, tmp_bronze: Path) -> None:
        b = BronzeStorage(root=tmp_bronze)
        dest = b.destination("tlc", 2024, "yellow_tripdata_2024-01.parquet")
        assert dest == tmp_bronze / "tlc" / "2024" / "yellow_tripdata_2024-01.parquet"

    def test_exists_false_when_missing(self, tmp_bronze: Path) -> None:
        b = BronzeStorage(root=tmp_bronze)
        assert b.exists("tlc", 2024, "missing.parquet") is False

    def test_exists_true_when_present(self, tmp_bronze: Path) -> None:
        b = BronzeStorage(root=tmp_bronze)
        dest = b.destination("tlc", 2024, "file.parquet")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"data")
        assert b.exists("tlc", 2024, "file.parquet") is True

    def test_list_files_returns_parquets(self, tmp_bronze: Path) -> None:
        b = BronzeStorage(root=tmp_bronze)
        for name in ("a.parquet", "b.parquet", "c.txt"):
            p = tmp_bronze / "tlc" / "2024" / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"x")
        files = b.list_files("tlc")
        assert len(files) == 2
        assert all(f.suffix == ".parquet" for f in files)

    def test_list_files_empty_source(self, tmp_bronze: Path) -> None:
        b = BronzeStorage(root=tmp_bronze)
        assert b.list_files("nonexistent") == []


class TestSilverStorage:
    def test_destination_path(self, tmp_silver: Path) -> None:
        s = SilverStorage(root=tmp_silver)
        dest = s.destination("tlc", 2024, 6, "trips.parquet")
        assert dest == tmp_silver / "tlc" / "year=2024" / "month=06" / "trips.parquet"

    def test_write_creates_parquet(self, tmp_silver: Path) -> None:
        s = SilverStorage(root=tmp_silver)
        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        path = s.write(df, "tlc", 2024, 1, "out.parquet")
        assert path.exists()
        result = pd.read_parquet(path)
        assert list(result.columns) == ["a", "b"]
        assert len(result) == 2

    def test_list_files_after_write(self, tmp_silver: Path) -> None:
        s = SilverStorage(root=tmp_silver)
        df = pd.DataFrame({"x": [1]})
        s.write(df, "tlc", 2023, 3, "march.parquet")
        s.write(df, "tlc", 2023, 4, "april.parquet")
        files = s.list_files("tlc")
        assert len(files) == 2
