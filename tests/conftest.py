"""
pytest configuration and shared fixtures.
"""

import pytest
from pathlib import Path


@pytest.fixture
def tmp_bronze(tmp_path: Path) -> Path:
    """Temporary Bronze layer root for tests."""
    d = tmp_path / "bronze"
    d.mkdir()
    return d


@pytest.fixture
def tmp_silver(tmp_path: Path) -> Path:
    """Temporary Silver layer root for tests."""
    d = tmp_path / "silver"
    d.mkdir()
    return d


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """Write a minimal valid Parquet file for use in validation tests."""
    import pandas as pd
    path = tmp_path / "sample.parquet"
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    df.to_parquet(path, index=False)
    return path
