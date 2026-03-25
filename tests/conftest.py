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
    import numpy as np
    path = tmp_path / "sample.parquet"
    # Use random data so compression doesn't shrink the file below the 100 KB floor
    rng = np.random.default_rng(42)
    df = pd.DataFrame({"id": range(10_000), "value": rng.random(10_000)})
    df.to_parquet(path, index=False)
    return path
