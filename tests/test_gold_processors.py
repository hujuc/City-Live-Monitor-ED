"""Tests for Gold layer processors."""

from pathlib import Path

import pandas as pd

from backend.processing.batch.gold.event_schedule import GoldEventScheduleProcessor
from backend.processing.batch.gold.noise_aggregation import GoldNoiseAggregationProcessor
from backend.processing.batch.gold.taxi_demand import GoldTaxiDemandProcessor
from backend.storage.gold import GoldStorage
from backend.storage.silver import SilverStorage

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_tlc_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "PULocationID": [161, 161, 162, 162],
            "hour_of_day": [20, 20, 8, 8],
            "day_of_week": [4, 5, 0, 1],
            "fare_amount": [15.0, 20.0, 10.0, 12.0],
            "trip_distance": [2.0, 3.0, 1.5, 2.5],
            "tip_amount": [3.0, 4.0, 2.0, 2.5],
            "tip_percentage": [20.0, 20.0, 20.0, 20.8],
            "cost_per_mile": [7.5, 6.7, 6.7, 4.8],
            "trip_duration_minutes": [15.0, 20.0, 10.0, 12.0],
            "trip_duration_capped_minutes": [15.0, 20.0, 10.0, 12.0],
            "is_rush_hour": [False, False, True, True],
            "is_weekend": [False, True, False, False],
            "is_speed_outlier": [False, False, False, False],
            "is_tip_outlier": [False, False, False, False],
            "is_duration_outlier_iqr": [False, False, False, False],
        }
    )


def _make_events_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": ["aaa", "bbb", "ccc"],
            "event_name": ["Jazz Night", "Marathon", "Parade"],
            "event_type": ["Concert", "Race", "March"],
            "borough": ["Manhattan", "Brooklyn", "Queens"],
            "event_location": ["Central Park", "Brooklyn Bridge", "5th Ave"],
            "start_datetime_utc": pd.to_datetime(
                ["2025-06-15 21:00:00", "2025-07-04 08:00:00", "2025-11-11 10:00:00"], utc=True
            ),
            "end_datetime_utc": pd.to_datetime(
                ["2025-06-15 23:30:00", "2025-07-04 14:00:00", "2025-11-11 13:00:00"], utc=True
            ),
            "event_duration_hours": [2.5, 6.0, 3.0],
        }
    )


def _make_noise_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "unique_key": ["1", "2", "3", "4"],
            "borough": ["Manhattan", "Manhattan", "Brooklyn", "Brooklyn"],
            "complaint_type": ["Noise - Residential"] * 4,
            "descriptor": ["Loud Music/Party"] * 4,
            "created_datetime_utc": pd.to_datetime(
                [
                    "2025-06-01 23:00:00",
                    "2025-06-02 23:30:00",
                    "2025-06-01 22:00:00",
                    "2025-06-03 23:00:00",
                ],
                utc=True,
            ),
            "resolution_duration_hours": [1.0, 2.0, 0.5, 1.5],
        }
    )


def _silver_with_tlc(tmp_path: Path, df: pd.DataFrame) -> SilverStorage:
    silver = SilverStorage(root=tmp_path / "silver")
    dest = silver.destination("tlc", 2025, 6, "trips_silver.parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)
    return silver


def _silver_with_events(tmp_path: Path, df: pd.DataFrame) -> SilverStorage:
    silver = SilverStorage(root=tmp_path / "silver")
    dest = silver.destination("events", 2025, 6, "events_silver.parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)
    return silver


def _silver_with_noise(tmp_path: Path, df: pd.DataFrame) -> SilverStorage:
    silver = SilverStorage(root=tmp_path / "silver")
    dest = silver.destination("noise311", 2025, 6, "noise_silver.parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)
    return silver


# ── GoldTaxiDemandProcessor ───────────────────────────────────────────────────


def test_taxi_demand_writes_gold_table(tmp_path: Path) -> None:
    silver = _silver_with_tlc(tmp_path, _make_tlc_df())
    gold = GoldStorage(root=tmp_path / "gold")

    proc = GoldTaxiDemandProcessor(
        silver_storage=silver, gold_storage=gold, zone_lookup_path=Path("nonexistent.csv")
    )
    rows = proc.build()

    assert rows > 0
    assert gold.exists("taxi_demand_by_zone")
    result = gold.read("taxi_demand_by_zone")
    assert "trip_count" in result.columns
    assert "avg_fare_amount" in result.columns
    assert "pct_rush_hour" in result.columns


def test_taxi_demand_groupby_key(tmp_path: Path) -> None:
    silver = _silver_with_tlc(tmp_path, _make_tlc_df())
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldTaxiDemandProcessor(
        silver_storage=silver, gold_storage=gold, zone_lookup_path=Path("nonexistent.csv")
    )
    proc.build()

    result = gold.read("taxi_demand_by_zone")
    # 2 zones × 1 hour each → 2 unique (zone, hour, day) combos per zone
    assert len(result) == 4  # (161,20,4), (161,20,5), (162,8,0), (162,8,1)


def test_taxi_demand_excludes_outliers(tmp_path: Path) -> None:
    df = _make_tlc_df()
    df.loc[0, "is_speed_outlier"] = True  # row 0 should be excluded
    silver = _silver_with_tlc(tmp_path, df)
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldTaxiDemandProcessor(
        silver_storage=silver, gold_storage=gold, zone_lookup_path=Path("nonexistent.csv")
    )
    proc.build()

    result = gold.read("taxi_demand_by_zone")
    # zone 161 day 4 should have trip_count=0 excluded → only 1 row for zone 161 day 4 is gone
    zone_161_fri = result[(result["PULocationID"] == 161) & (result["day_of_week"] == 4)]
    assert len(zone_161_fri) == 0  # the only trip for that combo was an outlier


def test_taxi_demand_returns_zero_on_empty_silver(tmp_path: Path) -> None:
    silver = SilverStorage(root=tmp_path / "silver")
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldTaxiDemandProcessor(silver_storage=silver, gold_storage=gold)
    assert proc.build() == 0


def test_taxi_demand_with_zone_lookup(tmp_path: Path) -> None:
    silver = _silver_with_tlc(tmp_path, _make_tlc_df())
    gold = GoldStorage(root=tmp_path / "gold")
    lookup_path = Path(__file__).parents[1] / "data" / "reference" / "taxi_zone_lookup.csv"
    proc = GoldTaxiDemandProcessor(
        silver_storage=silver, gold_storage=gold, zone_lookup_path=lookup_path
    )
    proc.build()

    result = gold.read("taxi_demand_by_zone")
    zone_161 = result[result["PULocationID"] == 161].iloc[0]
    assert zone_161["borough"] == "Manhattan"
    assert zone_161["zone_name"] == "Midtown Center"


# ── GoldEventScheduleProcessor ────────────────────────────────────────────────


def test_event_schedule_writes_gold_table(tmp_path: Path) -> None:
    silver = _silver_with_events(tmp_path, _make_events_df())
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldEventScheduleProcessor(silver_storage=silver, gold_storage=gold)
    rows = proc.build()

    assert rows == 3
    assert gold.exists("event_schedule")
    result = gold.read("event_schedule")
    assert "start_hour_of_day" in result.columns
    assert "is_nightlife_event" in result.columns
    assert "is_weekend_event" in result.columns


def test_event_schedule_nightlife_flag(tmp_path: Path) -> None:
    silver = _silver_with_events(tmp_path, _make_events_df())
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldEventScheduleProcessor(silver_storage=silver, gold_storage=gold)
    proc.build()

    result = gold.read("event_schedule")
    jazz = result[result["event_name"] == "Jazz Night"].iloc[0]
    marathon = result[result["event_name"] == "Marathon"].iloc[0]

    assert jazz["is_nightlife_event"]  # starts at 21:00
    assert not marathon["is_nightlife_event"]  # starts at 08:00, ends at 14:00


def test_event_schedule_deduplicates_across_partitions(tmp_path: Path) -> None:
    df = _make_events_df()
    silver = SilverStorage(root=tmp_path / "silver")
    # Write same events in two partitions
    for month in [6, 7]:
        dest = silver.destination("events", 2025, month, "events_silver.parquet")
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest, index=False)

    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldEventScheduleProcessor(silver_storage=silver, gold_storage=gold)
    rows = proc.build()

    assert rows == 3  # deduplicated by event_id


def test_event_schedule_returns_zero_on_empty_silver(tmp_path: Path) -> None:
    silver = SilverStorage(root=tmp_path / "silver")
    gold = GoldStorage(root=tmp_path / "gold")
    assert GoldEventScheduleProcessor(silver_storage=silver, gold_storage=gold).build() == 0


# ── GoldNoiseAggregationProcessor ─────────────────────────────────────────────


def test_noise_aggregation_writes_gold_table(tmp_path: Path) -> None:
    silver = _silver_with_noise(tmp_path, _make_noise_df())
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldNoiseAggregationProcessor(silver_storage=silver, gold_storage=gold)
    rows = proc.build()

    assert rows > 0
    assert gold.exists("noise_by_borough_hour")
    result = gold.read("noise_by_borough_hour")
    assert "complaint_count" in result.columns
    assert "hour_of_day" in result.columns
    assert "is_weekend" in result.columns


def test_noise_aggregation_groupby_borough_hour(tmp_path: Path) -> None:
    silver = _silver_with_noise(tmp_path, _make_noise_df())
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldNoiseAggregationProcessor(silver_storage=silver, gold_storage=gold)
    proc.build()

    result = gold.read("noise_by_borough_hour")
    manhattan = result[result["borough"] == "Manhattan"]
    # 2 Manhattan rows, both at hour 23 but different days → 2 groups
    assert len(manhattan) == 2
    assert manhattan["complaint_count"].sum() == 2


def test_noise_aggregation_derives_hour_from_timestamp(tmp_path: Path) -> None:
    silver = _silver_with_noise(tmp_path, _make_noise_df())
    gold = GoldStorage(root=tmp_path / "gold")
    proc = GoldNoiseAggregationProcessor(silver_storage=silver, gold_storage=gold)
    proc.build()

    result = gold.read("noise_by_borough_hour")
    assert set(result["hour_of_day"].unique()).issubset(set(range(24)))
    # All test rows are at hour 22 or 23
    assert result["hour_of_day"].min() >= 22


def test_noise_aggregation_returns_zero_on_empty_silver(tmp_path: Path) -> None:
    silver = SilverStorage(root=tmp_path / "silver")
    gold = GoldStorage(root=tmp_path / "gold")
    assert GoldNoiseAggregationProcessor(silver_storage=silver, gold_storage=gold).build() == 0
