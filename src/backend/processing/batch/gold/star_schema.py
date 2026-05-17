"""Gold layer: formal star schema (dim_zone, dim_time, fact_taxi_demand).

Produces three Parquet tables from the existing flat Gold aggregates and the
TLC zone lookup reference file:

  dim_zone      — one row per taxi zone (PULocationID, borough, zone_name, service_zone)
  dim_time      — one row per (hour_of_day, day_of_week) combination (168 rows)
  fact_taxi_demand — measures keyed by (zone_sk, time_sk)

These tables complement the existing flat `taxi_demand_by_zone` table and
allow Superset / BI tools to do proper dimensional queries with JOIN semantics.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from backend.storage.gold import GoldStorage
from config.settings import Config

_ZONE_LOOKUP_PATH = Config.PROJECT_ROOT / "data" / "reference" / "taxi_zone_lookup.csv"

_DAY_NAMES = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
              4: "Friday", 5: "Saturday", 6: "Sunday"}
_PERIOD_LABELS = {
    (7, False): "Morning Rush",
    (8, False): "Morning Rush",
    (9, False): "Morning Rush",
    (16, False): "Evening Rush",
    (17, False): "Evening Rush",
    (18, False): "Evening Rush",
    (19, False): "Evening Rush",
}


class GoldStarSchemaProcessor:
    """Builds dim_zone, dim_time, and fact_taxi_demand from existing Gold data."""

    def __init__(
        self,
        gold_storage: GoldStorage | None = None,
        zone_lookup_path: Path | None = None,
    ) -> None:
        self._gold = gold_storage or GoldStorage()
        self._zone_lookup_path = zone_lookup_path or _ZONE_LOOKUP_PATH

    def build(self) -> dict[str, int]:
        """Build all three star-schema tables. Returns row counts per table."""
        dim_zone = self._build_dim_zone()
        dim_time = self._build_dim_time()
        fact = self._build_fact(dim_zone, dim_time)

        self._gold.write(dim_zone, "dim_zone")
        self._gold.write(dim_time, "dim_time")
        self._gold.write(fact, "fact_taxi_demand")

        counts = {
            "dim_zone": len(dim_zone),
            "dim_time": len(dim_time),
            "fact_taxi_demand": len(fact),
        }
        logger.info(
            "[gold/star_schema] built: dim_zone={} dim_time={} fact_taxi_demand={}",
            counts["dim_zone"],
            counts["dim_time"],
            counts["fact_taxi_demand"],
        )
        return counts

    def _build_dim_zone(self) -> pd.DataFrame:
        """One row per taxi zone from the reference lookup."""
        if not self._zone_lookup_path.exists():
            logger.warning("[gold/star_schema] Zone lookup not found — dim_zone will be empty")
            return pd.DataFrame(columns=["zone_sk", "PULocationID", "borough", "zone_name", "service_zone"])

        df = pd.read_csv(self._zone_lookup_path)
        df = df.rename(columns={
            "LocationID": "PULocationID",
            "Borough": "borough",
            "Zone": "zone_name",
            "service_zone": "service_zone",
        })
        df["zone_sk"] = df["PULocationID"].astype(int)
        return df[["zone_sk", "PULocationID", "borough", "zone_name", "service_zone"]].sort_values("zone_sk").reset_index(drop=True)

    def _build_dim_time(self) -> pd.DataFrame:
        """One row per (hour_of_day, day_of_week) — 24 × 7 = 168 rows."""
        rows = []
        for dow in range(7):
            is_weekend = dow >= 5
            day_name = _DAY_NAMES[dow]
            for hour in range(24):
                is_rush = hour in (7, 8, 9, 16, 17, 18, 19) and not is_weekend
                period = _PERIOD_LABELS.get((hour, is_weekend), "Night" if hour < 6 or hour >= 22 else "Daytime")
                rows.append({
                    "time_sk": dow * 24 + hour,
                    "hour_of_day": hour,
                    "day_of_week": dow,
                    "day_name": day_name,
                    "is_weekend": is_weekend,
                    "is_rush_hour": is_rush,
                    "period_label": period,
                })
        return pd.DataFrame(rows)

    def _build_fact(self, dim_zone: pd.DataFrame, dim_time: pd.DataFrame) -> pd.DataFrame:
        """Fact table: measures from taxi_demand_by_zone, keyed to dimension SKs."""
        if not self._gold.exists("taxi_demand_by_zone"):
            logger.warning("[gold/star_schema] taxi_demand_by_zone not found — fact table will be empty")
            return pd.DataFrame()

        agg = self._gold.read("taxi_demand_by_zone")

        # Join zone_sk
        if not dim_zone.empty:
            agg = agg.merge(
                dim_zone[["PULocationID", "zone_sk"]],
                on="PULocationID",
                how="left",
            )
        else:
            agg["zone_sk"] = agg["PULocationID"]

        # Join time_sk
        time_keys = dim_time[["hour_of_day", "day_of_week", "time_sk"]]
        agg = agg.merge(time_keys, on=["hour_of_day", "day_of_week"], how="left")

        fact_cols = [
            "zone_sk",
            "time_sk",
            "PULocationID",
            "hour_of_day",
            "day_of_week",
            "trip_count",
            "avg_fare_amount",
            "avg_trip_duration_minutes",
            "avg_tip_percentage",
            "total_trip_distance",
            "pct_rush_hour",
        ]
        available = [c for c in fact_cols if c in agg.columns]
        return agg[available].sort_values(["zone_sk", "time_sk"]).reset_index(drop=True)
