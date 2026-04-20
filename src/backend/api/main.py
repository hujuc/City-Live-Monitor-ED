"""
FastAPI REST Server

Serves analytics-ready data from the Gold layer for dashboards and external consumers.
"""

from datetime import datetime
from typing import Any, cast

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from backend.storage.gold import GoldStorage

app = FastAPI(
    title="City Live Monitor API",
    description="NYC Nightlife Pulse — batch and streaming analytics",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_gold = GoldStorage()


def _serialize_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to JSON-safe list of records (handles NaN, Timestamp)."""
    records = df.replace({pd.NA: None}).where(df.notna(), None).to_dict(orient="records")
    return cast(list[dict[str, Any]], records)


def _filter_rows(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """Typed wrapper for pandas boolean filtering to keep static checkers precise."""
    return cast(pd.DataFrame, df[mask])


def _load_gold_or_404(table: str) -> pd.DataFrame:
    try:
        return _gold.read(table)
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail=f"Gold table '{table}' not available — run `uv run clm gold-build-all`",
        ) from None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/gold/taxi-demand")
def taxi_demand(
    borough: str | None = Query(None, description="Filter by borough (Manhattan, Brooklyn, ...)"),
    zone_id: int | None = Query(None, description="Filter by PULocationID"),
    hour_of_day: int | None = Query(None, ge=0, le=23),
    day_of_week: int | None = Query(None, ge=0, le=6, description="0=Monday, 6=Sunday"),
    is_weekend: bool | None = Query(None),
    limit: int = Query(1000, ge=1, le=50000),
) -> dict[str, Any]:
    """Taxi demand aggregates by zone × hour × day-of-week."""
    df = _load_gold_or_404("taxi_demand_by_zone")

    if borough is not None:
        df = _filter_rows(df, df["borough"] == borough)
    if zone_id is not None:
        df = _filter_rows(df, df["PULocationID"] == zone_id)
    if hour_of_day is not None:
        df = _filter_rows(df, df["hour_of_day"] == hour_of_day)
    if day_of_week is not None:
        df = _filter_rows(df, df["day_of_week"] == day_of_week)
    if is_weekend is not None:
        df = _filter_rows(df, df["is_weekend"] == is_weekend)

    total_rows = len(df)
    df = cast(pd.DataFrame, df.head(limit))
    return {"count": total_rows, "returned": len(df), "rows": _serialize_df(df)}


@app.get("/gold/events")
def events(
    borough: str | None = Query(None),
    is_nightlife_event: bool | None = Query(None),
    is_weekend_event: bool | None = Query(None),
    start_after: datetime | None = None,
    start_before: datetime | None = None,
    limit: int = Query(1000, ge=1, le=50000),
) -> dict[str, Any]:
    """Enriched permitted events with nightlife / weekend flags."""
    df = _load_gold_or_404("event_schedule")

    if borough is not None:
        df = _filter_rows(df, df["borough"] == borough)
    if is_nightlife_event is not None:
        df = _filter_rows(df, df["is_nightlife_event"] == is_nightlife_event)
    if is_weekend_event is not None:
        df = _filter_rows(df, df["is_weekend_event"] == is_weekend_event)
    if start_after is not None:
        df = _filter_rows(df, df["start_datetime_utc"] >= pd.Timestamp(start_after, tz="UTC"))
    if start_before is not None:
        df = _filter_rows(df, df["start_datetime_utc"] <= pd.Timestamp(start_before, tz="UTC"))

    total_rows = len(df)
    df = cast(pd.DataFrame, df.head(limit))
    return {"count": total_rows, "returned": len(df), "rows": _serialize_df(df)}


@app.get("/gold/noise")
def noise(
    borough: str | None = Query(None),
    complaint_type: str | None = Query(None),
    descriptor: str | None = Query(None),
    hour_of_day: int | None = Query(None, ge=0, le=23),
    day_of_week: int | None = Query(None, ge=0, le=6),
    is_weekend: bool | None = Query(None),
    limit: int = Query(1000, ge=1, le=50000),
) -> dict[str, Any]:
    """311 noise complaint aggregates by borough × type × hour × day-of-week."""
    df = _load_gold_or_404("noise_by_borough_hour")

    if borough is not None:
        df = _filter_rows(df, df["borough"] == borough)
    if complaint_type is not None:
        df = _filter_rows(df, df["complaint_type"] == complaint_type)
    if descriptor is not None:
        df = _filter_rows(df, df["descriptor"] == descriptor)
    if hour_of_day is not None:
        df = _filter_rows(df, df["hour_of_day"] == hour_of_day)
    if day_of_week is not None:
        df = _filter_rows(df, df["day_of_week"] == day_of_week)
    if is_weekend is not None:
        df = _filter_rows(df, df["is_weekend"] == is_weekend)

    total_rows = len(df)
    df = cast(pd.DataFrame, df.head(limit))
    return {"count": total_rows, "returned": len(df), "rows": _serialize_df(df)}


@app.get("/gold/summary")
def summary() -> dict[str, Any]:
    """High-level counts across all Gold tables — useful for dashboard headers."""
    result: dict[str, Any] = {}

    for table, label in [
        ("taxi_demand_by_zone", "taxi_demand"),
        ("event_schedule", "events"),
        ("noise_by_borough_hour", "noise"),
    ]:
        if not _gold.exists(table):
            result[label] = {"available": False}
            continue
        df = _gold.read(table)
        entry: dict[str, Any] = {"available": True, "row_count": len(df)}
        if label == "taxi_demand":
            entry["total_trips"] = int(cast(float, df["trip_count"].sum()))
            entry["boroughs"] = sorted(df["borough"].dropna().unique().tolist())
        elif label == "noise":
            entry["total_complaints"] = int(cast(float, df["complaint_count"].sum()))
            entry["boroughs"] = sorted(df["borough"].dropna().unique().tolist())
        elif label == "events":
            entry["nightlife_events"] = int(cast(float, df["is_nightlife_event"].sum()))
            entry["boroughs"] = sorted(df["borough"].dropna().unique().tolist())
        result[label] = entry

    return result
