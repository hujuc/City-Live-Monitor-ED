"""Tests for FastAPI endpoints."""

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.api import main as api_module
from backend.storage.gold import GoldStorage


@pytest.fixture
def client_with_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Fixture that wires the API to an isolated Gold directory populated with test data."""
    gold = GoldStorage(root=tmp_path / "gold")

    taxi_df = pd.DataFrame(
        {
            "PULocationID": [161, 161, 162],
            "borough": ["Manhattan", "Manhattan", "Queens"],
            "zone_name": ["Midtown Center", "Midtown Center", "LaGuardia Airport"],
            "hour_of_day": [20, 8, 20],
            "day_of_week": [5, 0, 5],
            "is_weekend": [True, False, True],
            "trip_count": [100, 50, 30],
            "avg_fare_amount": [15.0, 12.0, 40.0],
            "avg_trip_duration_minutes": [18.0, 10.0, 35.0],
            "avg_tip_percentage": [20.0, 15.0, 18.0],
            "total_trip_distance": [300.0, 120.0, 450.0],
            "pct_rush_hour": [0.0, 1.0, 0.0],
        }
    )
    gold.write(taxi_df, "taxi_demand_by_zone")

    events_df = pd.DataFrame(
        {
            "event_id": ["a", "b"],
            "event_name": ["Jazz Night", "Marathon"],
            "event_type": ["Concert", "Race"],
            "borough": ["Manhattan", "Brooklyn"],
            "event_location": ["Central Park", "Bridge"],
            "start_datetime_utc": pd.to_datetime(
                ["2025-06-15T21:00:00Z", "2025-07-04T08:00:00Z"], utc=True
            ),
            "end_datetime_utc": pd.to_datetime(
                ["2025-06-15T23:30:00Z", "2025-07-04T14:00:00Z"], utc=True
            ),
            "event_duration_hours": [2.5, 6.0],
            "start_hour_of_day": [21, 8],
            "start_day_of_week": [6, 4],
            "is_weekend_event": [True, False],
            "is_nightlife_event": [True, False],
        }
    )
    gold.write(events_df, "event_schedule")

    noise_df = pd.DataFrame(
        {
            "borough": ["Manhattan", "Brooklyn"],
            "complaint_type": ["Noise - Residential", "Noise - Street/Sidewalk"],
            "descriptor": ["Loud Music/Party", "Loud Talking"],
            "hour_of_day": [23, 22],
            "day_of_week": [5, 6],
            "is_weekend": [True, True],
            "complaint_count": [500, 300],
            "avg_resolution_hours": [2.0, 1.5],
        }
    )
    gold.write(noise_df, "noise_by_borough_hour")

    monkeypatch.setattr(api_module, "_gold", gold)
    return TestClient(api_module.app)


@pytest.fixture
def client_without_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Client pointed at an empty Gold directory — endpoints should return 503."""
    gold = GoldStorage(root=tmp_path / "gold")
    monkeypatch.setattr(api_module, "_gold", gold)
    return TestClient(api_module.app)


def test_health(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_taxi_demand_returns_all_rows(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/taxi-demand")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 3
    assert body["returned"] == 3


def test_taxi_demand_filter_by_borough(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/taxi-demand?borough=Manhattan")
    body = response.json()
    assert body["count"] == 2
    assert all(r["borough"] == "Manhattan" for r in body["rows"])


def test_taxi_demand_filter_by_hour(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/taxi-demand?hour_of_day=20")
    body = response.json()
    assert body["count"] == 2


def test_taxi_demand_filter_by_is_weekend(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/taxi-demand?is_weekend=true")
    body = response.json()
    assert body["count"] == 2
    assert all(r["is_weekend"] for r in body["rows"])


def test_taxi_demand_limit(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/taxi-demand?limit=1")
    body = response.json()
    assert body["count"] == 3
    assert body["returned"] == 1


def test_taxi_demand_invalid_hour_returns_422(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/taxi-demand?hour_of_day=24")
    assert response.status_code == 422


def test_events_returns_all_rows(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/events")
    body = response.json()
    assert body["count"] == 2


def test_events_filter_nightlife(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/events?is_nightlife_event=true")
    body = response.json()
    assert body["count"] == 1
    assert body["rows"][0]["event_name"] == "Jazz Night"


def test_events_filter_by_start_after(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/events?start_after=2025-07-01T00:00:00")
    body = response.json()
    assert body["count"] == 1
    assert body["rows"][0]["event_name"] == "Marathon"


def test_noise_returns_all_rows(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/noise")
    body = response.json()
    assert body["count"] == 2


def test_noise_filter_by_complaint_type(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/noise?complaint_type=Noise - Residential")
    body = response.json()
    assert body["count"] == 1
    assert body["rows"][0]["borough"] == "Manhattan"


def test_summary_reports_counts(client_with_gold: TestClient) -> None:
    response = client_with_gold.get("/gold/summary")
    body = response.json()
    assert body["taxi_demand"]["row_count"] == 3
    assert body["taxi_demand"]["total_trips"] == 180
    assert body["events"]["row_count"] == 2
    assert body["events"]["nightlife_events"] == 1
    assert body["noise"]["total_complaints"] == 800


def test_missing_gold_returns_503(client_without_gold: TestClient) -> None:
    response = client_without_gold.get("/gold/taxi-demand")
    assert response.status_code == 503
    assert "gold-build-all" in response.json()["detail"]


def test_summary_reports_unavailable_when_empty(client_without_gold: TestClient) -> None:
    response = client_without_gold.get("/gold/summary")
    body = response.json()
    assert body["taxi_demand"]["available"] is False
    assert body["events"]["available"] is False
    assert body["noise"]["available"] is False
