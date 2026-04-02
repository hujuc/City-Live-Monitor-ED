"""Common helpers for streaming Socrata pollers."""

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from loguru import logger


@dataclass
class CircuitBreaker:
    """Simple consecutive-failure circuit breaker."""

    max_failures: int
    cooldown_seconds: int
    _failures: int = 0
    _opened_at: float | None = None

    def before_call(self) -> bool:
        """Return True if call is allowed, False when breaker is open."""
        if self._opened_at is None:
            return True

        elapsed = time.time() - self._opened_at
        if elapsed >= self.cooldown_seconds:
            logger.info("[streaming] Circuit breaker cooldown elapsed; closing breaker")
            self._opened_at = None
            self._failures = 0
            return True

        return False

    def on_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def on_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.max_failures:
            self._opened_at = time.time()
            logger.error("[streaming] Circuit breaker opened after {} failures", self._failures)


def fetch_socrata_rows(
    endpoint: str,
    params: dict[str, Any],
    app_token: str | None,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    """Fetch one page of rows from a Socrata endpoint."""
    headers = {"X-App-Token": app_token} if app_token else {}
    response = requests.get(endpoint, params=params, headers=headers, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Socrata response is not a list")
    return payload


def _build_page_params(
    timestamp_field: str,
    static_where: str,
    current_cursor: str | None,
    first_page: bool,
    first_page_inclusive: bool,
    same_ts_offset: int,
    limit: int,
) -> dict[str, Any]:
    """Build Socrata query params for one pagination step."""
    where_clauses = [static_where] if static_where else []
    if current_cursor:
        use_gte = (first_page and first_page_inclusive) or same_ts_offset > 0
        comparator = ">=" if use_gte else ">"
        where_clauses.append(f"{timestamp_field} {comparator} '{current_cursor}'")

    params: dict[str, Any] = {"$order": f"{timestamp_field} ASC", "$limit": limit}
    if same_ts_offset > 0:
        params["$offset"] = same_ts_offset
    if where_clauses:
        params["$where"] = " AND ".join(where_clauses)
    return params


def fetch_socrata_rows_paginated(
    endpoint: str,
    timestamp_field: str,
    static_where: str,
    app_token: str | None,
    timeout_seconds: int,
    limit: int,
    cursor: str | None = None,
    first_page_inclusive: bool = False,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch all pages newer than cursor using timestamp keyset pagination.

    When a full page ends on the same timestamp as the cursor (duplicate
    timestamps), switches to >= with $offset to avoid skipping rows that
    share that timestamp across a page boundary.
    """
    all_rows: list[dict[str, Any]] = []
    current_cursor = cursor
    first_page = True
    max_timestamp: str | None = None
    page_number = 0

    logger.info(
        "[streaming] Starting paginated fetch endpoint={} field={} limit={} cursor={}",
        endpoint,
        timestamp_field,
        limit,
        cursor,
    )

    while True:
        params = _build_page_params(
            timestamp_field,
            static_where,
            current_cursor,
            first_page,
            first_page_inclusive,
            same_ts_offset,
            limit,
        )
        page = fetch_socrata_rows(
            endpoint=endpoint,
            params=params,
            app_token=app_token,
            timeout_seconds=timeout_seconds,
        )
        page_number += 1

        logger.info(
            "[streaming] Page {} fetched {} row(s) (cumulative={})",
            page_number,
            len(page),
            len(all_rows) + len(page),
        )

        if not page:
            break

        all_rows.extend(page)
        last_ts = page[-1].get(timestamp_field)
        if not last_ts:
            logger.warning(
                "[streaming] Missing '{}' in page tail row; stopping pagination",
                timestamp_field,
            )
            break

        last_ts_str = str(last_ts)
        max_timestamp = last_ts_str
        current_cursor = last_ts_str
        first_page = False

        if len(page) < limit:
            break

    logger.info(
        "[streaming] Paginated fetch completed pages={} rows={} max_timestamp={}",
        page_number,
        len(all_rows),
        max_timestamp,
    )

    return all_rows, max_timestamp


def write_ndjson_snapshot(root: Path, source: str, rows: list[dict[str, Any]]) -> Path:
    """Persist raw API rows in Bronze as immutable NDJSON snapshots."""
    now = datetime.now(tz=UTC)
    filename = f"snapshot_{now.strftime('%Y%m%dT%H%M%SZ')}.ndjson"
    destination = root / source / str(now.year) / f"{now.month:02d}" / filename
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")

    return destination


def make_event_id(source: str, row: dict[str, Any], preferred_fields: list[str]) -> str:
    """Build a stable deterministic id from selected fields and source fallback."""
    values = [str(row.get(field, "")) for field in preferred_fields]
    fingerprint = "|".join([source, *values])
    if fingerprint.replace("|", "").strip() == source:
        fingerprint = f"{source}|{json.dumps(row, sort_keys=True, ensure_ascii=True)}"
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return digest[:32]


def parse_iso8601_or_now(raw_value: str | None) -> datetime:
    """Parse datetime strings and fallback to current UTC when missing/invalid."""
    if raw_value:
        try:
            parsed = datetime.fromisoformat(raw_value)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            logger.warning("[streaming] Invalid timestamp: {}", raw_value)
    return datetime.now(tz=UTC)
