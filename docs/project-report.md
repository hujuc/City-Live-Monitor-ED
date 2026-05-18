# Project Report - City Live Monitor

**Course:** Engenharia de Dados (ED): University of Aveiro, 2026
**Authors:** Gabriel Silva · Hugo Castro
**Date:** May 2026

---

## Table of Contents

1. [Introduction and Motivation](#1-introduction-and-motivation)
2. [System Architecture](#2-system-architecture)
3. [Ingestion Layer](#3-ingestion-layer)
   - 3.1 Batch Ingestion: NYC TLC Yellow Taxi
   - 3.2 Streaming Ingestion: Socrata APIs via Kafka
4. [Processing Layer](#4-processing-layer)
   - 4.1 Batch Processing: TLC Bronze → Silver
   - 4.2 Stream Processing: Kafka Consumer and Windowed Aggregations
5. [Storage Layer: Medallion Architecture](#5-storage-layer--medallion-architecture)
   - 5.1 Bronze Layer
   - 5.2 Silver Layer
   - 5.3 Gold Layer and Star Schema
   - 5.4 Lakehouse with DuckDB and MinIO
6. [Serving Layer](#6-serving-layer)
   - 6.1 FastAPI REST API
   - 6.2 Apache Superset Dashboards
   - 6.3 Airflow Orchestration
7. [Results and Validation](#7-results-and-validation)
8. [Design Trade-offs and Limitations](#8-design-trade-offs-and-limitations)
9. [Conclusion](#9-conclusion)

---

## 1. Introduction and Motivation

Urban analytics platforms face a fundamental tension: historical batch data is voluminous, well-structured, and amenable to rich analytical processing, while real-time event streams are sparse, noisy, and require low-latency handling. City Live Monitor was built to resolve this tension by combining three years of NYC Yellow Taxi trip records with live data from two real-time city APIs, then unifying them into a queryable analytical layer.

The platform answers questions such as:

- **Where and when does taxi demand peak in NYC?**: using 103 million cleaned Silver trips aggregated by zone, hour, and day of the week.
- **What city events coincide with high taxi demand?**: by joining the Gold event schedule against taxi demand hot-spots.
- **How do 311 noise complaint patterns vary by borough and hour?**: using streamed complaint data aggregated into per-borough, per-type, per-hour cells.
- **Is the streaming pipeline healthy?**: via 5-minute windowed metrics tracking late events, duplicates, and out-of-order arrivals.

The choice of data sources was deliberate. TLC Yellow Taxi provides the richest structured dataset available for NYC: 35 monthly Parquet files spanning January 2023 to November 2025, each a faithful export of the taxi dispatch system with geospatial zone IDs, timestamps, fare breakdowns, and payment metadata. The streaming sources (Permitted Events and 311 Noise Complaints) are both available on the NYC Open Data Socrata platform and complement the taxi data: events create demand spikes, noise complaints reveal urban friction at the same hours taxis are most active.

---

## 2. System Architecture

```
┌──────────────── Sources ───────────────────────────────────────────────┐
│  NYC TLC Yellow Taxi                 NYC Open Data (Socrata)           │
│  35 monthly Parquet files            tvpp-9vvx.json  erm2-nwe9.json    │
│  Jan 2023 – Nov 2025                 Permitted Events  311 Noise       │
└──────┬──────────────────────────────────────────┬───────────────────────┘
       │ HTTP batch download                       │ HTTP poll (30s / 30min)
       │ (parallel, retry, validated)              │ (paginated, incremental)
       ▼                                           ▼
┌──── Bronze ────────────────────────────────────────────────────────────┐
│  data/bronze/tlc/yellow/YYYY/*.parquet  (.meta.json sidecars)          │
│  data/bronze/nyc_permitted_events/YYYY/MM/*.ndjson                     │
│  data/bronze/nyc_311_noise/YYYY/MM/*.ndjson                            │
│  (immutable, append-only, always has _ingestion_timestamp)             │
└──────┬──────────────────────────────────────────┬───────────────────────┘
       │ TLCBatchProcessor                         │ Socrata pollers →
       │ PermittedEventsSilver                     │ Kafka (events.raw,
       │ Noise311Silver                            │        noise311.raw)
       ▼                                           ▼
┌──── Silver ────────────────────────────────────────────────────────────┐
│  data/silver/tlc/yellow/year=YYYY/month=MM/*.parquet                   │
│  data/silver/events/*.parquet                                          │
│  data/silver/noise311/*.parquet                                        │
│  (cleaned, typed, enriched, deduplicated)                              │
└──────────────────────────┬─────────────────────────────────────────────┘
                           │ Gold processors + GoldStarSchemaProcessor
                           ▼
┌──── Gold ──────────────────────────────────────────────────────────────┐
│  taxi_demand_by_zone    event_schedule    noise_by_borough_hour        │
│  stream_metrics_5m      dim_zone          dim_time    fact_taxi_demand │
│  (mirrored to MinIO s3://gold-layer/ when MINIO_ENABLED=true)          │
└──────────────────────────┬─────────────────────────────────────────────┘
                           │ DuckDB read_parquet() views
                           ▼
┌──── Serving ───────────────────────────────────────────────────────────┐
│  Apache Superset  (4 dashboards)    FastAPI REST  :8000                │
│  Airflow DAG      (daily batch)     MinIO console :9001                │
└────────────────────────────────────────────────────────────────────────┘
```

The architecture follows the **medallion pattern**: three successive layers of data quality (Bronze = raw, Silver = clean, Gold = analytical). This pattern was chosen because it provides a clear audit trail (Bronze is immutable and always preserves the original data), separates concerns (cleaning lives in Silver processors, aggregation in Gold processors), and makes re-processing safe and idempotent.

---

## 3. Ingestion Layer

### 3.1 Batch Ingestion - NYC TLC Yellow Taxi

The TLC dataset is distributed as monthly Parquet files from a CloudFront CDN. The ingester (`src/backend/ingestion/batch/download_tlc.py`) implements four production-grade features that together make the ingestion process robust for the full 35-file dataset:

**Incremental download**: files that already exist on disk and pass validation are skipped. This avoids re-downloading 2 GB of data on restarts.

**Atomic writes via `.tmp` rename**: the file is written to `<name>.tmp` first, then renamed to its final path only after validation passes. This prevents partially-downloaded files from being treated as complete:

```python
with open(tmp, "wb") as f:
    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
        f.write(chunk)
if not _validate(tmp):
    tmp.unlink(missing_ok=True)
    return task, False, "downloaded file failed validation"
tmp.rename(dest)      # atomic on POSIX
_write_meta(dest)     # write .meta.json sidecar after rename
```

**Validation**: each file is checked for minimum size (100 KB) and the Parquet magic bytes (`PAR1`) at the start of the file:

```python
def _validate(path: Path) -> bool:
    if path.stat().st_size < MIN_FILE_BYTES:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"
```

**Parallel downloads with retry and exponential backoff**: up to 4 threads download concurrently; each thread retries up to 5 times with delays of 2s, 4s, 8s, 16s, 32s.

**Ingestion timestamp**: every Parquet file is accompanied by a `.meta.json` sidecar written at download time. Because Parquet is a binary format that cannot be modified without rewriting, the sidecar is the appropriate carrier for ingestion metadata:

```json
{
  "_ingestion_timestamp": "2026-04-20T18:21:12.264789+00:00",
  "source_file": "yellow_tripdata_2023-01.parquet",
  "size_bytes": 47673370
}
```

### 3.2 Streaming Ingestion - Socrata APIs via Kafka

Both streaming sources are polled from the NYC Open Data Socrata platform. The polling architecture is shared: both pollers inherit from `BaseIngester` and use the same `fetch_socrata_rows_paginated` helper from `src/backend/ingestion/streaming/common.py`.

**Polling intervals:**
- **311 Noise Complaints** (`erm2-nwe9.json`): every 30 seconds. Noise complaints are time-sensitive and can resolve quickly; frequent polling reduces latency.
- **Permitted Events** (`tvpp-9vvx.json`): every 30 minutes. Event permits are applied weeks in advance; sub-minute polling would waste API quota.

**Incremental polling with checkpoints**: rather than fetching the full dataset on every poll, each poller maintains a cursor (the latest `created_date` / `start_date_time` seen) in a local checkpoint file. The Socrata SoQL query uses this cursor as a `WHERE timestamp > cursor` filter:

```python
# data/checkpoints/noise311_created_date.txt contains the last seen ISO timestamp
checkpoint = self._read_checkpoint()   # e.g. "2026-05-17T19:00:00.000"

rows, max_created_date = fetch_socrata_rows_paginated(
    endpoint=Config.NOISE311_API_URL,
    timestamp_field="created_date",
    static_where="complaint_type like 'Noise%'",
    cursor=checkpoint or Config.NOISE311_BACKFILL_START,
    ...
)

if publish_errors == 0 and max_created_date:
    self._write_checkpoint(max_created_date)   # advance cursor only on success
```

The checkpoint is only advanced when all Kafka publishes succeed. This gives at-least-once delivery semantics: on a partial failure, the cursor stays at the last safe position and duplicate rows will be re-fetched and deduplicated at the Silver layer.

**Pagination**: Socrata responses are limited to 2,000 rows per request. For backfill scenarios (e.g., the first poll ever, which may have years of history), the helper fetches additional pages until no more rows are returned.

**Circuit breaker**: a consecutive-failure circuit breaker prevents the poller from hammering a temporarily unavailable API. After `max_failures` (default: 5) consecutive failures, the breaker opens for `cooldown_seconds` (default: 120s):

```python
@dataclass
class CircuitBreaker:
    max_failures: int
    cooldown_seconds: int
    _failures: int = 0
    _opened_at: float | None = None

    def before_call(self) -> bool:
        if self._opened_at is None:
            return True
        if time.time() - self._opened_at >= self.cooldown_seconds:
            self._opened_at = None   # auto-close after cooldown
            return True
        return False   # breaker open — skip this cycle
```

**Kafka publishing**: normalised events are published to topic `events.raw` or `noise311.raw`. Malformed events (missing required fields, invalid timestamps) are routed to dead-letter queues (`events.dlq`, `noise311.dlq`) rather than silently dropped, allowing inspection and replay:

```python
envelope = self._normalize(row)
if envelope is None:
    self._send_dlq(reason="malformed_event", raw=row)
    continue

self._producer.send(
    topic=Config.KAFKA_NOISE311_TOPIC,
    key=envelope["event_id"],    # partition key for ordering
    payload=envelope,
)
```

**Deterministic event IDs**: every event published to Kafka has a stable `event_id` derived from a SHA-256 hash of its key fields. This enables idempotent deduplication when the same event is re-ingested across polling cycles:

```python
def make_event_id(source: str, row: dict, preferred_fields: list[str]) -> str:
    parts = [source]
    for field in preferred_fields:
        if value := row.get(field):
            parts.append(str(value))
    fingerprint = "|".join(parts)
    return hashlib.sha256(fingerprint.encode()).hexdigest()[:32]
```

**Bronze persistence**: before publishing to Kafka, each poll writes a full NDJSON snapshot to the Bronze layer. This snapshot is independent of the Kafka pipeline and provides a permanent audit trail even if Kafka topics are purged:

```python
snapshot_path = write_ndjson_snapshot(bronze_root, self.source_name, rows)
# data/bronze/nyc_311_noise/2026/05/snapshot_20260517T200537Z.ndjson
```

Each NDJSON row includes `_ingestion_timestamp` injected at write time, satisfying the Bronze layer's ingestion metadata requirement.

---

## 4. Processing Layer

### 4.1 Batch Processing - TLC Bronze → Silver

The `TLCBatchProcessor` (`src/backend/processing/batch/tlc_processor.py`) transforms raw TLC Parquet files into cleaned, typed, feature-rich Silver partitions. The pipeline is structured in three sequential stages: type casting, quality filtering, and feature engineering.

**Type casting** coerces the raw Parquet types to the precise types needed for analysis. Critically, naive datetimes are converted to UTC-aware timestamps, which prevents silent timezone errors in downstream aggregations:

```python
data["pickup_datetime_utc"] = pd.to_datetime(
    data[pickup_col], errors="coerce", utc=True
)
data["dropoff_datetime_utc"] = pd.to_datetime(
    data[dropoff_col], errors="coerce", utc=True
)

for col in ["trip_distance", "fare_amount", "tip_amount", "total_amount", "passenger_count"]:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors="coerce")
```

**Quality filtering** applies eight rules in sequence, each tracked independently via `FilterRuleStat`. The per-rule tracking makes it possible to measure exactly what is being removed and why: essential for justifying the 16.6% drop rate:

| Filter rule | Condition |
|-------------|-----------|
| `invalid_datetime` | Pickup or dropoff datetime could not be parsed |
| `zero_or_negative_distance` | `trip_distance <= 0` |
| `negative_fare` | `fare_amount < 0` |
| `negative_tip` | `tip_amount < 0` |
| `negative_total` | `total_amount < 0` |
| `zero_or_missing_passengers` | `passenger_count < 1` |
| `non_positive_duration` | `dropoff_datetime <= pickup_datetime` |
| `invalid_pickup_coords` | Lat/lon outside `[-90,90]` / `[-180,180]` |

**Feature engineering** adds 14 derived columns to each surviving row. The most analytically significant are:

```python
# Temporal features
data["hour_of_day"]  = data["pickup_datetime_utc"].dt.hour          # 0–23
data["day_of_week"]  = data["pickup_datetime_utc"].dt.dayofweek     # 0=Monday
data["is_weekend"]   = data["day_of_week"] >= 5
data["is_rush_hour"] = data["hour_of_day"].isin([7, 8, 9, 16, 17, 18, 19])

# Economic features
data["tip_percentage"] = (data["tip_amount"] / data["fare_amount"].replace(0, float("nan"))) * 100
data["cost_per_mile"]  = data["total_amount"] / data["trip_distance"].replace(0, float("nan"))

# Outlier flags (not filters — preserved for downstream use)
data["is_tip_outlier"]      = data["tip_percentage"] > 50
data["is_speed_outlier"]    = data["trip_speed_mph"] > 80
data["is_unusual_midnight_trip"] = data["hour_of_day"].between(0, 4)

# Duration capping at 95th percentile (IQR-sensitive)
p95 = data["trip_duration_minutes"].quantile(0.95)
data["trip_duration_capped_minutes"] = data["trip_duration_minutes"].clip(upper=p95)
```

Outlier rows are **flagged, not filtered**. This is an intentional design decision: a tip that is 60% of the fare is unusual but not erroneous. Downstream analysts can choose to exclude outlier-flagged rows for their specific analysis without losing the raw record.

**Output partitioning**: Silver TLC files are written to Hive-style partitions (`year=YYYY/month=MM/`), enabling predicate push-down when DuckDB reads them:

```python
for (year, month), part in cleaned.groupby(["partition_year", "partition_month"]):
    out_name = f"{stem}_silver.parquet"
    self._silver.write(part, "tlc/yellow", year, month, out_name)
    # writes to data/silver/tlc/yellow/year=2023/month=01/yellow_tripdata_2023-01_silver.parquet
```

### 4.2 Stream Processing - Kafka Consumer and Windowed Aggregations

The `EventProcessor` (`src/backend/processing/streaming/event_processor.py`) is the core stateful stream-processing component. It is designed as a push-based processor: the Kafka consumer loop calls `process_event()` for each message; the processor maintains state in memory.

**Envelope parsing and validation**: each Kafka message is expected to have a well-defined envelope structure produced by the pollers (event_id, source, timestamp, payload). Messages that deviate from this structure are rejected before any state is updated:

```python
def process_event(self, raw: dict) -> EventEnvelope | None:
    envelope = self._parse_envelope(raw)
    if envelope is None:
        return None   # malformed: rejected, not counted in metrics

    if self._is_duplicate(envelope):
        self._record_metric(envelope, duplicate=True)
        return None   # duplicate: counted but not forwarded

    out_of_order = self._is_out_of_order(envelope)
    # ... late detection, metric recording, state update
    return envelope
```

**Deduplication within a sliding window**: the processor maintains a dictionary of seen event IDs mapped to their arrival time. IDs older than the dedup window (default: 10 minutes) are expired to prevent unbounded memory growth:

```python
def _is_duplicate(self, envelope: EventEnvelope) -> bool:
    now = datetime.now(tz=UTC)
    # expire old entries first
    expired = [eid for eid, seen_at in self._seen_ids.items()
               if (now - seen_at) > self._dedup_window]
    for eid in expired:
        del self._seen_ids[eid]
    return envelope.event_id in self._seen_ids
```

**Watermarking and late event detection**: an event is "late" if the gap between its event timestamp and its arrival time exceeds the watermark delay (5 minutes). This detects events that were delayed in the Kafka pipeline or arrived from a backfill:

```python
@property
def is_late(self, watermark_delay: timedelta = timedelta(minutes=5)) -> bool:
    return self.received_at - self.timestamp > watermark_delay
```

**Tumbling window aggregations**: metrics are grouped into 5-minute tumbling windows keyed by `(window_start, source, subject)`. Each window accumulates event counts, late counts, duplicate counts, and out-of-order counts.

The `KafkaAggregateRunner` wraps the `EventProcessor` in a Kafka consumer loop, flushing metrics to the `stream_metrics_5m` Gold table at configurable intervals. A single run with `--max-messages 500` produced 256 rows of windowed metrics covering both the `events` and `noise311` sources.

---

## 5. Storage Layer - Medallion Architecture

### 5.1 Bronze Layer

The Bronze layer is the system's source of truth. Every raw record ingested into the platform lives here permanently, unchanged. Two formats are used:

- **Parquet** for TLC data: the format in which TLC publishes files. No conversion is performed; files are stored exactly as downloaded.
- **NDJSON** for streaming data: one line per JSON record, one file per polling snapshot. NDJSON is human-readable, schema-flexible, and easy to inspect without tooling.

The sole transformation applied at Bronze ingestion is the injection of `_ingestion_timestamp`. For Parquet files (which are immutable binary formats), this is stored in a companion `.meta.json` sidecar rather than the file itself.

**Volumes:**

| Source | Files | Rows | Compressed size |
|--------|------:|-----:|----------------:|
| TLC Yellow Taxi | 35 + 35 sidecars | 123,897,542 | 2.0 GB |
| Permitted Events | 16 NDJSON | 204,469 | 97 MB |
| 311 Noise Complaints | 11 NDJSON | 100,131 | 170 MB |

### 5.2 Silver Layer

Silver applies the full transformation pipeline to Bronze data. The key property of Silver is that it preserves the original schema (no columns are removed, only added) while guaranteeing data quality: every row in Silver has passed all quality filters and has correct types.

**TLC transformation summary:**
- 123,897,542 Bronze rows → 103,383,051 Silver rows (83.4% retention)
- 13 source columns preserved + 14 derived columns added
- Naive datetimes → UTC-aware; zone IDs int64 → int32; monetary float64
- Partitioned into 171 files under `year=YYYY/month=MM/`

**Events transformation summary:**
- 204,469 Bronze rows → 35,209 Silver rows (17.2% retention after cross-snapshot dedup)
- The low retention is expected: the Socrata endpoint returns the full historical dataset on each poll. Deduplication by `event_id` is the primary transformation.
- Timestamps parsed to UTC-aware datetime; `event_duration_hours` computed

**Noise311 transformation summary:**
- 100,131 Bronze rows → 42,376 Silver rows (42.3% retention after dedup)
- `latitude`/`longitude` cast from string to float64; records outside NYC bounds removed
- `resolution_duration_hours` computed from `created_date` and `closed_date`

### 5.3 Gold Layer and Star Schema

Gold tables are analytics-ready Parquet files consumed directly by Superset and the REST API. All are single-file tables (no partitioning needed at Gold scale).

The most significant design decision in the Gold layer is the dual representation of taxi demand:

1. **`taxi_demand_by_zone`**: a denormalised flat table containing all measures alongside their context attributes (borough name, zone name, is_weekend, etc.). Optimised for direct dashboard queries without joins.

2. **Star schema (`dim_zone`, `dim_time`, `fact_taxi_demand`)**: a formal dimensional model enabling proper OLAP-style slice-and-dice queries. `dim_time` pre-computes all 168 hour×day combinations with human-readable labels, so charts can group by `period_label` without runtime string computation.

```
dim_zone (265 rows)              dim_time (168 rows)
────────────────────             ─────────────────────
zone_sk (PK) ────────────────┐  time_sk (PK) ──────────┐
PULocationID                 │  hour_of_day             │
borough                      │  day_of_week             │
zone_name                    │  day_name                │
service_zone                 │  is_weekend              │
                             │  is_rush_hour            │
                             │  period_label            │
                             └──────────┐ ┌─────────────┘
                                        │ │
                          fact_taxi_demand (39,198 rows)
                          ──────────────────────────────
                          zone_sk (FK)
                          time_sk (FK)
                          trip_count
                          avg_fare_amount
                          avg_trip_duration_minutes
                          avg_tip_percentage
                          total_trip_distance
                          pct_rush_hour
```

The `GoldStarSchemaProcessor` builds all three tables from the flat aggregate and the reference `taxi_zone_lookup.csv`.

### 5.4 Lakehouse with DuckDB and MinIO

**DuckDB as the query engine**: rather than loading Gold Parquet files into a persistent database, DuckDB reads them directly at query time via `read_parquet()` views. This eliminates an ETL step and means Superset always queries the current state of the Gold layer:

```sql
-- infra/superset/init_gold_views.sql
CREATE OR REPLACE VIEW taxi_demand AS
    SELECT * FROM read_parquet('/app/data/gold/taxi_demand_by_zone/*.parquet');

CREATE OR REPLACE VIEW batch_streaming_combined AS
    SELECT
        t.hour_of_day,
        SUM(t.trip_count) AS trip_count,
        COALESCE(SUM(CASE WHEN s.source = 'events' THEN s.event_count END), 0) AS event_count,
        COALESCE(SUM(CASE WHEN s.source = 'noise311' THEN s.event_count END), 0) AS noise_count
    FROM taxi_demand t
    LEFT JOIN stream_metrics_5m s
        ON t.hour_of_day = HOUR(s.window_start_utc)
    GROUP BY t.hour_of_day;
```

**MinIO as an optional S3 mirror**: when `MINIO_ENABLED=true`, every `GoldStorage.write()` call mirrors the Parquet file to `s3://gold-layer/<table_name>/`. This enables access by S3-compatible tools without changing the local query path. The default is `false` so local runs and CI never attempt a network connection.

**Bronze Validator**: `make validate-bronze` runs `BronzeValidator` across all three sources, checking:
1. Every file has `_ingestion_timestamp`
2. No column removals between successive partitions (additions are silently accepted)

The validator uses lowercase normalisation for Parquet column names because TLC changed `airport_fee` to `Airport_fee` between file versions: a case change that is not a breaking schema change:

```python
# Only flag removals — additions from upstream are expected
cols = frozenset(c.lower() for c in df.columns)
removed = baseline_cols - cols
if removed:
    result.schema_drift_files.append(str(parquet_path))
```

---

## 6. Serving Layer

### 6.1 FastAPI REST API

The REST API (`src/backend/api/main.py`) exposes the Gold tables as filterable endpoints. It reads Parquet files via `GoldStorage.read()` (which uses `pd.read_parquet` under the hood), the same abstraction used by the processing pipeline, so the API and the dashboards always see identical data.

Available endpoints:

| Endpoint | Filters | Description |
|----------|---------|-------------|
| `GET /health` |: | Service health check |
| `GET /gold/taxi-demand` | borough, zone_id, hour_of_day, day_of_week, is_weekend, limit | Taxi demand aggregates |
| `GET /gold/events` | borough, is_nightlife_event, is_weekend_event, start_after, start_before, limit | Event schedule |
| `GET /gold/noise` | borough, complaint_type, descriptor, hour_of_day, day_of_week, is_weekend, limit | Noise complaint aggregates |
| `GET /gold/summary` | - | Row counts and metadata for all Gold tables |

All endpoints return `{"count": N, "returned": M, "rows": [...]}`: the `count` field reflects the total matching rows, while `returned` reflects the applied `limit`. This lets API consumers page through large result sets efficiently.

Error handling is uniform: if a Gold table has not been built yet, the endpoint returns HTTP 503 with a actionable message rather than a 500:

```python
def _load_gold_or_404(table: str) -> pd.DataFrame:
    try:
        return _gold.read(table)
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail=f"Gold table '{table}' not available — run `uv run clm gold-build-all`",
        )
```

### 6.2 Apache Superset Dashboards

Four dashboards cover the platform's analytical scope:

**Dashboard 1: Late-Night Taxi**
Answers: where is taxi demand concentrated during late-night hours, and how does it compare to daytime? Charts: zone-level heatmap (trip_count × hour), top-10 zones by total trips, trips by hour split weekday vs weekend, average fare by borough.

**Dashboard 2: Tonight's Events**
Answers: what permitted events are happening, where, and when? Charts: events by borough, events by type, events per day timeline, upcoming nightlife events table. Nightlife events are identified by a heuristic flag (`is_nightlife_event`) set at Gold processing time.

**Dashboard 3: Live Noise Complaints**
Answers: when and where do noise complaints spike, and what types dominate? Charts: noise by borough × hour heatmap, noise by hour (weekday vs weekend), descriptor breakdown by borough, top complaint types.

**Dashboard 4: Batch + Streaming Combined**
Demonstrates the platform's hybrid nature by overlaying batch and streaming data in a single view. Charts: trips + events + noise counts by hour of day (from `batch_streaming_combined` view), streaming event volume by source and type, a quality table showing late-event and duplicate ratios per source from `stream_metrics_5m`.

All dashboards query DuckDB views that read Parquet files directly: no intermediate database loading step.

### 6.3 Airflow Orchestration

The batch pipeline is orchestrated by an Airflow DAG (`src/backend/airflow/dags/batch_pipeline.py`) that runs daily. The DAG has five tasks with the following dependency structure:

```
ingest_tlc_to_bronze
       │
process_tlc_bronze_to_silver    process_events_bronze_to_silver
       │                               │
       └──────────────┬────────────────┘
                      │  process_noise311_bronze_to_silver
                      │               │
                      └───────┬───────┘
                              ▼
                       build_gold_tables
```

`process_events_bronze_to_silver` and `process_noise311_bronze_to_silver` run independently of `ingest_tlc_to_bronze` and in parallel with each other. All three Silver tasks must complete before `build_gold_tables` runs.

Each task is a `BashOperator` wrapping the corresponding `clm` command. All tasks share a common `on_failure_callback` that logs task ID, run ID, and the exception:

```python
DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
}
```

`max_active_runs=1` prevents overlapping pipeline runs, which would cause race conditions on the Silver and Gold output directories. `catchup=False` prevents Airflow from trying to backfill missed daily runs when the platform is first deployed.

---

## 7. Results and Validation

### Row counts and retention

| Layer | Source | Rows | Notes |
|-------|--------|-----:|-------|
| Bronze | TLC Yellow Taxi | 123,897,542 | 35 monthly files, Jan 2023–Nov 2025 |
| Bronze | Permitted Events | 204,469 | 16 polling snapshots |
| Bronze | 311 Noise Complaints | 100,131 | 11 polling snapshots |
| Silver | TLC Yellow Taxi | 103,383,051 | 83.4% retention after quality filters |
| Silver | Permitted Events | 35,209 | 17.2% retention after cross-snapshot dedup |
| Silver | 311 Noise Complaints | 42,376 | 42.3% retention after dedup |
| Gold | taxi_demand_by_zone | 39,198 | 265 zones × 168 hour/day combinations |
| Gold | event_schedule | 35,209 | All unique events |
| Gold | noise_by_borough_hour | 8,074 | Borough × type × descriptor × hour × day |
| Gold | stream_metrics_5m | 256 | 5-min windows from one Kafka run |
| Gold | fact_taxi_demand | 39,198 | Star schema fact table |
| Gold | dim_zone | 265 | All TLC taxi zones |
| Gold | dim_time | 168 | 24 hours × 7 days |

### Bronze validation

`make validate-bronze` reports **0 issues** across 62 Bronze files after applying:
- Lowercase normalisation for Parquet column names (handles `airport_fee` / `Airport_fee` drift)
- Exclusion of `_ingestion_timestamp` and `_backfilled` from schema comparison
- Column-removal-only drift policy (additions from upstream are expected)

### Quality filter breakdown (TLC)

The 16.6% of Bronze TLC rows removed by quality filters break down roughly as follows (based on logged filter stats across the 35-file dataset):

| Filter | Approximate impact |
|--------|--------------------|
| `zero_or_negative_distance` | Largest contributor: many "meter-on" records |
| `zero_or_missing_passengers` | Driver-entered field, frequently 0 |
| `non_positive_duration` | Clock errors, same-second pickups and dropoffs |
| `negative_fare` / `negative_tip` | Reversals and corrections in TLC system |
| `invalid_datetime` | Rare; very old or malformed timestamps |
| `invalid_pickup_coords` | Only present in older files with lat/lon columns |

### Streaming pipeline quality (from stream_metrics_5m)

From the 256 rows of windowed metrics collected during a single `clm stream-aggregates` run:
- **Events source:** `late_event_ratio` near 1.0 for most windows: expected, since the Socrata Events API returns historical records from 2020 onward, all of which arrive "late" relative to the watermark
- **Noise311 source:** lower late-event ratio for recent complaints; higher for backfill records

The high late-event ratio for historical backfill data is not a pipeline failure: it is the correct behaviour of the watermark system detecting that old records are being ingested in bulk.

---

## 8. Design Trade-offs and Limitations

### Trade-off: local filesystem vs cloud object storage

The platform stores all data on the local filesystem rather than in a cloud object store. This simplifies development and testing: no credentials, no network dependency, no bucket management. The `GoldStorage` abstraction insulates the processing code from the storage backend, so switching to S3-native storage would require only changes to `GoldStorage`, not to processors or the API. The MinIO integration demonstrates this: enabling `MINIO_ENABLED=true` mirrors all writes to S3 semantics with zero changes to the pipeline.

The limitation is scalability: a local filesystem cannot serve concurrent writers or be accessed from multiple machines. For a production deployment, the Parquet files would be written directly to S3, and DuckDB would be configured with the `httpfs` extension.

### Trade-off: Pandas DataFrame pipeline vs Apache Spark

The entire batch processing pipeline uses Pandas DataFrames. This is appropriate for the dataset size (the largest single monthly TLC file is ~47 MB compressed) and avoids the significant operational complexity of running a Spark cluster for coursework. Pandas reads a 47 MB Parquet file in under a second and processes 3–4 million rows per file in a few seconds on a modern laptop.

The limitation is that this approach does not scale to datasets that exceed memory. A Spark implementation would partition the data across executors and process each partition independently. Given that each monthly file is processed independently (the processor loops over files), migrating to Spark would primarily involve replacing the Pandas transformation functions with PySpark equivalents: the architectural structure would remain the same.

### Trade-off: Silver deduplication strategy for streaming data

Silver deduplication for Events and Noise311 is performed at **write time**: each Silver processor reads all existing Silver files, builds a set of seen IDs, and only writes rows with new IDs. This is simple and correct but has O(N) memory cost as the Silver dataset grows.

An alternative would be deduplication at **read time** using DuckDB's `SELECT DISTINCT` over Bronze NDJSON files: this avoids storing the deduped state but re-processes the entire Bronze history on each Silver rebuild. For the current dataset sizes (35,209 and 42,376 unique records), write-time dedup is the better choice.

### Limitation: temporal coverage mismatch

TLC data covers January 2023 – November 2025, while streaming snapshots only cover March – May 2026. The `batch_streaming_combined` Gold view bridges this gap by treating the taxi demand data as a **time-of-week pattern** (aggregated by hour-of-day and day-of-week) rather than calendar dates, while streaming metrics are similarly aggregated by hour-of-day. This is analytically valid for identifying structural patterns (e.g., "Friday nights at 23:00 see both high taxi demand and high noise") but cannot answer calendar-date queries like "did taxi demand increase during the Taylor Swift concerts?"

### Limitation: streaming coverage

The `stream_metrics_5m` Gold table contains 256 rows from a single demonstration run. A production deployment would run `clm stream-aggregates` continuously or on a schedule, accumulating metrics over time. The infrastructure (Kafka topics, consumer group state, checkpoint files) is fully in place for continuous operation.

---

## 9. Conclusion

City Live Monitor implements a complete, production-grade data platform covering the full path from raw data sources to analytics dashboards. The key results are:

- **124 million raw TLC records** ingested, validated, and cleaned to 103 million Silver rows enriched with 14 analytical features
- **Two streaming data sources** (Permitted Events and 311 Noise) polled incrementally with circuit breakers, checkpoint-based cursors, and DLQ routing for malformed events
- **Seven Gold tables** covering flat aggregates and a formal star schema, all queryable via DuckDB in milliseconds
- **Four Superset dashboards** combining batch and streaming data, built on a fully-scripted bootstrap that installs datasets and charts from YAML manifests
- **0 Bronze validation issues** after implementing column-removal-only drift detection and case-normalised schema comparison

The platform demonstrates that a hybrid batch/streaming medallion architecture can be implemented on commodity hardware without managed cloud services, using only open-source tools (DuckDB, Kafka, MinIO, Airflow, Superset) and standard Python data libraries. The abstractions introduced: `BaseIngester`, `BaseProcessor`, `GoldStorage`, `BronzeValidator`: provide clear extension points for adding new data sources or output formats without modifying existing processing logic.
