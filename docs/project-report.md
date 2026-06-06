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
   - 6.4 Machine Learning Pipeline
7. [Results and Validation](#7-results-and-validation)
8. [Design Trade-offs and Limitations](#8-design-trade-offs-and-limitations)
9. [Conclusion](#9-conclusion)

---

## 1. Introduction and Motivation

Urban analytics platforms need to reconcile two very different data rhythms: large historical batch datasets that are ideal for deep cleaning and enrichment, and small, noisy real-time feeds that require low-latency ingestion and careful deduplication. City Live Monitor addresses that problem by combining NYC Yellow Taxi batch data with streaming 311 noise complaints and permitted events, then serving the result through a Bronze → Silver → Gold lakehouse.

The project currently supports four analytical perspectives:

- **Taxi demand:** where and when demand peaks across NYC taxi zones.
- **Event overlap:** which permitted events coincide with high demand areas and times.
- **Noise patterns:** how noise complaints vary by borough, type, descriptor, and hour.
- **Streaming health:** whether the Kafka-based stream pipeline is receiving late, duplicate, or out-of-order events.
- **Activity prediction:** a daily ML pipeline that forecasts high-activity days using taxi, noise, and event signals.

The choice of sources remains deliberate. The TLC Yellow Taxi dataset provides 35 monthly Parquet files spanning January 2023 to November 2025. The streaming sources are the NYC Open Data Socrata APIs for permitted events and 311 noise complaints, both of which are polled incrementally and written to Bronze as immutable NDJSON snapshots. In the current codebase, those snapshots are then used both by the Silver batch processors and by the ML feature builder.

---

## 2. System Architecture

```
┌──────────────── Sources ───────────────────────────────────────────────┐
│  NYC TLC Yellow Taxi                 NYC Open Data (Socrata)           │
│  35 monthly Parquet files            tvpp-9vvx.json  erm2-nwe9.json    │
└──────┬──────────────────────────────────────────┬───────────────────────┘
    │ batch download (validated, parallel)     │ HTTP pollers (checkpointed)
    ▼                                           ▼
┌──── Bronze ────────────────────────────────────────────────────────────┐
│  data/bronze/tlc/yellow/YYYY/*.parquet  (.meta.json sidecars)          │
│  data/bronze/nyc_permitted_events/YYYY/MM/*.ndjson                     │
│  data/bronze/nyc_311_noise/YYYY/MM/*.ndjson                            │
└──────┬──────────────────────────────────────────┬───────────────────────┘
    │ TLCBatchProcessor / Silver processors    │ Kafka topics
    │ PermittedEventsSilverProcessor           │ events.raw / noise311.raw
    │ Noise311SilverProcessor                  │ events.dlq / noise311.dlq
    ▼                                           ▼
┌──── Silver ────────────────────────────────────────────────────────────┐
│  data/silver/tlc/yellow/year=YYYY/month=MM/*.parquet                   │
│  data/silver/events/year=YYYY/month=MM/events_silver.parquet           │
│  data/silver/noise311/year=YYYY/month=MM/*.parquet                     │
│  (cleaned, typed, enriched, deduplicated, partitioned)                 │
└──────────────────────────┬─────────────────────────────────────────────┘
                  │ Gold processors + ML dataset builder
                  ▼
┌──── Gold ──────────────────────────────────────────────────────────────┐
│  taxi_demand_by_zone    event_schedule    noise_by_borough_hour        │
│  stream_metrics_5m      dim_zone          dim_time    fact_taxi_demand │
│  ml_training            ml_prediction                                  │
│  (mirrored to MinIO when MINIO_ENABLED=true)                           │
└──────────────────────────┬─────────────────────────────────────────────┘
                  │ DuckDB read_parquet() views
                  ▼
┌──── Serving ───────────────────────────────────────────────────────────┐
│  Apache Superset  (5 dashboards)    FastAPI REST  :8000                │
│  Airflow DAGs     (batch + ML)      MinIO console :9001                │
│  Prometheus       (9090)            Grafana        (3000)              │
└────────────────────────────────────────────────────────────────────────┘
```

The architecture follows the medallion pattern because it keeps raw data immutable in Bronze, moves cleaning and validation into Silver, and reserves Gold for analytics-ready outputs and predictions. The same pattern also makes reprocessing safe: every Gold table can be rebuilt from Bronze/Silver without mutating raw inputs.

---

## 3. Ingestion Layer

### 3.1 Batch Ingestion - NYC TLC Yellow Taxi

The TLC downloader (`src/backend/ingestion/batch/download_tlc.py`) fetches monthly Parquet files from the public CloudFront CDN and writes them to Bronze. The current implementation is incremental at the file level: existing files that pass validation are skipped, so a restart does not re-download the full corpus.

The downloader also uses:

- parallel downloads with a thread pool
- retry with exponential backoff on transient failures
- validation of file size and Parquet magic bytes
- atomic write via a temporary file and rename
- `.meta.json` sidecars containing `_ingestion_timestamp`, source filename, and file size

That sidecar is the only place where ingestion metadata is stored for TLC, because the Parquet payload itself is treated as immutable.

### 3.2 Streaming Ingestion - Socrata APIs via Kafka

Both streaming sources are polled from the NYC Open Data Socrata platform and share the same ingestion pattern:

- **311 Noise Complaints** are polled every 30 seconds.
- **Permitted Events** are polled every 30 minutes.

The pollers maintain checkpoint files under `data/checkpoints/`. On each successful run, the cursor is advanced only after all Kafka publishes succeed, which gives at-least-once semantics with safe re-ingestion on failure. The code also supports pagination for backfills, a consecutive-failure circuit breaker, and dead-letter topics for malformed events.

Each successful poll persists a full NDJSON snapshot to Bronze. Rows in that snapshot include `_ingestion_timestamp`, which keeps the Bronze layer auditable even if Kafka retention is shortened.

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

**Quality filtering** applies a sequence of rules, each tracked independently via `FilterRuleStat`. The processor currently rejects:

| Filter rule | Condition |
|-------------|-----------|
| `invalid_datetime` | Pickup or dropoff datetime could not be parsed |
| `out_of_range_year` | Pickup year outside 2020–2026 |
| `zero_or_negative_distance` | `trip_distance <= 0` |
| `negative_fare` | `fare_amount < 0` |
| `negative_tip` | `tip_amount < 0` |
| `negative_total` | `total_amount < 0` |
| `zero_or_missing_passengers` | `passenger_count < 1` |
| `distance_over_100_miles` | `trip_distance > 100` |
| `fare_over_500` | `fare_amount > 500` |
| `passenger_count_over_6` | `passenger_count > 6` |
| `tip_exceeds_fare` | `tip_amount > fare_amount` |
| `total_below_fare` | `total_amount < fare_amount` |
| `non_positive_duration` | `dropoff_datetime <= pickup_datetime` |
| `duration_under_1_minute` | Trip duration < 1 minute |
| `duration_over_24_hours` | Trip duration > 24 hours |
| `pickup_outside_nyc` | Pickup coords outside NYC bounds |
| `dropoff_outside_nyc` | Dropoff coords outside NYC bounds |

**Feature engineering** adds derived columns for trip duration, speed, temporal context, cost ratios, and outlier flags. The processor now exposes the following analytics features:

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
data["is_trip_distance_outlier"] = data["trip_distance"] > data["trip_distance"].quantile(0.99)
data["is_fare_outlier"] = data["fare_amount"] > data["fare_amount"].quantile(0.99)
data["is_duration_outlier_iqr"] = data["trip_duration_minutes"] > upper_iqr

# Duration capping at 95th percentile (IQR-sensitive)
p95 = data["trip_duration_minutes"].quantile(0.95)
data["trip_duration_capped_minutes"] = data["trip_duration_minutes"].clip(upper=p95)
```

Outlier rows are **flagged, not filtered**. This lets analysts inspect unusual records without losing raw signal. The processor also caps trip duration at the 95th percentile, which keeps extreme values from dominating downstream averages.

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

The `KafkaAggregateRunner` wraps the `EventProcessor` in a Kafka consumer loop, flushing metrics to the `stream_metrics_5m` Gold table at configurable intervals. The resulting metrics include counts for late, duplicate, and out-of-order events, plus a late-event ratio. Those metrics are used in the streaming health dashboard and in the processing report.

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
- 13 source columns preserved + derived features and outlier flags added
- Naive datetimes → UTC-aware; zone IDs cast to int32; monetary fields cast to numeric
- Partitioned into 171 files under `year=YYYY/month=MM/`

**Events transformation summary:**
- 204,469 Bronze rows → 35,209 Silver rows (17.2% retention after cross-snapshot dedup)
- The low retention is expected: the Socrata endpoint returns the full historical dataset on each poll.
- Deduplication by `event_id` is the primary transformation.
- Timestamps parsed to UTC-aware datetime; `event_duration_hours` computed; borough normalized from `event_borough`

**Noise311 transformation summary:**
- 100,131 Bronze rows → 42,376 Silver rows (42.3% retention after dedup)
- `latitude`/`longitude` cast from string to float64; records outside NYC bounds removed when coordinates exist
- `resolution_duration_hours` computed from `created_date` and `closed_date`

### 5.3 Gold Layer and Star Schema

Gold tables are analytics-ready Parquet files consumed directly by Superset, the REST API, and the ML workflow. Most tables are single-file outputs, while the ML pipeline writes its own training and prediction artefacts under `data/gold/ml_training/` and `data/gold/ml_prediction/`.

The most significant design decision in the Gold layer is the dual representation of taxi demand:

1. **`taxi_demand_by_zone`**: a denormalised flat table containing all measures alongside their context attributes (borough name, zone name, is_weekend, etc.). Optimised for direct dashboard queries without joins.

2. **Star schema (`dim_zone`, `dim_time`, `fact_taxi_demand`)**: a formal dimensional model enabling OLAP-style slice-and-dice queries. `dim_time` pre-computes all 168 hour×day combinations with human-readable labels.

3. **ML outputs (`ml_training`, `ml_prediction`)**: the activity classifier uses Gold/ML artefacts to train a daily binary model and publish a 7-day forecast.

```
dim_zone (265 rows)              dim_time (168 rows)
────────────────────             ─────────────────────
zone_sk (PK) ────────────────┐  time_sk (PK)  ──────────┐
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

The `GoldStarSchemaProcessor` builds all three star-schema tables from the flat aggregate and the reference taxi zone lookup.

The ML pipeline is built around `ml_dataset_builder.py`, `classifier.py`, and `predict.py`:

- `build_ml_dataset()` aggregates Silver TLC, events, and noise into a daily dataset with calendar and rolling features.
- `train_classifier()` trains a `HistGradientBoostingClassifier`, evaluates it with time-series cross-validation, and saves the model.
- `generate_predictions()` writes the current-day prediction plus a 7-day forecast to `data/gold/ml_prediction/daily_prediction.parquet`.

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

**MinIO as an optional S3 mirror**: when `MINIO_ENABLED=true`, every `GoldStorage.write()` call mirrors the Parquet file to `s3://gold-layer/<table_name>/`. This enables access by S3-compatible tools without changing the local query path. The default is `false`, so local runs and CI never attempt a network connection.

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

The ML pipeline also exposes prediction artefacts through Superset datasets backed by the Gold parquet files in `data/gold/ml_prediction/`.

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

Five dashboards cover the platform's analytical scope:

**Dashboard 1: Late-Night Taxi**
Answers: where is taxi demand concentrated during late-night hours, and how does it compare to daytime? Charts: zone-level heatmap (trip_count × hour), top-10 zones by total trips, trips by hour split weekday vs weekend, average fare by borough.

**Dashboard 2: Tonight's Events**
Answers: what permitted events are happening, where, and when? Charts: events by borough, events by type, events per day timeline, upcoming nightlife events table. Nightlife events are identified by a heuristic flag (`is_nightlife_event`) set at Gold processing time.

**Dashboard 3: Live Noise Complaints**
Answers: when and where do noise complaints spike, and what types dominate? Charts: noise by borough × hour heatmap, noise by hour (weekday vs weekend), descriptor breakdown by borough, top complaint types.

**Dashboard 4: Batch + Streaming Combined**
Demonstrates the platform's hybrid nature by overlaying batch and streaming data in a single view. Charts: trips + events + noise counts by hour of day (from `batch_streaming_combined` view), streaming event volume by source and type, a quality table showing late-event and duplicate ratios per source from `stream_metrics_5m`.

All dashboards query DuckDB views that read Parquet files directly: no intermediate database loading step.

**Dashboard 5: Prediction**
Adds a daily activity classifier built from the ML dataset. It shows the current-day confidence, a 7-day forecast, a detail table for forecast rows, and a recent trend comparison between actual and forecast probabilities.

The dashboard bootstrap is scripted and idempotent: `make superset-bootstrap` loads `infra/superset/init_gold_views.sql` and imports dashboard assets from `infra/superset/assets.zip`.

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

In addition to the batch DAG, the repository also includes `src/backend/airflow/dags/daily_prediction.py`, which refreshes noise counts, retrains the classifier, and generates the daily prediction artefact at 06:00 UTC.

### 6.4 Machine Learning Pipeline

The ML pipeline is a separate Airflow DAG (`src/backend/airflow/dags/daily_prediction.py`) that generates daily activity predictions using the trained HistGradientBoosting classifier. The workflow consists of three steps:

1. **Backfill historical counts** (optional): fetch aggregated daily noise and event counts from the Socrata API to extend training beyond the Bronze/Silver window. Output saved to `data/gold/ml_training/noise311_historical_counts.parquet` and `events_historical_counts.parquet`.

2. **Build ML dataset and train model**: the `train-model` command reads Silver taxi trips, noise complaints, and events, computes calendar features (day-of-week, month, holidays, etc.), rolling noise features (7d/14d/30d averages, percentile rank), and event-derived features (dow-average, weekend/Friday indicators). A binary target (high/low activity) is derived from the training-set noise median to prevent label leakage. The `HistGradientBoostingClassifier` is trained with TimeSeriesSplit cross-validation (n=5). Output: `data/gold/ml_training/ml_dataset.parquet` and `data/gold/ml_training/activity_classifier.pkl`.

3. **Generate predictions**: the `predict-activity` command applies the trained model to all dates in the ML dataset and generates a 7-day forecast by extending the feature logic with holiday proximity and seasonal dummies. Output: `data/gold/ml_prediction/daily_prediction.parquet` containing date, predicted label, probability, and forecast flag.

To manually trigger the ML pipeline:

```bash
clm backfill-noise311-daily-counts --start-date 2023-01-01 --end-date 2024-05-31
clm backfill-events-daily-counts --start-date 2023-01-01 --end-date 2026-05-28
clm train-model --start-date 2023-01-01  # defaults to today
clm predict-activity --horizon 7
```

Or via the Airflow DAG (scheduled daily at 06:00 UTC). ML outputs feed the Prediction dashboard in Superset and enable real-time activity classification on incoming data.

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
| Gold | noise_by_borough_hour | 11,570 | Borough × type × descriptor × hour × day |
| Gold | stream_metrics_5m | 256 | 5-min windows from one Kafka run |
| Gold | fact_taxi_demand | 39,198 | Star schema fact table |
| Gold | dim_zone | 265 | All TLC taxi zones |
| Gold | dim_time | 168 | 24 hours × 7 days |
| Gold | ml_prediction/daily_prediction | generated at runtime | Daily prediction output |

### Bronze validation

`make validate-bronze` reports **0 issues** across the current Bronze set after applying:
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
| `out_of_range_year` | Small number of files outside expected year range |
| `pickup_outside_nyc` / `dropoff_outside_nyc` | Only present in older files with lat/lon columns |

### Streaming pipeline quality (from stream_metrics_5m)

From the 256 rows of windowed metrics collected during a single `clm stream-aggregates` run:
- **Events source:** `late_event_ratio` near 1.0 for most windows, expected because the Socrata Events API returns historical records that are ingested in bulk.
- **Noise311 source:** lower late-event ratio for recent complaints; higher for backfill records.

The high late-event ratio for historical backfill data is not a pipeline failure: it is the correct behaviour of the watermark system detecting that old records are being ingested in bulk.

---

## 8. Monitoring and Observability

The platform ships a production-grade observability stack that starts automatically with `docker compose up`. No manual configuration is required.

### Infrastructure

| Component | Role |
| --------- | ---- |
| Prometheus | Scrapes all targets every 15 s; stores time-series metrics |
| Grafana | Renders four pre-provisioned dashboards from `infra/grafana/dashboards/` |
| Pushgateway | Accepts push metrics from short-lived jobs (Airflow tasks, ML runs) |
| kafka-exporter | Exposes Kafka broker and consumer group metrics to Prometheus |
| statsd-exporter | Translates Airflow StatsD UDP metrics to Prometheus format |

### Four dashboards

**Pipeline Health (`nyc-pipeline`)** monitors the Kafka streaming layer: consumer lag per partition, message ingestion rate, dead-letter queue growth, and broker availability. Consumer lag alerts fire when lag exceeds 500 messages for 5 minutes; DLQ alerts fire immediately on any new dead-letter message.

**Backend API (`nyc-backend-api`)** monitors the FastAPI backend: request rate, P95 latency, error-rate percentage, HTTP status distribution by endpoint, and the three ML health panels (ROC-AUC score, time since last retrain, time since last prediction). ML metrics are pushed to Pushgateway by the Airflow retrain and prediction tasks using `prometheus_client`.

**MinIO Storage (`nyc-minio`)** monitors disk usage percentage, free bytes, total object count, and S3 API traffic. Disk alerts fire at 90% (warning) and 95% (critical), because the Gold pipeline writes several gigabytes per batch run.

**Airflow (`nyc-airflow`)** monitors the scheduler heartbeat (Alive/Dead), DAG bag size, import error count, executor slot usage, and — via Pushgateway — the time elapsed since each DAG last succeeded. Each DAG's final task pushes `airflow_dag_last_success_timestamp_seconds` to Pushgateway; Grafana displays `time() - airflow_dag_last_success_timestamp_seconds` with a human-readable duration unit.

### Alert rules

Eighteen alert rules across five groups (`kafka`, `backend`, `ml`, `minio`, `airflow`) are defined in `infra/prometheus/alerts.yml`. Full details are in `docs/monitoring-dashboards.md`.

---

## 9. Design Trade-offs and Limitations

### Trade-off: local filesystem vs cloud object storage

The platform stores all data on the local filesystem rather than in a cloud object store. This simplifies development and testing: no credentials, no network dependency, no bucket management. The `GoldStorage` abstraction insulates the processing code from the storage backend, so switching to S3-native storage would require only changes to `GoldStorage`, not to processors or the API. The MinIO integration demonstrates this: enabling `MINIO_ENABLED=true` mirrors all writes to S3 semantics with zero changes to the pipeline.

The limitation is scalability: a local filesystem cannot serve concurrent writers or be accessed from multiple machines. For a production deployment, the Parquet files would be written directly to S3, and DuckDB would be configured with the `httpfs` extension.

### Trade-off: Pandas DataFrame pipeline vs Apache Spark

The entire batch processing pipeline uses Pandas DataFrames. This is appropriate for the dataset size (the largest single monthly TLC file is ~47 MB compressed) and avoids the significant operational complexity of running a Spark cluster for coursework. Pandas reads a 47 MB Parquet file in under a second and processes 3–4 million rows per file in a few seconds on a modern laptop.

The limitation is that this approach does not scale to datasets that exceed memory. A Spark implementation would partition the data across executors and process each partition independently. Given that each monthly file is processed independently (the processor loops over files), migrating to Spark would primarily involve replacing the Pandas transformation functions with PySpark equivalents: the architectural structure would remain the same.

### Trade-off: Silver deduplication strategy for streaming data

Silver deduplication for Events and Noise311 is performed at write time. The event processor merges new rows with the existing canonical month partition, deduplicates by `event_id`, and then rewrites the partition. Noise311 deduplicates by `unique_key` within the transformed snapshot. This keeps the canonical Silver state deterministic, but it does mean a rebuild touches the relevant partitions again.

The trade-off is intentional: the current dataset sizes are small enough to favor correctness and transparency over incremental complexity.

### Limitation: temporal coverage mismatch

TLC data covers January 2023 to November 2025, while the streaming snapshots and ML features are built from the available 2026 ingestion window. The `batch_streaming_combined` Gold view bridges this gap by working at the hour-of-day level rather than calendar dates. That is appropriate for structural analysis, but it cannot answer date-specific correlation questions.

### Limitation: streaming coverage

The `stream_metrics_5m` Gold table and the ML prediction output are generated from the currently available runtime runs. A production deployment would run `clm stream-aggregates` and the daily prediction DAG continuously or on a schedule, accumulating more history over time.

### Limitation: ML artefacts are runtime outputs

The classifier model, ML dataset, and daily prediction parquet are generated at runtime. They are not committed to the repository, so first-time bootstrap requires running the ML commands before the Superset prediction dashboard can show fresh data.

---

## 9. Conclusion

City Live Monitor now implements a complete batch, streaming, and ML analytics platform. The main current outcomes are:

- **124 million raw TLC records** ingested, validated, and cleaned to 103 million Silver rows with derived features and outlier flags.
- **Two streaming sources** polled incrementally with checkpoint-based cursors, circuit breakers, and DLQ routing for malformed events.
- **Gold tables for analytics and ML**, including taxi demand, events, noise, stream health metrics, star-schema tables, ML training data, and daily prediction artefacts.
- **Five Superset dashboards** covering taxi, events, noise, streaming health, and prediction.
- **Two Airflow DAGs**: one for batch medallion processing and one for daily prediction refresh.
- **Monitoring stack** with Prometheus and Grafana, plus Kafka Exporter.

The platform demonstrates that a hybrid batch/streaming/ML medallion architecture can be implemented on commodity hardware without managed cloud services, using open-source tools such as DuckDB, Kafka, MinIO, Airflow, Superset, Prometheus, and Grafana. The abstractions introduced (`BaseIngester`, `BaseProcessor`, `GoldStorage`, `BronzeValidator`) keep the processing code modular and let new sources or outputs be added without rewriting the existing pipeline.
