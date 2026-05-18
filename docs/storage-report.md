# Storage Report: City Live Monitor

**Authors:** Gabriel Silva · Hugo Castro, University of Aveiro, 2026
**Generated:** 2026-05-18
**Architecture:** Bronze → Silver → Gold Medallion Lakehouse

---

## 1. Architecture Overview

The platform implements a three-layer medallion architecture over a local filesystem lakehouse, with an optional S3-compatible mirror via MinIO.

```
┌─── Bronze ─────────────────────────────────────────────────────────────┐
│  Raw, immutable data. Format matches source (Parquet for TLC, NDJSON   │
│  for streaming). Append-only. Never modified after ingestion.          │
│  Every file has an _ingestion_timestamp guarantee.                     │
└────────────────────────┬───────────────────────────────────────────────┘
                         │ processors apply cleaning, typing, enrichment
┌─── Silver ─────────────▼───────────────────────────────────────────────┐
│  Cleaned, typed, deduplicated. Source schema preserved; new computed   │
│  columns added. Partitioned by (year, month) for TLC; by snapshot     │
│  date for streaming sources.                                           │
└────────────────────────┬───────────────────────────────────────────────┘
                         │ aggregation + dimensional modelling
┌─── Gold ───────────────▼───────────────────────────────────────────────┐
│  Analytics-ready tables. One Parquet file per table. DuckDB views      │
│  expose them to Superset. Optionally mirrored to MinIO S3.             │
└────────────────────────────────────────────────────────────────────────┘
```

### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Local filesystem as primary store | Eliminates infrastructure dependency for course work; same API surface as a cloud object store |
| Parquet throughout Silver/Gold | Columnar format enables predicate push-down, column pruning, efficient compression |
| NDJSON at Bronze for streaming | Human-readable, line-delimited, schema-flexible; matches Socrata API response shape |
| `.meta.json` sidecars for TLC | Parquet files cannot be modified in-place; a companion JSON file carries ingestion metadata without touching raw data |
| Append-only Bronze | Ensures reproducibility and audit traceability; every raw snapshot is permanently available |
| Column-removal-only schema drift | Upstream sources add fields over time (e.g., `airport_fee` added in 2022); only removed columns indicate a breaking change |

---

## 2. Bronze Layer

### 2.1 Volume Summary

| Source | Files | Rows | Size | Partition Scheme |
|--------|------:|-----:|-----:|-----------------|
| TLC Yellow Taxi | 35 Parquet + 35 `.meta.json` | 123,897,542 | 2.0 GB | `YYYY/yellow_tripdata_YYYY-MM.parquet` |
| NYC Permitted Events | 16 NDJSON | 204,469 | 97 MB | `YYYY/MM/snapshot_<ISO8601Z>.ndjson` |
| 311 Noise Complaints | 11 NDJSON | 100,131 | 170 MB | `YYYY/MM/snapshot_<ISO8601Z>.ndjson` |
| **Total** | **97 files** | **~124.2 M** | **~2.3 GB** | |

Coverage: January 2023 – November 2025 (TLC); March 2026 – May 2026 (streaming snapshots).

### 2.2 Ingestion Timestamp Guarantee

Every Bronze artefact carries `_ingestion_timestamp` (UTC ISO-8601):

- **TLC Parquet**: stored in a companion `.meta.json` sidecar (e.g., `yellow_tripdata_2023-01.meta.json`). Parquet files are immutable; the sidecar avoids file mutation. The 35 files predating the feature were backfilled via `clm backfill-bronze-meta` using the file's mtime as a conservative timestamp.
- **NDJSON snapshots**: injected as a field inside each JSON row on write. Older snapshots (before the field was standardised) were patched in-place by `clm backfill-ndjson-meta`.

```json
// Example .meta.json sidecar
{
  "_ingestion_timestamp": "2026-04-20T18:21:12.264789+00:00",
  "source_file": "yellow_tripdata_2023-01.parquet",
  "size_bytes": 47673370,
  "_backfilled": true
}
```

### 2.3 TLC Yellow Taxi Bronze Schema

| Column | Type | Notes |
|--------|------|-------|
| `VendorID` | int64 | 1 = Creative Mobile, 2 = VeriFone |
| `tpep_pickup_datetime` | timestamp[us] | Local NYC time, no timezone |
| `tpep_dropoff_datetime` | timestamp[us] | Local NYC time, no timezone |
| `passenger_count` | double | Nullable (driver-entered) |
| `trip_distance` | double | Miles |
| `RatecodeID` | double | 1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated, 6=Group |
| `store_and_fwd_flag` | string | Y/N; whether record stored locally before transmit |
| `PULocationID` | int64 | TLC Taxi Zone ID (1–265) |
| `DOLocationID` | int64 | TLC Taxi Zone ID (1–265) |
| `payment_type` | int64 | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `fare_amount` | double | USD |
| `extra` | double | USD; misc extras and surcharges |
| `mta_tax` | double | USD |
| `tip_amount` | double | USD; auto-populated for credit card tips |
| `tolls_amount` | double | USD |
| `improvement_surcharge` | double | USD |
| `total_amount` | double | USD |
| `congestion_surcharge` | double | USD; added 2019 |
| `airport_fee` | double | USD; added 2022; absent in older files |

Schema note: `airport_fee` was added by TLC in mid-2022 and `Airport_fee` (capital A) appeared in some 2024 files. The Bronze Validator normalises column names to lowercase before schema-drift comparison so this does not trigger a false positive.

### 2.4 NYC Permitted Events Bronze Schema

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | string | Unique Socrata row identifier |
| `event_name` | string | Official event name |
| `start_date_time` | string | ISO-8601, local time |
| `end_date_time` | string | ISO-8601, local time |
| `event_agency` | string | Permitting agency |
| `event_type` | string | Parade, Film, Athletic, etc. |
| `event_borough` | string | NYC borough |
| `event_location` | string | Street address / park name |
| `street_closure_type` | string | Full / Partial / No closure |
| `community_board` | string | |
| `police_precinct` | string | |
| `cemsid` | string | Internal city event ID |
| `_ingestion_timestamp` | string | UTC ISO-8601; injected on write |
| `_backfilled` | bool | True only for pre-feature snapshots |

### 2.5 311 Noise Complaints Bronze Schema

| Column | Type | Notes |
|--------|------|-------|
| `unique_key` | string | Socrata row ID |
| `created_date` | string | ISO-8601 |
| `closed_date` | string | ISO-8601, nullable |
| `agency` | string | Always "NYPD" for noise |
| `complaint_type` | string | "Noise - Residential", "Noise - Commercial", etc. |
| `descriptor` | string | Specific noise type |
| `borough` | string | NYC borough |
| `latitude` | string | Nullable |
| `longitude` | string | Nullable |
| `location` | dict | GeoJSON point, nullable |
| `_ingestion_timestamp` | string | UTC ISO-8601; injected on write |
| `_backfilled` | bool | True only for pre-feature snapshots |
| *(+ 18 address/administrative columns)* | | |

### 2.6 Schema Drift Policy

The `BronzeValidator` (`src/backend/utils/bronze_validator.py`) checks all Bronze files on each `make validate-bronze` run:

1. **Missing `_ingestion_timestamp`**: logged as a warning per file.
2. **Schema drift**: only **column removals** are flagged as issues. Column additions are expected (upstream fields are added over time) and are silently accepted. This prevents false positives from TLC schema evolution.

---

## 3. Silver Layer

### 3.1 Volume Summary

| Source | Partitions | Rows | Size | Partition Scheme |
|--------|----------:|-----:|-----:|-----------------|
| TLC Yellow Taxi | 171 Parquet | 103,383,051 | 4.8 GB | `year=YYYY/month=MM/` |
| Permitted Events | 13 Parquet | 35,209 | 2.0 MB | one file per snapshot batch |
| 311 Noise | 8 Parquet | 42,376 | 5.1 MB | one file per snapshot batch |
| **Total** | **192 files** | **~103.5 M** | **~4.8 GB** | |

### 3.2 TLC Silver Transformations Applied

| Category | Rule |
|----------|------|
| **Type casting** | `tpep_pickup_datetime` / `tpep_dropoff_datetime` → UTC-aware `datetime64[us, UTC]`; monetary columns → float64; zone IDs → int32 |
| **Quality filters** | Zero-distance trips removed; negative fares removed; zero-passenger trips removed; invalid zone IDs (0, >265) removed; trip duration < 1 min or > 24 h removed; coordinates outside NYC bounds removed |
| **Outlier flags** | `is_tip_outlier` (tip > 50% of fare); `is_speed_outlier` (> 80 mph); `is_unusual_midnight_trip` (hour 0–4) |
| **Outlier capping** | `trip_duration_capped_minutes`; duration capped at 95th percentile per month partition |
| **Feature engineering** | `trip_duration_minutes`, `trip_speed_mph`, `hour_of_day`, `day_of_week`, `is_weekend`, `is_rush_hour` (hours 7–9 and 16–19, all days), `tip_percentage`, `cost_per_mile` |

**Retention rate:** 103,383,051 / 123,897,542 = **83.4%** (16.6% of Bronze rows filtered by quality rules).

### 3.3 TLC Silver Schema

| Column | Type | Source / Derived |
|--------|------|-----------------|
| `VendorID` | int32 | Bronze (cast) |
| `tpep_pickup_datetime` | datetime64[us] | Bronze (cast) |
| `tpep_dropoff_datetime` | datetime64[us] | Bronze (cast) |
| `passenger_count` | float64 | Bronze |
| `trip_distance` | float64 | Bronze |
| `RatecodeID` | float64 | Bronze |
| `store_and_fwd_flag` | str | Bronze |
| `PULocationID` | int32 | Bronze (cast) |
| `DOLocationID` | int32 | Bronze (cast) |
| `payment_type` | int64 | Bronze |
| `fare_amount` – `airport_fee` | float64 | Bronze monetary columns |
| `pickup_datetime_utc` | datetime64[us, UTC] | Derived; UTC-aware pickup |
| `dropoff_datetime_utc` | datetime64[us, UTC] | Derived; UTC-aware dropoff |
| `trip_duration_minutes` | float64 | Derived |
| `trip_speed_mph` | float64 | Derived |
| `hour_of_day` | int32 | Derived (0–23) |
| `day_of_week` | int32 | Derived (0=Mon) |
| `is_weekend` | bool | Derived |
| `is_rush_hour` | bool | Derived |
| `tip_percentage` | float64 | Derived |
| `cost_per_mile` | float64 | Derived |
| `is_unusual_midnight_trip` | bool | Outlier flag |
| `is_tip_outlier` | bool | Outlier flag |
| `is_speed_outlier` | bool | Outlier flag |
| `is_duration_outlier_iqr` | bool | Outlier flag |
| `trip_duration_capped_minutes` | float64 | Outlier-capped version |

### 3.4 Events Silver Transformations Applied

| Category | Rule |
|----------|------|
| **Type casting** | `start_date_time` / `end_date_time` → UTC-aware `datetime64[us, UTC]` |
| **Deduplication** | Write-time dedup by `event_id` across all ingested snapshots; each event appears exactly once |
| **Feature engineering** | `event_duration_hours`, `borough` (normalised from `event_borough`) |

**Dedup ratio:** 204,469 Bronze rows → 35,209 Silver rows. The Socrata API returns the full historical dataset on each poll; deduplication eliminates 82.8% of Bronze rows as cross-snapshot duplicates.

### 3.5 Events Silver Schema

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | str | Unique identifier |
| `event_name` | str | |
| `start_datetime_utc` | datetime64[us, UTC] | Cast from string |
| `end_datetime_utc` | datetime64[us, UTC] | Cast from string |
| `event_type` | str | |
| `borough` | str | Normalised borough name |
| `event_location` | str | |
| `event_duration_hours` | float64 | Derived |
| `event_agency`, `street_closure_type`, `community_board`, `police_precinct`, `cemsid` | str | Preserved from Bronze |

### 3.6 Noise311 Silver Transformations Applied

| Category | Rule |
|----------|------|
| **Type casting** | `latitude` / `longitude` → float64; `created_date` / `closed_date` → UTC-aware datetime |
| **Deduplication** | By `unique_key` across snapshots |
| **Geocoding filter** | Records outside NYC bounding box (lat 40.48–40.93, lon −74.26–−73.68) removed; records with null coordinates kept |
| **Feature engineering** | `resolution_duration_hours`, `created_datetime_utc`, `closed_datetime_utc` |

**Dedup ratio:** 100,131 Bronze rows → 42,376 Silver rows (57.7% cross-snapshot duplicates).

### 3.7 Noise311 Silver Added Columns

| Column | Type | Description |
|--------|------|-------------|
| `latitude` | float64 | Cast from string |
| `longitude` | float64 | Cast from string |
| `created_datetime_utc` | datetime64[us, UTC] | UTC-aware created date |
| `closed_datetime_utc` | datetime64[us, UTC] | UTC-aware closed date |
| `resolution_duration_hours` | float64 | Derived |
| `descriptor_2` | str | Socrata field alias |
| `address_type` | str | Socrata field |
| `facility_type` | str | Socrata field |

---

## 4. Gold Layer

### 4.1 Volume Summary

| Table | Rows | Size | Description |
|-------|-----:|-----:|-------------|
| `taxi_demand_by_zone` | 39,198 | 1.3 MB | Trip demand × zone × hour × day |
| `event_schedule` | 35,209 | 1.5 MB | Cleaned event calendar with time features |
| `noise_by_borough_hour` | 8,074 | 80 KB | Noise aggregations × borough × type × hour |
| `stream_metrics_5m` | 256 | 16 KB | 5-min Kafka window metrics |
| `dim_zone` | 265 | 12 KB | Star schema: TLC zone dimension |
| `dim_time` | 168 | 12 KB | Star schema: 168 hour × day combinations |
| `fact_taxi_demand` | 39,198 | 1.3 MB | Star schema fact table |
| **Total** | **~122 K** | **~4.2 MB** | |

### 4.2 Flat Aggregate Tables

#### `taxi_demand_by_zone`

Pre-aggregated from Silver TLC (103M rows → 39,198 unique zone/hour/day combinations).

| Column | Type | Description |
|--------|------|-------------|
| `PULocationID` | int64 | TLC zone (1–265) |
| `borough` | str | NYC borough |
| `zone_name` | str | TLC zone name |
| `hour_of_day` | int32 | 0–23 |
| `day_of_week` | int32 | 0=Monday |
| `is_weekend` | bool | |
| `trip_count` | int64 | Trips in this cell |
| `avg_fare_amount` | float64 | USD |
| `avg_trip_duration_minutes` | float64 | |
| `avg_tip_percentage` | float64 | |
| `total_trip_distance` | float64 | Miles |
| `pct_rush_hour` | float64 | Fraction of trips during rush hour |

#### `event_schedule`

Pass-through of Silver Events with temporal features added.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | str | Unique identifier |
| `event_name` | str | |
| `event_type` | str | |
| `borough` | str | |
| `event_location` | str | |
| `start_datetime_utc` | datetime64[us, UTC] | |
| `end_datetime_utc` | datetime64[us, UTC] | |
| `event_duration_hours` | float64 | |
| `start_hour_of_day` | int8 | |
| `start_day_of_week` | int8 | |
| `is_weekend_event` | bool | |
| `is_nightlife_event` | bool | Heuristic: late-night + type contains nightlife |

#### `noise_by_borough_hour`

Aggregated from Silver Noise (42,376 rows → 8,074 unique borough/type/descriptor/hour/day cells).

| Column | Type | Description |
|--------|------|-------------|
| `borough` | str | NYC borough |
| `complaint_type` | str | Noise category |
| `descriptor` | str | Specific noise type |
| `hour_of_day` | int8 | |
| `day_of_week` | int8 | |
| `is_weekend` | bool | |
| `complaint_count` | int64 | |
| `avg_resolution_hours` | float64 | |

#### `stream_metrics_5m`

Populated by `clm stream-aggregates` (Kafka consumer). 256 rows from a single run.

| Column | Type | Description |
|--------|------|-------------|
| `window_start_utc` | datetime64[us, UTC] | Tumbling window start |
| `window_end_utc` | datetime64[us, UTC] | Tumbling window end (start + 5 min) |
| `source` | str | "events" or "noise311" |
| `subject` | str | Event type / complaint type |
| `event_count` | int64 | Events in window |
| `late_event_count` | int64 | Events arriving after watermark |
| `duplicate_event_count` | int64 | Duplicates detected by dedup window |
| `out_of_order_event_count` | int64 | Events with timestamp < previous |
| `late_event_ratio` | float64 | late_event_count / event_count |

### 4.3 Star Schema

The dimensional model enables OLAP-style queries over taxi demand data.

```
        dim_zone (265 rows)           dim_time (168 rows)
        ───────────────────           ───────────────────
        zone_sk (PK)                  time_sk (PK)
        PULocationID                  hour_of_day
        borough                       day_of_week
        zone_name                     day_name
        service_zone                  is_weekend
              │                       is_rush_hour
              │                       period_label
              └──────────┐  ┌─────────┘
                         │  │
               fact_taxi_demand (39,198 rows)
               ──────────────────────────────
               zone_sk (FK → dim_zone)
               time_sk (FK → dim_time)
               PULocationID
               hour_of_day / day_of_week
               trip_count
               avg_fare_amount
               avg_trip_duration_minutes
               avg_tip_percentage
               total_trip_distance
               pct_rush_hour
```

**Rationale:** The star schema separates slowly-changing descriptive attributes (zone geography, time labels) from high-cardinality fact data. This allows dimension tables to be joined once and cached, reducing query cost for slice-and-dice operations in Superset.

**dim_time design:** 168 rows = 24 hours × 7 days. All temporal combinations are pre-computed, so Superset charts can group by `period_label` (e.g., "Mon 08:00: Rush Hour") without runtime string operations.

---

## 5. Lakehouse Implementation

### 5.1 Query Engine: DuckDB

All Gold tables are exposed to Superset through DuckDB `read_parquet()` views defined in `infra/superset/init_gold_views.sql`:

```sql
CREATE OR REPLACE VIEW taxi_demand AS
  SELECT * FROM read_parquet('/app/data/gold/taxi_demand_by_zone/*.parquet');

CREATE OR REPLACE VIEW batch_streaming_combined AS
  SELECT t.hour_of_day, t.trip_count, ...
  FROM taxi_demand t
  LEFT JOIN stream_metrics_5m s ON t.hour_of_day = s.window_hour;
```

DuckDB reads Parquet files directly from disk using columnar I/O, with predicate push-down and column pruning. No ETL step is needed to load data into a database engine.

### 5.2 MinIO S3 Mirror (Optional)

When `MINIO_ENABLED=true` in `.env`, `GoldStorage.write()` mirrors every Gold table write to `s3://gold-layer/<table_name>/`. This enables:

- Access from external tools that consume S3 paths
- Separation of compute (DuckDB) from storage (MinIO)
- Multi-reader access without filesystem sharing

The MinIO endpoint is `http://localhost:9000` (console at `:9001`). Credentials: `minioadmin` / `minioadmin` (configurable via `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`).

### 5.3 Serving Layer

| Tool | Dataset | Access Pattern |
|------|---------|---------------|
| Apache Superset | DuckDB views over Gold Parquet | Interactive dashboards, ad-hoc SQL |
| FastAPI (`:8000`) | DuckDB read at request time | REST API for programmatic access |
| Airflow DAG | Triggers Silver + Gold processors | Scheduled batch orchestration |

---

## 6. Data Consistency Across Layers

### 6.1 Row Count Flow

```
Source        Bronze          Silver         Gold (aggregated)
──────────────────────────────────────────────────────────────
TLC           123,897,542     103,383,051    39,198 cells
              (35 files)      (171 files)    (zone×hour×day)

Events        204,469         35,209         35,209
              (16 snapshots)  (deduped)      (pass-through)

Noise311      100,131         42,376         8,074 cells
              (11 snapshots)  (deduped)      (borough×type×hour)
```

**TLC Silver retention:** 83.4% of Bronze rows pass quality filters. The 16.6% removed include zero-distance trips, invalid zone IDs, extreme durations, and data entry errors.

**Streaming deduplication:** The Socrata API returns full historical datasets on each poll. The Silver layer deduplicates aggressively: Events retains 17.2% of Bronze rows; Noise311 retains 42.3%. This is expected; each snapshot is a complete snapshot of the source, not a delta.

### 6.2 Temporal Consistency

- TLC data spans January 2023 – November 2025 (35 monthly files).
- Streaming snapshots span March 2026 – May 2026 (collected during development).
- The Gold `batch_streaming_combined` view joins TLC demand (aggregated to hour-of-day/day-of-week, i.e., time-of-week patterns) with streaming metrics, making the temporal gap acceptable for pattern analysis.

### 6.3 Schema Consistency Validation

`make validate-bronze` / `clm validate-bronze` checks:

1. All Bronze files have `_ingestion_timestamp`
2. No column removals between successive partitions of the same source

Current status after fixes: **0 issues across 62 files** (35 TLC Parquet + 16 Events NDJSON + 11 Noise NDJSON).

---

## 7. Query Performance

All Gold tables are single Parquet files (< 2 MB each). DuckDB reads them in milliseconds.

| Query | Approx. Latency | Notes |
|-------|----------------|-------|
| Full scan of `taxi_demand_by_zone` (39,198 rows) | < 10 ms | Single file, columnar |
| Superset chart render (GROUP BY borough + hour) | < 100 ms | DuckDB predicate push-down |
| `batch_streaming_combined` LEFT JOIN | < 50 ms | Two small files |
| Silver TLC full scan (103M rows, 4.8 GB) | ~15–30 s | Not used in dashboards; Gold pre-aggregates |

Silver TLC is never scanned at query time; all dashboard queries hit Gold tables exclusively. The 103M-row Silver layer is consumed once by the Gold processors to produce the 39,198-row `taxi_demand_by_zone` aggregate.

---

## 8. Storage Layout Reference

```
data/
├── bronze/
│   ├── tlc/yellow/
│   │   ├── 2023/  yellow_tripdata_2023-{01..12}.parquet
│   │   │         yellow_tripdata_2023-{01..12}.meta.json
│   │   ├── 2024/  (12 files + 12 sidecars)
│   │   └── 2025/  (11 files Jan–Nov + 11 sidecars)
│   ├── nyc_permitted_events/
│   │   ├── 2026/03/  snapshot_<ISO8601Z>.ndjson  (5 files)
│   │   ├── 2026/04/  (10 files)
│   │   └── 2026/05/  (1 file)
│   └── nyc_311_noise/
│       ├── 2026/03/  (1 file)
│       ├── 2026/04/  (8 files)
│       └── 2026/05/  (2 files)
├── silver/
│   ├── tlc/yellow/
│   │   └── year=YYYY/month=MM/  *_silver.parquet  (171 files)
│   ├── events/  *.parquet  (13 files)
│   └── noise311/  *.parquet  (8 files)
└── gold/
    ├── taxi_demand_by_zone/  part.0.parquet
    ├── event_schedule/       part.0.parquet
    ├── noise_by_borough_hour/  part.0.parquet
    ├── stream_metrics_5m/    part.0.parquet
    ├── dim_zone/             part.0.parquet
    ├── dim_time/             part.0.parquet
    └── fact_taxi_demand/     part.0.parquet
```
