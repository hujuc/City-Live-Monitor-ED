# Assignment 2 - TODO

Status legend: `[x]` done, `[-]` partial, `[ ]` not started

---

## 3. Ingestion Layer

### 3.1 Batch Data Ingestion

- [x] Programmatic download of multiple months of NYC Taxi data
- [x] Incremental download logic (skip existing files)
- [x] Parallel downloads for multiple months
- [x] Retry with exponential backoff for failed downloads
- [x] Validate downloaded files (size, format, magic bytes)
- [x] Write `.meta.json` sidecar with `_ingestion_timestamp` after each download

### 3.2 Streaming Data Ingestion

- [x] Poll Socrata APIs on scheduled interval (311: 30s, Events: 30min)
- [x] Emit events to Kafka topics
- [x] Deterministic event IDs + idempotent Silver dedup (at-least-once; exactly-once not achievable without Kafka transactions)
- [x] Circuit breaker for API failures
- [x] Pagination for large backfills (>2000 rows per poll)
- [x] Inject `_ingestion_timestamp` into every NDJSON row on write
- [x] Dead Letter Queue (DLQ) topics for malformed events (`events.dlq`, `noise311.dlq`)

### 3.3 Data Profiling

- [x] Profile ALL ingested data sources (batch + streaming samples)
  - Report correctly uses Yellow Taxi (`tpep_*` columns), 35 files, statistical profiles with severity ratings
- [x] Data Profiling Report (`docs/data-profiling-report.md`) — generated from Yellow Taxi Bronze; schema, stats, severity ratings, cleaning recommendations for all 3 sources

### 3.4 Infrastructure Setup

- [x] Docker Compose with all services (Kafka, MinIO, Airflow, Superset, Backend)
- [x] Monitoring: Kafka Exporter + Prometheus (port 9090) + Grafana (port 3000) — `infra/prometheus/`, `infra/grafana/`
- [x] Version control with Git (master + dev + feature branches)
- [x] Environment-based configuration (.env)
- [x] Pre-commit hooks (ruff, formatting, whitespace)

### 3.5 Deliverables

- [x] Data Profiling Report (`docs/data-profiling-report.md`) — Yellow Taxi Bronze; schema, stats, severity ratings, cleaning recommendations for all 3 sources
- [x] README.md (Architecture, Prerequisites, Quick Start, Services, CLI Reference, Data Sources, Storage Layout, Dashboards, Development)
- [x] Code repository (.gitignore, meaningful commits, feature branching strategy)

---

## 4. Stream & Batch Processing Layer

### 4.1 Batch Processing (NYC Taxi Data)

- [x] Clean and standardize 35 months of taxi trip data (Jan 2023 - Nov 2025)
- [x] Data type casting (timestamps to UTC, monetary to float64, zone IDs to int32)
- [x] Standardization rules (borough names, payment types)
- [x] Data quality filtering — implemented rules:
  - [x] zero/negative trip_distance
  - [x] negative fare, tip, total
  - [x] zero/missing passenger_count
  - [x] non-positive duration
  - [x] `trip_distance > 100 miles`
  - [x] `fare_amount > 500`
  - [x] `passenger_count > 6`
  - [x] `tip_amount > fare_amount`
  - [x] `total_amount < fare_amount`
  - [x] `trip_duration < 60 seconds` floor
  - [x] `trip_duration > 24 hours` ceiling
  - [x] coordinates outside NYC bounds (lat 40.5–40.9, lon -74.3 to -73.7)
- [x] Per-rule filter stats tracked and logged (`FilterRuleStat`)
- [x] Flag outliers: `is_tip_outlier` (tip >50%), `is_speed_outlier` (>80 mph), `is_unusual_midnight_trip` (hour 0-4), `is_trip_distance_outlier` (>99th pct), `is_fare_outlier` (>99th pct)
  - Note: spec says `is_rush_hour` and `is_unusual_midnight_trip` apply "on weekdays" — code has no weekday exclusion; document deviation in report
- [x] Feature engineering (trip_duration_minutes, trip_speed_mph, hour_of_day, day_of_week, is_weekend, is_rush_hour, tip_percentage, cost_per_mile)
- [x] IQR outlier detection + cap trip_duration at 95th percentile
- [x] Partition output by year/month for efficient querying

### 4.2 Stream Processing (Real-Time Data)

- [x] Event envelope parsing (extract metadata, validate schema, reject to DLQ)
- [x] Watermarking (5-minute delay, late event detection)
- [x] State-keyed deduplication (remove duplicates within sliding window)
- [x] Out-of-order event detection (timestamp < previous)
- [x] Windowed aggregations (5-minute tumbling windows, counts per subject/source)
- [x] Produce aggregated metrics tables (`stream_metrics_5m`)

### 4.3 Processing Orchestration

- [x] Airflow DAG for batch processing pipeline (`batch_pipeline`, 5 tasks, daily schedule)
- [x] Airflow DAG for daily ML prediction refresh (`daily_prediction`, 3 tasks, 06:00 UTC)
- [x] Schedule batch jobs (catchup=False, max_active_runs=1)
- [x] on_failure_callback on all DAG tasks

### 4.4 Deliverables

- [x] Processing Report (`docs/processing-report.md`) — batch summary with real row counts, per-rule filter breakdown, stream processing summary, windowed aggregation sample, out-of-order analysis, Gold layer output table, DAG topology

---

## 5. Storage (Medallion Architecture)

### 5.1 Bronze Layer

- [x] Raw TLC Yellow Taxi Parquet files (Jan 2023 - Nov 2025, 35 files)
- [x] Raw Events/311 NDJSON snapshots
- [x] Immutable, append-only storage
- [x] `_ingestion_timestamp` on all files (NDJSON rows + `.meta.json` for Parquet)
- [x] Backfilled existing TLC sidecars via `clm backfill-bronze-meta`
- [x] Schema drift detection (`BronzeValidator`, column-removal-only policy)
- [x] `make validate-bronze` — 0 issues across 62 files

### 5.2 Silver Layer

- [x] Cleaned TLC Yellow Taxi trips (103M rows, 83.4% retention)
- [x] Normalized permitted events (write-time dedup by event_id)
- [x] Deduplicated noise complaints (NYC bounds filter, null coords kept)

### 5.3 Gold Layer

- [x] `taxi_demand_by_zone` (39,198 rows: 265 zones x 168 hour/day combos)
- [x] `event_schedule` (35,209 unique events)
- [x] `noise_by_borough_hour` (11,570 rows: borough x type x descriptor x hour x day)
- [x] `stream_metrics_5m` (256 rows from one Kafka run)
- [x] Star schema: `dim_zone` (265), `dim_time` (168), `fact_taxi_demand` (39,198)
- [x] Gold tables mirrored to MinIO (`MINIO_ENABLED=true`)
- [-] ML Gold tables (`ml_training/ml_dataset.parquet`, `activity_prediction.parquet`) — code is ready (`clm build-ml-dataset`, `clm train-model`, `clm predict-activity`) but files are not committed; generated at runtime

### 5.4 Deliverables

- [x] Storage Report (`docs/storage-report.md`) — medallion architecture, lakehouse implementation, data dictionary, data consistency across 3 layers, query performance

---

## 6. Serving & Visualization

- [x] Dashboard 1: Late-Night Taxi (4 charts: Late-Night Taxi Heatmap, Top 10 Late-Night Zones, Trips by Hour Weekend vs Weekday, Avg Fare by Borough x Hour)
- [x] Dashboard 2: Events (4 charts: Events by Borough, Events by Type, Events per Day, Upcoming Nightlife Events)
- [x] Dashboard 3: Noise (4 charts: Noise by Borough x Hour, Noise by Hour Weekend vs Weekday, Noise Descriptor by Borough, Top Noise Complaint Types)
- [x] Dashboard 4: Batch + Streaming Combined (3 charts: Trips Events & Noise by Hour, Streaming Volume by Source & Type, Streaming Quality by Source)
- [x] Dashboard 5: Prediction (4 charts: 7-Day Activity Forecast, Forecast Detail, Activity Trend Last 14 Days + Forecast, Today's Activity Confidence)
- [x] All dashboards query DuckDB views over Gold Parquet directly
- [x] ML pipeline: `classifier.py` (HistGradientBoosting, 12 features, TimeSeriesSplit CV), `predict.py` (7-day forward forecast), `ml_dataset_builder.py`

### 6.3 Deliverables

- [x] Description and screenshots of each dashboard and all charts (for project report / submission)
- [-] Verify Superset export (`make superset-export`) is current; confirm dashboards load cleanly after fresh `docker compose up`

---

## 7. Final Deliverables

- [x] Code Repository: modular, runnable — README.md complete, all feature branches merged to master
- [x] Architecture & Contracts: architecture diagram in project-report.md + data contracts written (`docs/data-contracts.md`) for both real-time sources
- [x] Populated Storage: demonstrable Bronze (62 files), Silver (279+ files), Gold (7 tables + star schema), MinIO mirror
- [-] Dashboards: 5 dashboards working locally — screenshots documented; Superset export freshness still pending
- [x] Project Report (`docs/project-report.md`) — merged to master; covers full orchestrated system
- [ ] Final Presentation

---

## Known Deviations from Spec (document in report)

- **is_rush_hour**: spec says "7-9am, 5-7pm on weekdays" — code uses `isin([7,8,9,16,17,18,19])` with no weekday exclusion. Documented in project report.
- **is_unusual_midnight_trip**: spec implies "weekdays" — code uses `hour_of_day.between(0, 4)` with no day filter.
- **Exactly-once semantics**: spec says "implement where possible" — implemented as at-least-once + idempotent Silver dedup. Documented in project report.
- **Missing upper-bound quality filters**: trip_distance >100, fare >500, passenger_count >6, tip>fare, total<fare, duration <60s or >24h — not implemented. Decide: implement or document as deliberate choice.
