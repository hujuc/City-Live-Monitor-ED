# Assignment 2 — TODO

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
- [x] Exactly-once semantics (deterministic event IDs + dedup window)
- [x] Circuit breaker for API failures
- [x] Pagination for large backfills (>2000 rows per poll)
- [x] Inject `_ingestion_timestamp` into every NDJSON row on write

### 3.3 Data Profiling

- [x] Profile ALL ingested data sources (batch + streaming samples)
- [x] Generate comprehensive data quality report
- [x] Identify and document data issues

### 3.4 Infrastructure Setup

- [x] Docker Compose with all services (Kafka, MinIO, Airflow, Superset, Backend)
- [x] Version control with Git
- [x] Environment-based configuration (.env)
- [x] Pre-commit hooks (ruff, formatting, whitespace)

### 3.5 Deliverables

- [x] Data Profiling Report (executive summary, schema docs, statistical profiles, data quality issues, cleaning recommendations)
- [ ] README.md (prerequisites, quick start, services, architecture diagram)
- [x] Code repository (.gitignore, meaningful commits, branching strategy)

---

## 4. Stream & Batch Processing Layer

### 4.1 Batch Processing (NYC Taxi Data)

- [x] Clean and standardize 6+ months of taxi trip data
- [x] Data type casting (timestamps to UTC, coordinates to DECIMAL, money to DECIMAL)
- [x] Standardization rules (borough names, zone names, payment types)
- [x] Data quality filtering (remove invalid: zero distance, negative fare, bad coordinates, etc.)
- [x] Per-rule filter stats tracked and logged (`FilterRuleStat`, `_apply_quality_filters`)
- [x] Flag outliers (tip >50%, speed >80mph, unusual midnight-4am trips)
- [x] Feature engineering (trip_duration_minutes, trip_speed_mph, hour_of_day, day_of_week, is_weekend, is_rush_hour, tip_percentage, cost_per_mile)
- [x] Outlier detection (IQR/Z-score, cap trip_duration at 95th percentile)
- [x] Partition output by date for efficient querying

### 4.2 Stream Processing (Real-Time Data)

- [x] Event envelope parsing (extract metadata, validate schema, reject to DLQ)
- [x] Watermarking (5-minute delay, late event detection)
- [x] State-keyed deduplication (remove duplicates within window)
- [x] Out-of-order event detection (timestamp < previous)
- [x] Windowed aggregations (counts, averages per subject per time window)
- [x] Produce aggregated metrics tables

### 4.3 Processing Orchestration

- [x] Airflow DAG for batch processing pipeline
- [x] Schedule batch jobs appropriately
- [x] Monitor processing health (`on_failure_callback` em todos os operadores do DAG)

### 4.4 Deliverables

- [-] Processing Report — filtering breakdown real implementado; stream metrics (throughput, latência, late %) requerem re-geração após Kafka run

---

## 5. Storage (Medallion Architecture)

### 5.1 Bronze Layer

- [x] Raw TLC Parquet files (Jan 2023 - Nov 2025)
- [x] Raw Events/311 NDJSON snapshots
- [x] Immutable, append-only storage
- [x] `_ingestion_timestamp` em novas ingestões (NDJSON rows + `.meta.json` para Parquet)
- [x] `_ingestion_timestamp` nos 33 ficheiros TLC já existentes (sidecars gerados retroativamente via `clm backfill-bronze-meta`)
- [x] Schema drift detection (`BronzeValidator`)
- [x] `make validate-bronze` / `clm validate-bronze` — valida as 3 fontes

### 5.2 Silver Layer

- [x] Cleaned taxi trips (midnight-4am filter, quality checks applied)
- [x] Normalized permitted events (write-time dedup by event_id across snapshots)
- [x] Deduplicated noise complaints with lat/lon (NYC bounds filter, null coords kept)
- [x] New columns, removed columns, data transformations applied

### 5.3 Gold Layer

- [x] Taxi demand by zone x hour x day-of-week
- [x] Event schedule with zone mapping (borough-level)
- [x] Noise complaint aggregations (borough x complaint_type x descriptor x hour x day)
- [x] Streaming: 5-minute rolling window metrics tables (`stream_metrics_5m`)
- [x] Gold tables uploaded to MinIO (`GoldStorage.write()` com `MINIO_ENABLED=true`)
- [x] Dimensional model (star schema formal) — dim_zone (265 rows), dim_time (168 rows), fact_taxi_demand (39198 rows)
- [x] `stream_metrics_5m` populada com dados reais (256 rows via `clm stream-aggregates`)

### 5.4 Deliverables

- [ ] Storage Report (medallion architecture, lakehouse implementation, data dictionary, data consistency across 3 layers, query performance testing)

---

## 6. Serving & Visualization

- [x] 3 Superset dashboards, each with minimum 4 charts:
  - [x] Dashboard 1: Late-night taxi heatmap
  - [x] Dashboard 2: Tonight's events map
  - [x] Dashboard 3: Live noise complaint feed
- [x] Query combined batch + streaming outputs — Dashboard 4: "Batch + Streaming Combined" (trips + events + noise by hour, streaming volume by source, quality table)

### 6.3 Deliverables

- [ ] Description and screenshots of each dashboard and charts

---

## 7. Final Deliverables

- [ ] Code Repository: modular, documented, runnable, README.md with setup instructions
- [ ] Architecture & Contracts: 1-page architecture diagram + data contracts per real-time source
- [x] Populated Storage: demonstrable Bronze, Silver, Gold tables — Gold também em MinIO
- [ ] Dashboards: 3 working Superset dashboards combining batch + streaming metrics
- [ ] Project Report: design decisions, trade-offs, failure modes, limitations
- [ ] Final Presentation: demo of complete orchestrated system + challenges encountered
