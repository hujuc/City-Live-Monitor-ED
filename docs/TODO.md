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

### 3.2 Streaming Data Ingestion

- [x] Poll Socrata APIs on scheduled interval (311: 30s, Events: 30min)
- [x] Emit events to Kafka topics
- [x] Exactly-once semantics (deterministic event IDs + dedup window)
- [x] Circuit breaker for API failures
- [x] Pagination for large backfills (>2000 rows per poll)

### 3.3 Data Profiling

- [ ] Profile ALL ingested data sources (batch + streaming samples)
- [ ] Generate comprehensive data quality report
- [ ] Identify and document data issues

### 3.4 Infrastructure Setup

- [x] Docker Compose with all services (Kafka, MinIO, Airflow, Superset, Backend)
- [x] Version control with Git
- [x] Environment-based configuration (.env)
- [x] Pre-commit hooks (ruff, formatting, whitespace)

### 3.5 Deliverables

- [ ] Data Profiling Report (executive summary, schema docs, statistical profiles, data quality issues, cleaning recommendations)
- [ ] README.md (prerequisites, quick start, services, architecture diagram)
- [x] Code repository (.gitignore, meaningful commits, branching strategy)

---

## 4. Stream & Batch Processing Layer

### 4.1 Batch Processing (NYC Taxi Data)

- [x] Clean and standardize 6+ months of taxi trip data
- [x] Data type casting (timestamps to UTC, coordinates to DECIMAL, money to DECIMAL)
- [x] Standardization rules (borough names, zone names, payment types)
- [x] Data quality filtering (remove invalid: zero distance, negative fare, bad coordinates, etc.)
- [x] Flag outliers (tip >50%, speed >80mph, unusual midnight-4am trips)
- [x] Feature engineering (trip_duration_minutes, trip_speed_mph, hour_of_day, day_of_week, is_weekend, is_rush_hour, tip_percentage, cost_per_mile)
- [x] Outlier detection (IQR/Z-score, cap trip_duration at 95th percentile)
- [x] Partition output by date for efficient querying

### 4.2 Stream Processing (Real-Time Data)

- [x] Event envelope parsing (extract metadata, validate schema, reject to DLQ)
- [x] Watermarking (5-minute delay, late event detection)
- [x] State-keyed deduplication (remove duplicates within window)
- [x] Out-of-order event detection (timestamp < previous)
- [ ] Windowed aggregations (counts, averages per subject per time window)
- [ ] Produce aggregated metrics tables

### 4.3 Processing Orchestration

- [ ] Airflow DAG for batch processing pipeline
- [ ] Schedule batch jobs appropriately
- [ ] Monitor processing health

### 4.4 Deliverables

- [ ] Processing Report (batch summary, filtering breakdown, stream processing summary, windowed aggregations, out-of-order analysis)

---

## 5. Storage (Medallion Architecture)

### 5.1 Bronze Layer

- [x] Raw TLC Parquet files (Jan 2023 - Nov 2025)
- [x] Raw Events/311 NDJSON snapshots
- [x] Immutable, append-only storage
- [ ] Validation: row counts match source, no schema drift, _ingestion_timestamp on all partitions

### 5.2 Silver Layer

- [x] Cleaned taxi trips (midnight-4am filter, quality checks applied)
- [x] Normalized permitted events (write-time dedup by event_id across snapshots)
- [x] Deduplicated noise complaints with lat/lon (NYC bounds filter, null coords kept)
- [x] New columns, removed columns, data transformations applied

### 5.3 Gold Layer

- [ ] Dimensional model (star schema) for batch data loaded into lakehouse (MinIO)
- [ ] Taxi demand by zone x hour x day-of-week
- [ ] Event schedule with zone mapping
- [ ] Noise complaint aggregations by zone x hour
- [ ] Streaming: 5-minute rolling window metrics tables

### 5.4 Deliverables

- [ ] Storage Report (medallion architecture, lakehouse implementation, data dictionary, data consistency across 3 layers, query performance testing)

---

## 6. Serving & Visualization

- [x] 3 Superset dashboards, each with minimum 4 charts:
  - [x] Dashboard 1: Late-night taxi heatmap (historical dropoff density by zone, filterable by hour/day)
  - [x] Dashboard 2: Tonight's events map (active permitted events overlaid on taxi demand zones)
  - [x] Dashboard 3: Live noise complaint feed (rolling 30-min complaint counts by borough)
- [ ] Query combined batch + streaming outputs to show trends/correlations

### 6.3 Deliverables

- [ ] Description and screenshots of each dashboard and charts

---

## 7. Final Deliverables

- [ ] Code Repository: modular, documented, runnable, README.md with setup instructions
- [ ] Architecture & Contracts: 1-page architecture diagram + data contracts per real-time source
- [ ] Populated Storage: demonstrable Bronze, Silver, Gold tables (including star schema)
- [ ] Dashboards: 3 working Superset dashboards combining batch + streaming metrics
- [ ] Project Report: design decisions, trade-offs, failure modes, limitations
- [ ] Final Presentation: demo of complete orchestrated system + challenges encountered
