[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=23275115&assignment_repo_type=AssignmentRepo)

# City Live Monitor

End-to-end hybrid data platform combining batch and streaming pipelines for NYC urban analytics. Ingests NYC Yellow Taxi trip data (batch) and real-time 311 noise complaints and permitted event feeds (streaming), processes them through a Bronze → Silver → Gold medallion architecture, and exposes the results through Apache Superset dashboards and a REST API.

**Authors:** Gabriel Silva · Hugo Castro — University of Aveiro, 2026

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Sources                                                            │
│  NYC TLC Parquet (Jan 2023–Nov 2025)   NYC Open Data Socrata APIs   │
└────────────────┬──────────────────────────────┬────────────────────┘
                 │ batch download                │ HTTP poll (30s / 30min)
                 ▼                               ▼
┌─────────────── Bronze ──────────────────────────────────────────────┐
│  data/bronze/tlc/yellow/YYYY/*.parquet   (.meta.json sidecars)      │
│  data/bronze/nyc_permitted_events/YYYY/MM/*.ndjson                  │
│  data/bronze/nyc_311_noise/YYYY/MM/*.ndjson                         │
└────────────────┬──────────────────────────────┬────────────────────┘
                 │ TLCBatchProcessor             │ Kafka (events.raw,
                 │ PermittedEventsSilver         │  noise311.raw)
                 │ Noise311Silver                │    ▼
                 ▼                         KafkaAggregateRunner
┌─────────────── Silver ──────────────────────────────────────────────┐
│  data/silver/tlc/yellow/YYYY-MM/*.parquet  (cleaned, typed, enriched)│
│  data/silver/events/*.parquet              (deduped by event_id)    │
│  data/silver/noise311/*.parquet            (geocoded, filtered)     │
└────────────────────────────┬────────────────────────────────────────┘
                             │ Gold processors
                             ▼
┌─────────────── Gold ────────────────────────────────────────────────┐
│  taxi_demand_by_zone   event_schedule   noise_by_borough_hour       │
│  stream_metrics_5m     dim_zone   dim_time   fact_taxi_demand       │
│  (mirrored to MinIO s3://gold-layer/ when MINIO_ENABLED=true)       │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ DuckDB read_parquet() views
                                ▼
┌─────────────── Serving ─────────────────────────────────────────────┐
│  Apache Superset  (4 dashboards)       FastAPI REST  :8000          │
│  Airflow DAG      (daily batch)        MinIO console :9001          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Tool | Minimum version |
|------|----------------|
| Python | 3.12 |
| [uv](https://docs.astral.sh/uv/) | 0.5+ |
| Docker + Docker Compose | 24+ |
| Git | any |

---

## Quick Start

### 1. Clone and set up the environment

```bash
git clone <repo-url>
cd assignment-2-pair-work-ed2026_g04
make setup          # creates .venv and installs all dependencies
cp .env.example .env
```

Edit `.env` if needed (defaults work for local development).

### 2. Run the full pipeline (local, no Docker needed)

```bash
# Ingest TLC batch data (downloads Parquet files from NYC TLC)
# Note: files are already in data/bronze/tlc/ — skip if present

# Process Bronze → Silver → Gold
make pipeline       # silver-all + gold-build

# Validate the Bronze layer
make validate-bronze

# Show a summary of all layers
uv run clm summary
```

### 3. Start streaming ingestion (requires Kafka)

```bash
make kafka-wait                # start Kafka + wait until ready
make stream-events             # poll NYC Permitted Events → Kafka (once)
make stream-noise311           # poll 311 Noise → Kafka (once)
make stream-aggregates         # consume Kafka → stream_metrics_5m Gold table
```

### 4. Start dashboards

```bash
make superset-up               # start Superset + its PostgreSQL DB
make superset-bootstrap        # register DuckDB views + import dashboards
# Open http://localhost:8088  (admin / admin)
```

### 5. Start the full platform (all services)

```bash
make docker-up                 # starts Kafka, MinIO, Airflow, Superset, Backend
make superset-bootstrap        # register dashboards (first time only)
make health                    # check all services
```

---

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Superset dashboards | http://localhost:8088 | admin / admin |
| Airflow scheduler | http://localhost:8080 | admin / admin |
| FastAPI REST API | http://localhost:8000 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka broker | localhost:9092 | — |

---

## CLI Reference (`clm`)

```bash
uv run clm --help
```

| Command | Description |
|---------|-------------|
| `clm summary` | Bronze/Silver/Gold layer sizes and row counts |
| `clm silver-process-tlc` | TLC Bronze → Silver (cleaning, feature engineering) |
| `clm silver-process-events` | Events Bronze → Silver (dedup by event_id) |
| `clm silver-process-noise311` | 311 Noise Bronze → Silver (geocoding filter) |
| `clm gold-build-all` | Build all Gold tables including star schema |
| `clm gold-build-star-schema` | Build dim_zone, dim_time, fact_taxi_demand |
| `clm stream-events` | Poll NYC Permitted Events → Kafka (once) |
| `clm stream-noise311` | Poll 311 Noise → Kafka (once) |
| `clm stream-aggregates` | Kafka → 5-min window metrics → Gold |
| `clm validate-bronze` | Check _ingestion_timestamp + schema drift across all sources |
| `clm backfill-bronze-meta` | Create .meta.json sidecars for existing TLC Parquet files |
| `clm report-data-profiling` | Generate docs/data-profiling-report.md |
| `clm report-processing` | Generate docs/processing-report.md |
| `clm api` | Start FastAPI REST server |

---

## Make Targets

| Target | What it does |
|--------|-------------|
| `make setup` | Create venv and install dependencies |
| `make pipeline` | Bronze → Silver → Gold (full rebuild) |
| `make silver-all` | Process all three Silver sources |
| `make gold-build` | Build all Gold tables (flat + star schema) |
| `make gold-build-star-schema` | Build dimensional model only |
| `make ingest-streaming` | Poll Events + 311 Noise into Kafka (once) |
| `make stream-aggregates` | Kafka → stream_metrics_5m |
| `make validate-bronze` | Validate Bronze layer integrity |
| `make docker-up` | Start all Docker services |
| `make superset-up` | Start Superset only |
| `make superset-bootstrap` | Register DuckDB views + import dashboards |
| `make airflow-up` | Start Airflow only |
| `make kafka-wait` | Start Kafka and wait for readiness |
| `make health` | Check API, Superset, Airflow, MinIO |
| `make summary` | Print Bronze/Silver/Gold summary table |
| `make test` | Run test suite |
| `make lint` | Run ruff + black |
| `make clean-silver-gold` | Wipe Silver + Gold (Bronze preserved) |

---

## Data Sources

| Source | Type | Volume | Schedule |
|--------|------|--------|----------|
| [NYC TLC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Batch Parquet | 35 files, ~124 M rows, Jan 2023–Nov 2025 | Manual / Airflow daily |
| [NYC Permitted Events](https://data.cityofnewyork.us/resource/tvpp-9vvx.json) | Streaming (Socrata) | ~200 K rows | Every 30 minutes |
| [311 Noise Complaints](https://data.cityofnewyork.us/resource/erm2-nwe9.json) | Streaming (Socrata) | ~100 K rows | Every 30 seconds |

A free [Socrata app token](https://data.cityofnewyork.us/profile/app_tokens) can be added to `.env` as `SOCRATA_APP_TOKEN` to increase API rate limits. The pollers work without it.

---

## Storage Layout (Medallion Architecture)

```
data/
├── bronze/
│   ├── tlc/yellow/YYYY/                 # Raw TLC Parquet + .meta.json sidecars
│   ├── nyc_permitted_events/YYYY/MM/    # NDJSON snapshots with _ingestion_timestamp
│   └── nyc_311_noise/YYYY/MM/          # NDJSON snapshots with _ingestion_timestamp
├── silver/
│   ├── tlc/yellow/YYYY-MM/             # Cleaned + typed + feature-engineered
│   ├── events/                          # Deduplicated by event_id
│   └── noise311/                        # Geocoded + filtered NYC bounds
├── gold/
│   ├── taxi_demand_by_zone/            # Trips × zone × hour × day-of-week
│   ├── event_schedule/                  # Events with borough mapping
│   ├── noise_by_borough_hour/          # Noise aggregations × type × hour
│   ├── stream_metrics_5m/              # Kafka 5-min windowed metrics
│   ├── dim_zone/                        # Star schema: 265 NYC taxi zones
│   ├── dim_time/                        # Star schema: 168 hour × day combinations
│   └── fact_taxi_demand/               # Star schema: 39 198 fact rows
└── checkpoints/                         # Streaming cursor state
```

Gold tables are also mirrored to MinIO (`s3://gold-layer/`) when `MINIO_ENABLED=true`.

---

## Dashboards (Superset)

| Dashboard | Charts | Dataset |
|-----------|--------|---------|
| Late-Night Taxi | Heatmap (zone × hour), Top 10 zones, Trips by hour (weekday vs weekend), Avg fare by borough | `taxi_demand` |
| Events | Events by borough, Events by type, Events per day, Upcoming nightlife events | `events` |
| Noise | Noise by borough × hour, Noise by hour (weekday vs weekend), Descriptor by borough, Top complaint types | `noise` |
| Batch + Streaming Combined | Trips + events + noise by hour, Streaming volume by source & type, Streaming quality table | `batch_streaming_combined`, `stream_metrics_5m` |

---

## Development

```bash
make test          # pytest (81 tests, ~2s without coverage)
make lint          # ruff check + black format
make typecheck     # pyright type checking
```

### Project structure

```
src/
├── backend/
│   ├── api/              # FastAPI REST endpoints
│   ├── airflow/dags/     # batch_pipeline.py (Airflow DAG)
│   ├── cli/              # clm command-line interface
│   ├── ingestion/
│   │   ├── batch/        # TLC Parquet downloader
│   │   └── streaming/    # Socrata pollers + Kafka producer
│   ├── processing/
│   │   ├── batch/        # TLC, Events, Noise silver + gold processors
│   │   └── streaming/    # EventProcessor (watermark/dedup) + KafkaAggregateRunner
│   ├── reporting/        # Data profiling + processing report generators
│   ├── storage/          # GoldStorage (local + MinIO)
│   └── utils/            # BronzeValidator, MinIO client, logging
└── config/
    └── settings.py       # Centralised configuration (reads .env)
```
