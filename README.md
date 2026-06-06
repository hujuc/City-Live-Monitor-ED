[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=23275115&assignment_repo_type=AssignmentRepo)

# City Live Monitor

End-to-end hybrid data platform for NYC urban analytics. It combines batch ingestion of NYC Yellow Taxi Parquet files with streaming ingestion of NYC Open Data feeds for 311 noise complaints and permitted events, processes the data through a Bronze → Silver → Gold medallion architecture, and exposes the results through a REST API, Apache Superset dashboards, and operational monitoring.

**Authors:** Gabriel Silva · Hugo Castro — University of Aveiro, 2026

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Sources                                                                     │
│ NYC TLC Parquet (batch)     NYC Open Data Socrata APIs (streaming)          │
└────────────────┬───────────────────────────────────────┬────────────────────┘
                 │ batch download                        │ HTTP pollers
                 ▼                                       ▼
┌──────────────────────────── Bronze ─────────────────────────────────────────┐
│ data/bronze/tlc/yellow/YYYY/*.parquet (.meta.json sidecars)                 │
│ data/bronze/nyc_permitted_events/YYYY/MM/*.ndjson                           │
│ data/bronze/nyc_311_noise/YYYY/MM/*.ndjson                                  │
└────────────────┬───────────────────────────────┬────────────────────────────┘
                 │ Silver processors             │ Kafka topics
                 │ TLCBatchProcessor             │ events.raw / noise311.raw
                 │ PermittedEventsSilverProcessor│          ▼
                 │ Noise311SilverProcessor       │ KafkaAggregateRunner
                 ▼                               ▼
┌──────────────────────────── Silver ─────────────────────────────────────────┐
│ data/silver/tlc/yellow/year=YYYY/month=MM/*.parquet                         │
│ data/silver/events/year=YYYY/month=MM/events_silver.parquet                 │
│ data/silver/noise311/year=YYYY/month=MM/*.parquet                           │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │ Gold processors
                                ▼
┌────────────────────────────── Gold ─────────────────────────────────────────┐
│ taxi_demand_by_zone   event_schedule   noise_by_borough_hour                │
│ stream_metrics_5m     dim_zone        dim_time      fact_taxi_demand        │
│ Gold files can be mirrored to MinIO when MINIO_ENABLED=true                 │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │ DuckDB / Superset / API
                                ▼
┌────────────────────────────── Serving ───────────────────────────────────────┐
│ FastAPI REST API   Apache Superset   Airflow   MinIO   Prometheus   Grafana  │
└──────────────────────────────────────────────────────────────────────────────┘
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

### 2. Run the batch pipeline locally

```bash
make pipeline       # silver-all + gold-build

make validate-bronze

uv run clm summary
```

### 3. Start streaming ingestion and aggregation

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
make docker-up                 # starts Kafka, MinIO, Airflow, Superset, Backend, monitoring stack
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
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / `GRAFANA_PASSWORD` |

---

## CLI Reference (`clm`)

```bash
uv run clm --help
```

| Command | Description |
|---------|-------------|
| `clm api` | Start the FastAPI REST server |
| `clm stream-events` | Poll NYC Permitted Events → Kafka |
| `clm stream-noise311` | Poll 311 Noise → Kafka |
| `clm stream-aggregates` | Consume Kafka → 5-minute Gold metrics |
| `clm summary` | Bronze/Silver/Gold layer sizes and row counts |
| `clm silver-process-tlc` | TLC Bronze → Silver (cleaning, feature engineering) |
| `clm silver-process-events` | Events Bronze → Silver (dedup by event_id) |
| `clm silver-process-noise311` | 311 Noise Bronze → Silver (geocoding filter) |
| `clm gold-build-all` | Build all Gold tables including star schema |
| `clm gold-build-star-schema` | Build dim_zone, dim_time, fact_taxi_demand |
| `clm validate-bronze` | Check _ingestion_timestamp + schema drift across all sources |
| `clm backfill-bronze-meta` | Create .meta.json sidecars for existing TLC Parquet files |
| `clm backfill-ndjson-meta` | Backfill ingestion metadata for NDJSON bronze files |
| `clm report-data-profiling` | Generate docs/data-profiling-report.md |
| `clm report-processing` | Generate docs/processing-report.md |
| `clm backfill-noise311-daily-counts` | Build daily noise count history for ML |
| `clm backfill-events-daily-counts` | Build daily event count history for ML |
| `clm train-model` | Build ML dataset from Silver and train HistGradientBoosting classifier |
| `clm predict-activity` | Generate daily predictions and 7-day forecast from the trained model |
| `clm backfill-noise311-daily-counts` | Fetch historical daily noise311 complaint counts from Socrata for ML training |
| `clm backfill-events-daily-counts` | Fetch historical daily event counts from Socrata for ML training |

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
| `make summary` | Print Bronze/Silver/Gold summary table |
| `make reports` | Regenerate profiling and processing reports |
| `make backfill-bronze-meta` | Create missing TLC `.meta.json` sidecars |
| `make backfill-ndjson-meta` | Backfill metadata for NDJSON bronze files |
| `make train-model` | Train ML model and rebuild dataset from Silver |
| `make predict` | Generate daily predictions and 7-day forecast |
| `make backfill-noise-counts` | Fetch historical noise311 daily counts from Socrata |
| `make backfill-events-counts` | Fetch historical event daily counts from Socrata |
| `make docker-up` | Start all Docker services |
| `make superset-up` | Start Superset only |
| `make superset-bootstrap` | Register DuckDB views + import dashboards |
| `make airflow-up` | Start Airflow only |
| `make kafka-wait` | Start Kafka and wait for readiness |
| `make health` | Check API, Superset, Airflow, MinIO |
| `make test` | Run test suite |
| `make lint` | Run ruff + black |
| `make clean-silver-gold` | Wipe Silver + Gold (Bronze preserved) |

---

## Data Sources

| Source | Type | Volume | Schedule |
|--------|------|--------|----------|
| [NYC TLC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Batch Parquet | Jan 2023–Nov 2025 files in Bronze | Manual / Airflow daily |
| [NYC Permitted Events](https://data.cityofnewyork.us/resource/tvpp-9vvx.json) | Streaming (Socrata) | Polled into Bronze and Kafka | Every 30 minutes |
| [311 Noise Complaints](https://data.cityofnewyork.us/resource/erm2-nwe9.json) | Streaming (Socrata) | Polled into Bronze and Kafka | Every 30 seconds |

A free [Socrata app token](https://data.cityofnewyork.us/profile/app_tokens) can be added to `.env` as `SOCRATA_APP_TOKEN` to increase API rate limits. The pollers work without it.

---

## Storage Layout (Medallion Architecture)

```
data/
├── bronze/
│   ├── tlc/yellow/YYYY/                 # Raw TLC Parquet + .meta.json sidecars
│   ├── nyc_permitted_events/YYYY/MM/    # NDJSON snapshots with _ingestion_timestamp
│   └── nyc_311_noise/YYYY/MM/           # NDJSON snapshots with _ingestion_timestamp
├── silver/
│   ├── tlc/yellow/year=YYYY/month=MM/   # Cleaned + typed + feature-engineered
│   ├── events/year=YYYY/month=MM/       # Deduplicated by event_id
│   └── noise311/year=YYYY/month=MM/     # Geocoded + filtered NYC bounds
├── gold/
│   ├── taxi_demand_by_zone/            # Trips × zone × hour × day-of-week
│   ├── event_schedule/                  # Events with borough mapping
│   ├── noise_by_borough_hour/          # Noise aggregations × type × hour
│   ├── stream_metrics_5m/              # Kafka 5-min windowed metrics
│   ├── dim_zone/                        # Star schema: 265 NYC taxi zones
│   ├── dim_time/                        # Star schema: 168 hour × day combinations
│   ├── fact_taxi_demand/               # Star schema: 39 198 fact rows
│   ├── ml_training/                    # ML dataset + trained classifier
│   └── ml_prediction/                  # Daily predictions + 7-day forecast
└── checkpoints/                         # Streaming cursor state
```

Gold tables are also mirrored to MinIO (`s3://gold-layer/`) when `MINIO_ENABLED=true`.

The Silver batch processors are deterministic but not incremental in the sense of skipping previously processed Bronze files. They re-read Bronze and overwrite the canonical Silver partitions, so `make pipeline` intentionally rebuilds Silver and Gold from the current Bronze state.

---

## Dashboards (Superset)

| Dashboard | Purpose | Key Charts |
|-----------|---------|-----------|
| Late-Night Taxi | Taxi demand patterns during late-night hours | Zone heatmap (trips × hour), Top 10 zones, Trips by hour (weekday vs weekend), Avg fare by borough |
| Tonight's Events | Permitted events schedule and locations | Events by borough, Events by type, Events per day, Upcoming nightlife events |
| Live Noise Complaints | Noise complaint patterns and hotspots | Noise by borough × hour, Noise by hour (weekday vs weekend), Descriptors by borough, Top complaint types |
| Batch + Streaming Combined | Hybrid view of batch and streaming data | Trips + events + noise by hour, Streaming volume by source, Streaming quality metrics |
| Prediction | Daily activity forecast and trends | Current-day prediction, 7-day forecast, Recent forecast accuracy comparison |

Five dashboards query DuckDB views that read Parquet files directly from Gold. The bootstrap is scripted: `make superset-bootstrap` registers the views and imports assets from `infra/superset/assets.zip`.

---

## Machine Learning

The platform includes a daily activity prediction pipeline that forecasts high-activity days using taxi demand, noise complaints, and permitted events. The ML pipeline is automated via Airflow but can also be triggered manually.

**To refresh ML data:**

1. Ensure Silver layer is up-to-date (run `make pipeline`)
2. (Optional) Backfill historical event and noise counts from Socrata:
   ```bash
   make backfill-noise-counts
   make backfill-events-counts
   ```
3. Build ML dataset and train the classifier:
   ```bash
   make train-model
   ```
4. Generate daily predictions and 7-day forecast:
   ```bash
   make predict
   ```

Outputs are written to `data/gold/ml_training/` and `data/gold/ml_prediction/` and automatically consumed by the Superset Prediction dashboard.

---

## Documentation

Project documentation is maintained in `docs/` and includes:

| Document | Purpose |
|----------|--------|
| [project-report.md](docs/project-report.md) | Complete technical report covering architecture, ingestion, processing, serving, results, and design decisions |
| [data-profiling-report.md](docs/data-profiling-report.md) | Statistical profiles of Bronze layer sample files (schema, nulls, value distributions) |
| [processing-report.md](docs/processing-report.md) | Row counts and retention metrics across Bronze → Silver → Gold transformations |
| [project-plan.md](docs/project-plan.md) | Original project roadmap and milestones |

Documentation is auto-generated via:
- `make report-data-profiling` — samples Bronze files and produces stats
- `make report-processing` — reads current pipeline artifacts and computes retention

To regenerate:
```bash
make reports
```

---

## Development

```bash
make test          # pytest
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
│   ├── storage/          # Bronze/Silver/Gold storage helpers
│   └── utils/            # BronzeValidator, MinIO client, logging
└── config/
    └── settings.py       # Centralised configuration (reads .env)
```

