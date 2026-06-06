# City Live Monitor - Project Plan

**Course:** Data Engineering 2025/2026
**Authors:** Gabriel Silva, Hugo Castro

---

## Idea: NYC Nightlife Pulse

> Where is the city going at 2am - and can we predict it?

Use historical taxi trips to model late-night mobility patterns, cross them with
permitted events happening tonight, and watch noise complaints light up in real
time to validate the prediction.

---

## Data Sources

### 1. NYC TLC Yellow Taxi Trips - Batch

| Property | Value |
| --- | --- |
| Type | Batch / Static |
| Format | Parquet |
| Provider | NYC Taxi & Limousine Commission |
| URL | `https://d37ci6vzurychx.cloudfront.net/trip-data/` |
| Available | Jan 2019 – Nov 2025 (full history) |
| Planned download | 2023 full year + 2024 full year + Jan–Nov 2025 (~2.3 GB) |
| Downloaded | Jan–Jun 2024 (6 files, ~335 MB) |
| Key fields | `pickup_datetime`, `dropoff_datetime`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, `trip_distance` |

**Role:** Historical baseline. Filter to midnight–4am trips to build a model of
where taxis go during nightlife hours, by borough and taxi zone.

---

### 2. NYC Permitted Events - Periodic

| Property | Value |
| --- | --- |
| Type | Real-time Periodic |
| Format | JSON (Socrata REST API) |
| Provider | NYC Parks Department via NYC Open Data |
| Endpoint | `https://data.cityofnewyork.us/resource/tvpp-9vvx.json` |
| Coverage | Jun 2024 – Dec 2026 (29,041 events) |
| Poll interval | Every 30–60 minutes (events don't change by the second) |
| Key fields | `event_name`, `event_type`, `event_borough`, `event_location`, `start_date_time`, `end_date_time` |
| Auth | None required (Socrata app token recommended for reliability) |

**Role:** Live feed of what events are permitted tonight. Join against historical
taxi zones to predict where late-night demand will concentrate.

---

### 3. NYC 311 Noise Complaints - Event-driven

| Property | Value |
| --- | --- |
| Type | Real-time / Event-driven |
| Format | JSON (Socrata REST API, polled as stream) |
| Provider | NYC 311 via NYC Open Data |
| Endpoint | `https://data.cityofnewyork.us/resource/erm2-nwe9.json` |
| Coverage | 2020 – present (continuously updated) |
| Mechanism | Poll every 30s for rows newer than last checkpoint - emit each new complaint as a Kafka event |
| Key fields | `complaint_type`, `borough`, `latitude`, `longitude`, `created_date` |
| Filter | `complaint_type like 'Noise%'` |
| Auth | Free Socrata app token (register at data.cityofnewyork.us) |

**Role:** Real-time signal that nightlife activity is happening. Noise complaints
spike at 2am on weekends near active event locations - validates and enriches
the prediction from the batch + events layers.

---

## How the Three Layers Work Together

```text
Batch (TLC trips)
  "Historically, taxis cluster around venue X at 2am on Saturdays"
          │
          ▼
Periodic (Permitted Events)
  "Venue X has a permitted event tonight until 3am"
          │
          ▼
Event stream (311 Noise Complaints)
  "Noise complaints spiking at venue X right now - prediction confirmed"
```

---

## Data Overlap

| Dataset | Date Range |
| --- | --- |
| TLC Trips (target) | Jan 2023 – Nov 2025 |
| Permitted Events | Jun 2024 – Dec 2026 |
| 311 Noise Complaints | 2020 – present |
| **Batch ∩ Events overlap** | **Jun 2024 – Nov 2025 (18 months)** |
| Note | 2023 data builds the pre-overlap baseline model; 2020–2022 skipped (COVID anomaly) |

---

## Medallion Architecture

| Layer | Contents |
| --- | --- |
| Bronze | Raw TLC Parquet files, raw Events JSON snapshots, raw 311 JSON events |
| Silver | Cleaned taxi trips (midnight–4am filter, quality checks applied), normalized events, deduplicated noise complaints with lat/lon |
| Gold | Taxi demand by zone × hour × day-of-week, event schedule with zone mapping, noise complaint aggregations by zone × hour |

---

## Dashboard Ideas (Apache Superset)

1. **Late-night taxi heatmap** - historical dropoff density by zone, filterable by hour and day of week
2. **Tonight's events map** - active permitted events overlaid on taxi demand zones
3. **Live noise complaint feed** - rolling 30-min complaint counts by borough, spiking in real time

---

## Pending

- [ ] Download TLC 2023 full year + Jul 2024 – Nov 2025 (extend batch coverage)
- [ ] Register Socrata app token for 311 reliability
- [ ] Wire Kafka producer into 311 polling ingester
- [ ] Set up `infra/docker-compose.yml` (Kafka, Airflow, MinIO, Superset)
