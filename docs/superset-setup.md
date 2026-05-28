# Superset Dashboards — Setup Guide

Three dashboards × four charts each, reading Gold layer Parquet files directly via DuckDB.

## Prerequisites

- Gold layer built: `uv run clm gold-build-all`
- Data files exist at `data/gold/{taxi_demand_by_zone,event_schedule,noise_by_borough_hour}/*.parquet`

## Starting Superset

```bash
# Build + start the stack (custom image with duckdb-engine)
docker compose up -d superset-db superset

# Wait for the container to be healthy, then run the bootstrap
docker compose exec superset bash /app/bootstrap.sh
```

The bootstrap script is idempotent — safe to re-run. It:
1. Runs Superset DB migrations
2. Creates the admin user (default: `admin` / `admin`, override with `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` env vars)
3. Initializes the DuckDB file at `/app/superset_home/gold.duckdb` with views over the Gold Parquet files

Then log in at [http://localhost:8088](http://localhost:8088) and register the DuckDB connection (one time):
- **Settings → Database Connections → + Database → Other**
- SQLAlchemy URI: `duckdb:////app/superset_home/gold.duckdb`
- Display name: `gold_duckdb`
- Click **Test Connection**, then **Save**

## Creating datasets

Superset needs a "dataset" entry for each view before charts can reference it.

1. **Data → Datasets → + Dataset**
2. Database: `gold_duckdb`, Schema: `main`
3. Table: pick one of `taxi_demand`, `events`, `noise`
4. Save. Repeat for the other two.

## Dashboard 1 — Late-Night Taxi Heatmap

### Chart 1.1: Heatmap of trip density (zone × hour)
Visualization: **Heatmap**
- X axis: `hour_of_day`
- Y axis: `zone_name`
- Metric: `SUM(trip_count)`
- Filter: `hour_of_day IN (20, 21, 22, 23, 0, 1, 2, 3)`

### Chart 1.2: Top 10 nighttime zones
Visualization: **Bar chart**, virtual dataset SQL:
```sql
SELECT zone_name, SUM(trip_count) AS trips
FROM taxi_demand
WHERE hour_of_day IN (20, 21, 22, 23, 0, 1, 2, 3)
GROUP BY zone_name
ORDER BY trips DESC
LIMIT 10;
```

### Chart 1.3: Trip count by hour
Visualization: **Line chart**
- X: `hour_of_day`, Y: `SUM(trip_count)`
- Group by: `is_weekend` (two series)

### Chart 1.4: Average fare by borough × hour
Visualization: **Pivot table**
- Rows: `borough`, Columns: `hour_of_day`
- Metric: `AVG(avg_fare_amount)`

---

## Dashboard 2 — Tonight's Events

### Chart 2.1: Events by borough
Visualization: **Bar chart**
- Dimension: `borough`
- Metric: `COUNT(*)`

### Chart 2.2: Events by type
Visualization: **Bar chart (horizontal)**
- Dimension: `event_type`
- Metric: `COUNT(*)`, ordered descending, limit 15

### Chart 2.3: Events per day (calendar heatmap)
Visualization: **Calendar heatmap**, virtual dataset SQL:
```sql
SELECT DATE_TRUNC('day', start_datetime_utc) AS day, COUNT(*) AS events
FROM events
GROUP BY day
ORDER BY day;
```

### Chart 2.4: Upcoming nightlife events
Visualization: **Table**
```sql
SELECT event_name, borough, start_datetime_utc, event_duration_hours
FROM events
WHERE is_nightlife_event = true
  AND start_datetime_utc >= CURRENT_DATE
ORDER BY start_datetime_utc
LIMIT 50;
```

---

## Dashboard 3 — Live Noise Complaints

### Chart 3.1: Complaints by borough × hour
Visualization: **Heatmap**
- X: `hour_of_day`, Y: `borough`
- Metric: `SUM(complaint_count)`

### Chart 3.2: Top complaint types
Visualization: **Bar chart**
- Dimension: `complaint_type`
- Metric: `SUM(complaint_count)`, ordered descending, limit 10

### Chart 3.3: Complaints by hour, weekend vs weekday
Visualization: **Line chart**
- X: `hour_of_day`, Y: `SUM(complaint_count)`
- Group by: `is_weekend` (two series)

### Chart 3.4: Descriptor breakdown per borough
Visualization: **Pie chart** (or grouped bar)
```sql
SELECT borough, descriptor, SUM(complaint_count) AS total
FROM noise
GROUP BY borough, descriptor
ORDER BY total DESC;
```

---

## Dashboard 4 — Today's Activity Prediction

### Setup

Run the classifier and generate predictions before opening Superset:

```bash
uv run clm predict-activity   # today + 7-day forecast (default)
```

This writes `data/gold/ml_prediction/daily_prediction.parquet`. Re-run after retraining or ingesting new data.

Register two datasets in Superset:

- **Data → Datasets → + Dataset**, Database: `gold_duckdb`, Table: `activity_prediction`
- Same flow, Table: `activity_forecast` (forecast rows only — next 7 days, includes `day_name`)

### Chart 4.1: Today's confidence (Big Number)

Visualization: **Big Number with Trendline**

Virtual dataset SQL:

```sql
SELECT
    date,
    ROUND(probability * 100, 1) AS confidence_pct
FROM activity_prediction
WHERE is_forecast = false
  AND date = (SELECT MAX(date) FROM activity_prediction WHERE NOT is_forecast)
```

- Metric: `MAX(confidence_pct)`
- Subheader: static text `"% confidence — High Activity"` or `"% confidence — Low Activity"`

### Chart 4.2: 7-Day forecast (Bar chart) — main view

Visualization: **Bar chart**

- Dataset: `activity_forecast`
- X: `date` (Time grain: Day), Y: `MAX(probability)`
- Color/series: `activity_label`
- Bar colors: red for High Activity, blue for Low Activity
- This is the centrepiece — shows the full week at a glance with confidence per day.

### Chart 4.3: 7-Day forecast detail (Table)

Visualization: **Table**

- Dataset: `activity_forecast`
- Columns: `day_name`, `date`, `activity_label`, `probability`
- Sort by `date` ascending
- Conditional formatting: highlight `activity_label = "High Activity"` rows

### Chart 4.4: Recent trend (Line chart)

Visualization: **Line chart**

Virtual dataset SQL:

```sql
SELECT date, probability, activity_label, is_forecast
FROM activity_prediction
WHERE date >= CURRENT_DATE - INTERVAL 14 DAY
ORDER BY date
```

- X: `date`, Y: `MAX(probability)`
- Color/series: `is_forecast` (two series: actual vs. forecast)
- Time grain: Day
- Gives context: are the next 7 days unusual compared to the last two weeks?

---

## Exporting dashboards for version control

After making UI changes, re-export so they're captured in the repo:

```bash
make superset-export
```

This overwrites `infra/superset/assets.zip`. Commit it. Fresh installs then auto-import on `make superset-bootstrap` — no manual UI setup needed.

## Refreshing data

Gold Parquet files are overwritten on each `gold-build-all` run. Superset caches query results — to see fresh data:
- **Dataset → ⋮ → Refresh columns** (picks up schema changes)
- **Chart cache**: force refresh via the chart's clock-icon menu, or shorten `CACHE_DEFAULT_TIMEOUT` in Superset config
