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
uv run clm predict-activity          # today + 7-day forecast (default)
uv run clm predict-activity --horizon 0   # today only, no forecast
```

This writes `data/gold/ml_prediction/daily_prediction.parquet` and prints today's prediction plus the 7-day forecast table to the terminal. Re-run whenever you want fresh predictions (e.g., after retraining the model or ingesting new data).

Register two datasets in Superset:

- **Data → Datasets → + Dataset**, Database: `gold_duckdb`, Table: `activity_prediction` (historical + forecast, `is_forecast` column distinguishes them)
- Same flow, Table: `activity_forecast` (forecast rows only — next 7 days)

### Chart 4.1: Today's prediction (Big Number)

Visualization: **Big Number with Trendline**

- Metric: `MAX(probability)`
- Time column: `date`
- Time grain: Day
- Add filter: `date = CURRENT_DATE`
- Subheader: `activity_label` (use a custom label expression or a separate **Big Number** chart below)

### Chart 4.2: Activity label — High or Low (Big Number)

Visualization: **Big Number**

Virtual dataset SQL:

```sql
SELECT
    date,
    activity_label,
    ROUND(probability * 100, 1) AS pct
FROM activity_prediction
WHERE date = (SELECT MAX(date) FROM activity_prediction)
```

- Metric: `MAX(pct)` (shows probability as a percentage)
- Add a Markdown chart alongside it with the label text if Superset's Big Number doesn't support string metrics.

### Chart 4.3: Prediction history (Line chart)

Visualization: **Line chart**

- Dataset: `activity_prediction`, filter `is_forecast = false`
- X: `date`, Y: `MAX(probability)`
- Time grain: Day
- Shows the model's confidence over historical dates — useful for spotting seasonal patterns.

### Chart 4.4: High-activity days calendar

Visualization: **Calendar Heatmap**, virtual dataset SQL:

```sql
SELECT date, probability
FROM activity_prediction
WHERE is_forecast = false
ORDER BY date
```

- Date column: `date`
- Metric: `MAX(probability)`

### Chart 4.5: 7-Day forecast (Table)

Visualization: **Table**

- Dataset: `activity_forecast`
- Columns: `date`, `activity_label`, `probability`
- Sort by `date` ascending
- Shows the model's next-7-day outlook based on calendar features and recent noise trends.

### Chart 4.6: 7-Day forecast (Bar chart)

Visualization: **Bar chart**

- Dataset: `activity_forecast`
- X: `date`, Y: `MAX(probability)`
- Color by `activity_label` (High Activity / Low Activity)

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
