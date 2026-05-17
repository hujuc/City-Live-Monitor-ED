-- Bootstrap DuckDB with views over our Gold layer Parquet files.
-- Run once against the persistent DuckDB file to register tables.
-- Superset will then query these views exactly like regular SQL tables.

-- ── Flat aggregate tables ────────────────────────────────────────────────────

CREATE OR REPLACE VIEW taxi_demand AS
SELECT
	CAST(PULocationID AS INTEGER) AS PULocationID,
	borough,
	zone_name,
	hour_of_day,
	day_of_week,
	is_weekend,
	trip_count,
	avg_fare_amount,
	avg_trip_duration_minutes,
	avg_tip_percentage,
	total_trip_distance,
	pct_rush_hour
FROM read_parquet('/app/gold_data/gold/taxi_demand_by_zone/taxi_demand_by_zone.parquet');

CREATE OR REPLACE VIEW events AS
SELECT * FROM read_parquet('/app/gold_data/gold/event_schedule/event_schedule.parquet');

CREATE OR REPLACE VIEW noise AS
SELECT * FROM read_parquet('/app/gold_data/gold/noise_by_borough_hour/noise_by_borough_hour.parquet');

-- ── Star schema (dimensional model) ─────────────────────────────────────────

CREATE OR REPLACE VIEW dim_zone AS
SELECT * FROM read_parquet('/app/gold_data/gold/dim_zone/dim_zone.parquet');

CREATE OR REPLACE VIEW dim_time AS
SELECT * FROM read_parquet('/app/gold_data/gold/dim_time/dim_time.parquet');

CREATE OR REPLACE VIEW fact_taxi_demand AS
SELECT * FROM read_parquet('/app/gold_data/gold/fact_taxi_demand/fact_taxi_demand.parquet');

-- ── Streaming aggregates ─────────────────────────────────────────────────────

CREATE OR REPLACE VIEW stream_metrics_5m AS
SELECT * FROM read_parquet('/app/gold_data/gold/stream_metrics_5m/stream_metrics_5m.parquet');

-- ── Combined batch + streaming view ─────────────────────────────────────────
-- Joins hourly taxi demand with streaming 5-minute event/noise metrics.
-- Only returns rows for hours that have corresponding streaming windows.

CREATE OR REPLACE VIEW batch_streaming_combined AS
SELECT
	td.PULocationID,
	td.borough,
	td.zone_name,
	td.hour_of_day,
	td.day_of_week,
	td.is_weekend,
	td.trip_count,
	td.avg_fare_amount,
	td.avg_trip_duration_minutes,
	td.avg_tip_percentage,
	td.total_trip_distance,
	td.pct_rush_hour,
	COALESCE(ev.event_count, 0)     AS streaming_event_count,
	COALESCE(ev.late_event_count, 0) AS streaming_late_events,
	COALESCE(no.event_count, 0)     AS streaming_noise_count
FROM taxi_demand td
LEFT JOIN (
	SELECT
		HOUR(CAST(window_start_utc AS TIMESTAMP)) AS hour_of_day,
		SUM(event_count)                           AS event_count,
		SUM(late_event_count)                      AS late_event_count
	FROM stream_metrics_5m
	WHERE source = 'nyc_permitted_events'
	GROUP BY 1
) ev ON td.hour_of_day = ev.hour_of_day
LEFT JOIN (
	SELECT
		HOUR(CAST(window_start_utc AS TIMESTAMP)) AS hour_of_day,
		SUM(event_count)                           AS event_count
	FROM stream_metrics_5m
	WHERE source = 'nyc_311_noise'
	GROUP BY 1
) no ON td.hour_of_day = no.hour_of_day;
