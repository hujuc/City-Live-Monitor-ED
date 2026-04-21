-- Bootstrap DuckDB with views over our Gold layer Parquet files.
-- Run once against the persistent DuckDB file to register tables.
-- Superset will then query these views exactly like regular SQL tables.

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
