-- Bootstrap DuckDB with views over our Gold layer Parquet files.
-- Run once against the persistent DuckDB file to register tables.
-- Superset will then query these views exactly like regular SQL tables.

CREATE OR REPLACE VIEW taxi_demand AS
SELECT * FROM read_parquet('/app/gold_data/gold/taxi_demand_by_zone/taxi_demand_by_zone.parquet');

CREATE OR REPLACE VIEW events AS
SELECT * FROM read_parquet('/app/gold_data/gold/event_schedule/event_schedule.parquet');

CREATE OR REPLACE VIEW noise AS
SELECT * FROM read_parquet('/app/gold_data/gold/noise_by_borough_hour/noise_by_borough_hour.parquet');
