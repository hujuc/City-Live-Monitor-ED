# Data Profiling Report

**Authors:** Gabriel Silva · Hugo Castro, University of Aveiro, 2026
**Generated:** 2026-05-19
**Sources profiled:** NYC TLC Yellow Taxi (Bronze), NYC Permitted Events (Bronze), 311 Noise Complaints (Bronze)

---

## Executive Summary

| Source | Files | Rows sampled | Critical issues |
|--------|------:|-------------:|----------------|
| TLC Yellow Taxi (batch) | 35 Parquet | 9,384,487 (3 file(s)) | Negative fares/tips, extreme distances, ~2.3% nulls in optional fields |
| NYC Permitted Events (streaming) | 10 NDJSON | 114,266 | `event_street_side` 99.3% null; `cemsid` 0.6% null |
| 311 Noise Complaints (streaming) | 5 NDJSON | 1,521,642 | `closed_date` 1.6% null (open complaints); `vehicle_type` 94.5% null |

Overall data quality is **good for the batch source and adequate for streaming**. The dominant issues are: (1) a small fraction of TLC trips with physically impossible values (negative fares, distances > 100 miles), and (2) sparse optional columns in both Socrata sources that carry no analytical value and can be safely ignored.

---

## 1. TLC Yellow Taxi (Batch, Bronze)

### 1.1 Coverage and Volume

- Files: 35 monthly Parquet files, January 2023 to November 2025
- Sample used for profiling: 9,384,487 rows across 3 file(s): `yellow_tripdata_2023-01`, `yellow_tripdata_2023-02`, `yellow_tripdata_2023-03`

### 1.2 Schema

| Column | Type | Null % | Notes |
|--------|------|-------:|-------|
| `vendorid` | int64 | 0.0 | 1 = Creative Mobile, 2 = VeriFone |
| `tpep_pickup_datetime` | datetime64[us] | 0.0 | Local NYC time, no timezone |
| `tpep_dropoff_datetime` | datetime64[us] | 0.0 | Local NYC time, no timezone |
| `passenger_count` | float64 | 2.52 | Driver-entered; nullable |
| `trip_distance` | float64 | 0.0 | Miles |
| `ratecodeid` | float64 | 2.52 | 1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated, 6=Group |
| `store_and_fwd_flag` | str | 2.52 | Y/N; whether trip stored locally before transmit |
| `pulocationid` | int64 | 0.0 | TLC Taxi Zone ID (1-265) |
| `dolocationid` | int64 | 0.0 | TLC Taxi Zone ID (1-265) |
| `payment_type` | int64 | 0.0 | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `fare_amount` | float64 | 0.0 | USD base fare |
| `extra` | float64 | 0.0 | USD surcharges |
| `mta_tax` | float64 | 0.0 | USD |
| `tip_amount` | float64 | 0.0 | USD; auto-populated for credit card |
| `tolls_amount` | float64 | 0.0 | USD |
| `improvement_surcharge` | float64 | 0.0 | USD |
| `total_amount` | float64 | 0.0 | USD |
| `congestion_surcharge` | float64 | 2.52 | USD; added 2019 |
| `airport_fee` | float64 | 68.09 | USD; added 2022 (absent in older files) |

Schema note: `airport_fee` was added mid-2022 and `Airport_fee` (capital A) appears in some 2024 files. The Bronze Validator normalizes column names to lowercase to avoid false schema-drift positives.

### 1.3 Statistical Profile (sample, n = 9,384,487)

| Column | Min | Mean | Median | p95 | Max |
|--------|----:|-----:|-------:|----:|----:|
| `passenger_count` | 0.0 | 1.36 | 1.0 | 3.0 | 9.0 |
| `trip_distance` (mi) | 0.0 | 3.87 | 1.79 | 14.21 | 335004.33 |
| `fare_amount` ($) | -959.9 | 18.52 | 12.8 | 64.6 | 2203.1 |
| `tip_amount` ($) | -96.22 | 3.42 | 2.8 | 11.27 | 984.3 |
| `total_amount` ($) | -982.95 | 27.27 | 20.3 | 80.55 | 2208.1 |
| `congestion_surcharge` ($) | -2.5 | 2.28 | 2.5 | 2.5 | 2.75 |
| `airport_fee` ($) | -1.25 | 0.11 | 0.0 | 1.25 | 1.25 |

### 1.4 Data Quality Issues

| Issue | Severity | Estimated rate | Handling |
|-------|----------|---------------:|---------|
| Negative `fare_amount` | High | 0.85% | Remove (reversals/corrections) |
| Zero or negative `trip_distance` | High | 1.44% | Remove (meter-on records) |
| Zero or missing `passenger_count` | Medium | 1.67% | Remove (driver entry error) |
| `trip_distance` > 100 miles | Medium | 0.00% | Flag or remove (GPS errors) |
| `fare_amount` > $500 | Low | 0.00% | Flag (negotiated or airport charter) |
| Negative `tip_amount` | Low | 0.01% | Remove (reversal transactions) |
| Negative `total_amount` | Low | 0.85% | Remove (reversal transactions) |
| `passenger_count` > 6 | Low | 0.00% | Flag (exceeds standard capacity) |
| Non-UTC timestamps | Informational | 100% | Convert to UTC in Silver |
| Null `passenger_count` / `RatecodeID` | Informational | 2.52% | Acceptable; driver-entered fields |

### 1.5 Cleaning Recommendations

1. Remove records with `fare_amount < 0`, `tip_amount < 0`, `trip_distance <= 0`, `passenger_count < 1`.
2. Convert `tpep_pickup_datetime` and `tpep_dropoff_datetime` to UTC-aware timestamps.
3. Cast monetary columns to float64; cast zone IDs to int32.
4. Flag (not remove) extreme outliers: `tip > 50%` of fare, `speed > 80 mph`, `distance > 100 miles`.
5. Cap `trip_duration_minutes` at the 95th percentile to limit downstream distortion.

---

## 2. NYC Permitted Events (Streaming, Bronze)

### 2.1 Coverage and Volume

- Files: 10 NDJSON polling snapshots, April 2026 - May 2026
- Total Bronze rows: 309,318 (with cross-snapshot duplicates; each poll returns the full historical dataset)
- Sample used for profiling: 114,266 rows from first 3 snapshot(s)

### 2.2 Schema

| Column | Type | Null % | Notes |
|--------|------|-------:|-------|
| `event_id` | str | 0.0 | Unique Socrata row identifier |
| `event_name` | str | 0.06 | Official event name |
| `start_date_time` | str | 0.0 | Local time |
| `end_date_time` | str | 0.0 | Local time |
| `event_agency` | str | 0.0 | Permitting agency |
| `event_type` | str | 0.0 | Parade, Film, Athletic, etc. |
| `event_borough` | str | 0.0 | NYC borough |
| `event_location` | str | 0.0 | Street address or park name |
| `street_closure_type` | str | 0.0 | Full / Partial / No closure |
| `community_board` | str | 0.0 |  |
| `police_precinct` | str | 0.0 |  |
| `cemsid` | str | 0.64 | Internal city event ID |
| `event_street_side` | str | 99.34 | Almost entirely null; drop |

### 2.3 Data Quality Issues

| Issue | Severity | Handling |
|-------|----------|---------|
| Cross-snapshot duplicates | High | Deduplicate by `event_id` at Silver |
| Local timestamps (no timezone) | Medium | Convert to UTC in Silver |
| `event_street_side` 99.3% null | Low | Drop column |
| `cemsid` 0.6% null | Low | Passthrough; not used in analysis |
| `event_name` ~0.1% null | Low | Drop rows with missing name |

### 2.4 Cleaning Recommendations

1. Deduplicate by `event_id` across all snapshots (primary transformation; reduces ~200K rows to ~35K unique events).
2. Parse `start_date_time` / `end_date_time` to UTC-aware datetime.
3. Compute `event_duration_hours` and normalize `borough` from `event_borough`.
4. Drop `event_street_side` (~99% null, no analytical value).

---

## 3. 311 Noise Complaints (Streaming, Bronze)

### 3.1 Coverage and Volume

- Files: 5 NDJSON polling snapshots, March 2026 - May 2026
- Total Bronze rows: 1,521,998 (with cross-snapshot duplicates; latest snapshot is small due to incremental polling after backfill)
- Sample used for profiling: 1,521,642 rows from first 3 snapshot(s)

### 3.2 Schema

| Column | Type | Null % | Notes |
|--------|------|-------:|-------|
| `unique_key` | str | 0.0 | Socrata row ID |
| `created_date` | str | 0.0 | ISO-8601 |
| `closed_date` | str | 1.63 | Null for open complaints |
| `agency` | str | 0.0 | Always 'NYPD' for noise |
| `agency_name` | str | 0.0 |  |
| `complaint_type` | str | 0.0 | 'Noise - Residential', etc. |
| `descriptor` | str | 0.0 | Specific noise type |
| `location_type` | str | 6.86 |  |
| `incident_zip` | str | 0.01 |  |
| `incident_address` | str | 0.58 |  |
| `street_name` | str | 0.59 |  |
| `cross_street_1` | str | 0.78 |  |
| `cross_street_2` | str | 0.75 |  |
| `intersection_street_1` | str | 6.47 |  |
| `intersection_street_2` | str | 6.43 |  |
| `address_type` | str | 0.0 |  |
| `city` | str | 3.07 |  |
| `landmark` | str | 9.93 |  |
| `status` | str | 0.0 | Open / Closed |
| `resolution_description` | str | 1.58 | Null for open complaints |
| `resolution_action_updated_date` | str | 1.58 |  |
| `community_board` | str | 0.0 |  |
| `council_district` | str | 0.42 |  |
| `police_precinct` | str | 0.0 |  |
| `bbl` | str | 6.57 |  |
| `borough` | str | 0.0 | NYC borough |
| `x_coordinate_state_plane` | str | 0.41 |  |
| `y_coordinate_state_plane` | str | 0.41 |  |
| `open_data_channel_type` | str | 0.0 |  |
| `park_facility_name` | str | 0.0 |  |
| `park_borough` | str | 0.0 |  |
| `latitude` | str | 0.41 | Decimal degrees; may be null |
| `longitude` | str | 0.41 | Decimal degrees; may be null |
| `location.type` | str | 0.41 |  |
| `location.coordinates` | object | 0.41 |  |
| `descriptor_2` | str | 90.45 |  |
| `facility_type` | str | 93.14 |  |
| `vehicle_type` | str | 94.51 | Almost entirely null |

### 3.3 Data Quality Issues

| Issue | Severity | Handling |
|-------|----------|---------|
| Cross-snapshot duplicates | High | Deduplicate by `unique_key` at Silver |
| `latitude`/`longitude` as string | Medium | Cast to float64 in Silver |
| Local timestamps | Medium | Convert to UTC in Silver |
| `closed_date` 1.6% null | Informational | Expected; reflects open complaints |
| `vehicle_type` 94.5% null | Low | Drop or ignore |
| Records outside NYC bounds | Low | Remove in Silver (geocoding filter) |

### 3.4 Cleaning Recommendations

1. Deduplicate by `unique_key` across snapshots.
2. Cast `latitude` and `longitude` to float64; remove records outside NYC bounding box (lat 40.48-40.93, lon -74.26 to -73.68). Keep null-coordinate records.
3. Parse `created_date` / `closed_date` to UTC-aware datetime.
4. Compute `resolution_duration_hours` where `closed_date` is present.
5. Drop `vehicle_type` and other columns with > 90% null rate that have no analytical use.
