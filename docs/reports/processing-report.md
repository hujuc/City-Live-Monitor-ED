# Data Processing Report

**Authors:** Gabriel Silva · Hugo Castro, University of Aveiro, 2026
**Generated:** 2026-06-06
**Pipeline:** Bronze -> Silver -> Gold, Medallion Architecture

---

## 1. Batch Processing Summary

| Metric | Value |
|--------|------:|
| Bronze TLC files processed | 35 |
| Bronze TLC rows ingested | 123,897,542 |
| Silver TLC rows written | 102,925,036 |
| Rows removed by quality filters | 20,972,506 |
| Retention rate | 83.1% |
| Silver partitions created | 171 (year=YYYY/month=MM) |
| Silver columns: source preserved | 19 |
| Silver columns: derived/engineered | 15 |
| Coverage | Jan 2023 - Nov 2025 |

---

## 2. Filtering Breakdown (TLC Yellow Taxi)

Filter rules are applied sequentially. Percentages are relative to the sample dataset (2 Bronze file(s), n = 5,980,721).

| Filter rule | Condition | Rows removed | % |
|-------------|-----------|-------------:|--:|
| `out_of_range_year` | see Silver processor | 7 | 0.0% |
| `zero_or_negative_distance` | `trip_distance` <= 0 | 86,959 | 1.45% |
| `negative_fare` | `fare_amount` < 0 | 43,776 | 0.73% |
| `negative_tip` | `tip_amount` < 0 | 9 | 0.0% |
| `negative_total` | `total_amount` < 0 | 219 | 0.0% |
| `zero_or_missing_passengers` | `passenger_count` < 1 | 232,193 | 3.88% |
| `distance_over_100_miles` | see Silver processor | 68 | 0.0% |
| `fare_over_500` | see Silver processor | 25 | 0.0% |
| `passenger_count_over_6` | see Silver processor | 19 | 0.0% |
| `tip_exceeds_fare` | see Silver processor | 5,358 | 0.09% |
| `non_positive_duration` | dropoff <= pickup datetime | 138 | 0.0% |
| `duration_under_1_minute` | see Silver processor | 15,906 | 0.27% |
| `duration_over_24_hours` | see Silver processor | 35 | 0.0% |
| **Total removed** | | **384,712** | **6.4%** |

Note: `zero_or_missing_passengers` is typically the largest single contributor. Per-rule breakdown per file is logged via `FilterRuleStat` during each Silver run.

---

## 3. Streaming Source Summary

### 3.1 Permitted Events (Socrata `tvpp-9vvx`)

| Metric | Value |
|--------|------:|
| Bronze snapshots | 17 NDJSON files |
| Bronze rows (total, with duplicates) | 243,897 |
| Polling interval | 30 minutes |
| Deduplication key | `event_id` |
| Silver rows (unique events) | 71,629 |
| Cross-snapshot duplicate rate | 70.6% |

The high duplicate rate is expected: the Socrata Events API returns the full historical dataset on every poll. Silver deduplication by `event_id` is the primary transformation.

### 3.2 311 Noise Complaints (Socrata `erm2-nwe9`)

| Metric | Value |
|--------|------:|
| Bronze snapshots | 11 NDJSON files |
| Bronze rows (total, with duplicates) | 100,131 |
| Polling interval | 30 seconds |
| Deduplication key | `unique_key` |
| Silver rows (unique complaints) | 100,130 |

---

## 4. Stream Processing Summary (EventProcessor)

The `EventProcessor` handles events from both Kafka topics (`events.raw`, `noise311.raw`). The following metrics are from a representative run using `clm stream-aggregates`.

| Metric | Events source | Noise311 source |
|--------|:-------------:|:---------------:|
| Deduplication window | 10 min | 10 min |
| Watermark delay | 5 min | 5 min |
| Tumbling window size | 5 min | 5 min |
| Late event ratio (backfill run) | ~1.0 | ~0.6 |
| Duplicate ratio | low | low |
| Out-of-order detection | enabled | enabled |

**Late event ratio interpretation:** The Events source (`late_event_ratio` near 1.0) reflects historical backfill behavior, not pipeline failure. Events from 2020 onward all arrive 'late' relative to the 5-minute watermark because they are ingested in bulk from the Socrata API. Recent Noise complaints show a lower late-event ratio since their `created_date` is close to their ingestion time.

---

## 5. Windowed Aggregation Sample

Below is a representative sample from the `stream_metrics_5m` Gold table (5-minute tumbling windows, produced by `clm stream-aggregates`):

| window_start_utc | source | subject | event_count | late_count | dup_count | late_ratio |
|------------------|--------|---------|------------:|-----------:|----------:|-----------:|
| 2024-06-30 08:00:00+00:00 | nyc_permitted_events | Special Event | 1 | 0 | 0 | 0.00 |
| 2026-01-01 05:00:00+00:00 | nyc_permitted_events | Special Event | 1 | 0 | 0 | 0.00 |
| 2026-01-08 09:00:00+00:00 | nyc_permitted_events | Street Event | 1 | 0 | 0 | 0.00 |
| 2026-01-27 09:00:00+00:00 | nyc_permitted_events | Street Event | 1 | 0 | 0 | 0.00 |
| 2026-02-09 09:00:00+00:00 | nyc_permitted_events | Street Event | 1 | 0 | 0 | 0.00 |

---

## 6. Out-of-Order Event Analysis

Out-of-order events are events whose `timestamp` field is earlier than the most recently seen timestamp for the same source. This can occur when:

1. Socrata returns rows out of chronological order within a page
2. Backfill polling retrieves old records alongside new ones
3. Network delays cause reordering in Kafka delivery

From the representative run:
- **Events source:** out-of-order events are common during backfill; negligible during steady-state polling
- **Noise311 source:** low out-of-order rate for recent complaints; elevated during initial backfill when multiple years of history are fetched

The `EventProcessor` tracks `out_of_order_event_count` per window and surfaces these in `stream_metrics_5m` under the `out_of_order_event_count` column. No events are discarded due to out-of-order arrival; they are counted and forwarded.

---

## 7. Gold Layer Output

| Table | Rows | Description |
|-------|-----:|-------------|
| `taxi_demand_by_zone` | 38,930 | Aggregated by zone x hour x day-of-week |
| `event_schedule` | 71,629 | All unique permitted events with time features |
| `noise_by_borough_hour` | 11,570 | Aggregated by borough x type x descriptor x hour x day |
| `stream_metrics_5m` | 25,993 | 5-minute Kafka window metrics |
| `dim_zone` | 265 | Star schema zone dimension |
| `dim_time` | 168 | Star schema time dimension (24h x 7d) |
| `fact_taxi_demand` | 38,930 | Star schema fact table |

Note: `dim_zone`, `dim_time`, `fact_taxi_demand`, and `stream_metrics_5m` require `clm gold-build-all --star-schema` and `clm stream-aggregates` respectively to populate on a fresh environment.

---

## 8. Processing Orchestration

The full batch pipeline is orchestrated by an Airflow DAG (`batch_pipeline`) with the following topology:

```
ingest_tlc_to_bronze
        |
process_tlc_bronze_to_silver    process_events_bronze_to_silver
        |                               |
        +---------------+---------------+
                        |    process_noise311_bronze_to_silver
                        |               |
                        +-------+-------+
                                |
                         build_gold_tables
```

DAG settings: `schedule_interval=@daily`, `catchup=False`, `max_active_runs=1`, `retries=1` with 5-minute retry delay on all tasks.
