# Monitoring Dashboards - City Live Monitor (NYC Nightlife Pulse)

## Access

| Service    | URL                        | Default credentials |
|------------|----------------------------|---------------------|
| Grafana    | http://localhost:3000       | admin / admin       |
| Prometheus | http://localhost:9090       | -                   |
| Pushgateway| http://localhost:9091       | -                   |

---

## Prometheus Scrape Targets

Prometheus scrapes all targets every 15 seconds (`scrape_interval: 15s`).

| Job name     | Target                              | Metrics path                    | Notes                          |
|--------------|-------------------------------------|---------------------------------|--------------------------------|
| `kafka`      | `kafka-exporter:9308`               | `/metrics` (default)            | kafka-exporter sidecar         |
| `backend`    | `backend:8000`                      | `/metrics`                      | FastAPI with prometheus-client |
| `minio`      | `minio:9000`                        | `/minio/v2/metrics/cluster`     | MinIO cluster metrics endpoint |
| `pushgateway`| `pushgateway:9091`                  | `/metrics` (default)            | `honor_labels: true` - used by Airflow DAG success timestamps and ML metrics pushed from Airflow tasks |
| `airflow`    | `statsd-exporter:9102`              | `/metrics` (default)            | Airflow emits StatsD; statsd-exporter translates to Prometheus format |

---

## Dashboard 1 - Pipeline Health (uid: `nyc-pipeline`)

**URL:** http://localhost:3000/d/nyc-pipeline
**Refresh:** every 10 seconds
**Window:** last 1 hour
**Source file:** `infra/grafana/dashboards/kafka_pipeline.json`

Monitors the Kafka streaming layer: consumer group lag, message throughput, dead-letter queue (DLQ) growth, and broker availability.

| Panel | Type | What it shows |
|-------|------|---------------|
| Kafka Consumer Lag - noise311 | Time series | Per-partition consumer lag on `noise311.raw` broken down by consumer group |
| Kafka Consumer Lag - events | Time series | Per-partition consumer lag on `events.raw` broken down by consumer group |
| Messages In - noise311.raw | Time series | Message ingestion rate (msg/s) per partition, computed as `rate(kafka_topic_partition_current_offset[1m])` |
| Messages In - events.raw | Time series | Message ingestion rate (msg/s) per partition for the events topic |
| DLQ New Messages (10m) - noise311.dlq | Stat | Count of new dead-letter messages in the last 10 minutes; green = 0, red = any growth |
| DLQ New Messages (10m) - events.dlq | Stat | Count of new dead-letter messages in the last 10 minutes for the events DLQ |
| Broker Online | Stat | Number of online Kafka brokers (`kafka_brokers`); green ≥ 1, red = 0 |
| Total Consumer Lag (all groups) | Stat | `sum(kafka_consumergroup_lag)` across all partitions and groups; green < 100, yellow < 500, red ≥ 500 |

---

## Dashboard 2 - Backend API (uid: `nyc-backend-api`)

**URL:** http://localhost:3000/d/nyc-backend-api
**Refresh:** every 30 seconds
**Window:** last 1 hour
**Source file:** `infra/grafana/dashboards/backend_api.json`

Monitors the FastAPI backend (`backend:8000/metrics`). Covers availability, request throughput, error rates, latency percentiles, and ML model health (metrics pushed via Pushgateway by Airflow tasks).

| Panel | Type | What it shows |
|-------|------|---------------|
| Backend Up | Stat | `up{job="backend"}` - green = reachable, red = Prometheus cannot scrape `/metrics` |
| Request Rate (req/s) | Stat | `sum(rate(http_requests_total{job="backend"}[1m]))` - total requests per second |
| Error Rate (5xx %) | Stat | Percentage of 5xx responses over the last 5 minutes; green < 1%, yellow < 5%, red ≥ 5% |
| P95 Latency | Stat | 95th percentile request duration; green < 0.5 s, yellow < 2 s, red ≥ 2 s |
| Request Rate by Endpoint | Time series | Per-`handler` request rate - shows which endpoints are active |
| HTTP Status Distribution | Time series | Per-status-code request rate - distinguishes 2xx, 4xx, 5xx over time |
| Request Latency Percentiles | Time series | P50 / P95 / P99 histogram quantiles over 5-minute windows |
| Gold Table 503 Errors | Time series | Rate of HTTP 503 responses per handler - fires when Gold-layer Parquet files are missing |
| ML Model Health (ROC-AUC) | Stat | `ml_classifier_roc_auc` pushed from the retrain task; green ≥ 0.85, yellow ≥ 0.70, red below |
| ML - Time Since Last Retrain | Stat | `time() - ml_last_retrain_timestamp_seconds`; green < 7 days, yellow < 8 days, red ≥ 8 days |
| ML - Time Since Last Prediction | Stat | `time() - ml_last_prediction_timestamp_seconds`; green < 23 h, yellow < 25 h, red ≥ 25 h |

---

## Dashboard 3 - MinIO Storage (uid: `nyc-minio`)

**URL:** http://localhost:3000/d/nyc-minio
**Refresh:** every 30 seconds
**Window:** last 1 hour
**Source file:** `infra/grafana/dashboards/minio.json`

Monitors MinIO object storage capacity, S3 API traffic, and request errors. Growth spikes on the disk and traffic panels correlate with batch pipeline runs writing Gold-layer Parquet files.

| Panel | Type | What it shows |
|-------|------|---------------|
| Disk Usage % | Stat | `(total - free) / total * 100`; green < 80%, yellow < 90%, orange < 95%, red ≥ 95% |
| Free Disk Space | Stat | Raw free bytes; red < 3 GiB, orange < 5 GiB, yellow < 10 GiB, green ≥ 10 GiB |
| MinIO Online | Stat | `up{job="minio"}` - Up / Down status |
| Total Objects Stored | Stat | `minio_cluster_usage_object_total` - total object count |
| Disk Usage Over Time | Time series | Used vs free raw bytes over time - spikes mark batch writes |
| Data Stored (usable) | Time series | `minio_cluster_usage_total_bytes` vs usable capacity - what the Gold layer actually consumes |
| S3 Incoming Requests | Time series | `rate(minio_s3_requests_incoming_total[1m])` - S3 API request rate in req/s |
| S3 Rejected Requests | Time series | Rates of auth-rejected, invalid, and bad-header S3 requests - correlates with Gold upload failures |
| S3 Traffic | Time series | Bytes sent and received per second - upload spikes show Gold writes, download spikes show API reads |
| MinIO Memory Usage | Time series | `minio_node_process_resident_memory_bytes` - resident set size of the MinIO process |

---

## Dashboard 4 - Airflow (uid: `nyc-airflow`)

**URL:** http://localhost:3000/d/nyc-airflow
**Refresh:** every 30 seconds
**Window:** last 1 hour
**Source file:** `infra/grafana/dashboards/airflow.json`

Monitors the Airflow scheduler and the three project DAGs. Airflow emits StatsD metrics; the `statsd-exporter` container translates them into Prometheus format. DAG last-success timestamps are pushed to Pushgateway by the DAGs themselves and exposed with `honor_labels: true`.

| Panel | Type | What it shows |
|-------|------|---------------|
| Scheduler Alive | Stat | `rate(airflow_scheduler_heartbeat[2m]) > bool 0` - Alive / Dead |
| DAGs Loaded | Stat | `airflow_dagbag_size` - expected value is 3; red < 1, yellow < 3, green ≥ 3 |
| Import Errors | Stat | `airflow_dag_processing_import_errors` - should always be 0; any value ≥ 1 is red |
| Executor Open Slots | Stat | `airflow_executor_open_slots` - red = 0 (tasks will queue), green ≥ 4 |
| Running Tasks | Stat | `airflow_executor_running_tasks` - current count of active task instances |
| Time Since batch_pipeline Last Ran | Stat | `time() - airflow_dag_last_success_timestamp_seconds{dag_id="city_live_monitor_batch_pipeline"}`; green < 7 d, yellow < 10 d, red ≥ 10 d |
| Time Since daily_prediction Last Ran | Stat | Same metric for `city_live_monitor_daily_prediction`; green < 1 d, yellow < 2 d, red ≥ 2 d |
| Time Since weekly_retrain Last Ran | Stat | Same metric for `city_live_monitor_weekly_retrain`; green < 7 d, yellow < 10 d, red ≥ 10 d |
| DAG File Parser Lag | Stat | Seconds since each DAG file was last re-parsed (resets every ~30 s - not last-run time); red ≥ 300 s means processor is stuck |
| Pool Slots | Time series | `default_pool` running / queued / open slots over time |
| Task Execution Rate | Time series | Task instance successes and failures per minute - spikes confirm a DAG is actively running |
| Scheduler Loop Duration (P50 / P90 / P99) | Time series | Scheduler loop iteration time in seconds - spikes indicate scheduler pressure |
| DAG Parse Time | Time series | `airflow_dag_processing_total_parse_time` - total time to parse all DAG files per scan cycle |

---

## Alerts

Alert rules are defined in `infra/prometheus/alerts.yml`.

### Group: `kafka`

| Alert | Severity | Condition | Duration |
|-------|----------|-----------|----------|
| `KafkaConsumerLagHigh` | warning | `kafka_consumergroup_lag > 500` | 5 min sustained |
| `KafkaDLQGrowing` | warning | Any DLQ topic (`*.dlq`) gained messages in the last 10 min | immediate |
| `KafkaBrokerDown` | critical | `kafka_brokers < 1` | 1 min sustained |

### Group: `backend`

| Alert | Severity | Condition | Duration |
|-------|----------|-----------|----------|
| `BackendMetricsDown` | critical | `up{job="backend"} == 0` | 2 min sustained |
| `BackendHighErrorRate` | warning | 5xx rate / total rate > 5% over 5-minute window | 5 min sustained |

### Group: `ml`

| Alert | Severity | Condition | Duration |
|-------|----------|-----------|----------|
| `MLModelStale` | warning | `time() - ml_last_retrain_timestamp_seconds > 691200` (> 8 days) | immediate |
| `MLPredictionStale` | warning | `time() - ml_last_prediction_timestamp_seconds > 90000` (> 25 hours) | immediate |

### Group: `minio`

| Alert | Severity | Condition | Duration |
|-------|----------|-----------|----------|
| `MinIODiskUsageHigh` | warning | Disk used > 90% | 5 min sustained |
| `MinIODiskCritical` | critical | Disk used > 95% - Gold writes will fail | 2 min sustained |
| `MinIODown` | critical | `up{job="minio"} == 0` | 2 min sustained |

### Group: `airflow`

| Alert | Severity | Condition | Duration |
|-------|----------|-----------|----------|
| `AirflowSchedulerDown` | critical | No scheduler heartbeat increase in 5 minutes | 5 min sustained |
| `AirflowDAGImportErrors` | warning | `airflow_dag_processing_import_errors > 0` | 1 min sustained |
| `AirflowDAGProcessingStale` | warning | `airflow_dag_processing_total_parse_time == 0` for 10 minutes | 10 min sustained |
| `AirflowDailyPredictionStale` | warning | `daily_prediction` DAG has not succeeded in > 2 days | 1 min sustained |
| `AirflowExecutorNoSlots` | warning | `airflow_executor_open_slots == 0` - all slots occupied | 5 min sustained |
