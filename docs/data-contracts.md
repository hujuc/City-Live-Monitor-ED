# Data Contracts: Real-Time Sources

**Authors:** Gabriel Silva · Hugo Castro, University of Aveiro, 2026
**Date:** 2026-05-19

These contracts define the schema and guarantees for the two streaming data sources as they flow through the ingestion pipeline. Each contract covers: the Kafka envelope published to the message bus, the Bronze NDJSON payload written to disk, and the delivery semantics.

---

## 1. 311 Noise Complaints

**Source:** NYC Open Data Socrata API (`erm2-nwe9.json`)
**Kafka topic:** `noise311.raw`
**DLQ topic:** `noise311.dlq`
**Polling interval:** 30 seconds
**Checkpoint field:** `created_date`

### 1.1 Kafka Envelope Schema

Every message published to `noise311.raw` has this top-level structure:

| Field | Type | Nullable | Description |
|-------|------|:--------:|-------------|
| `event_id` | string (32-char hex) | No | SHA-256 hash of `source + unique_key + created_date + incident_zip + complaint_type + descriptor` |
| `source` | string | No | Always `"noise311"` |
| `timestamp` | string (ISO-8601 UTC) | No | Parsed from `created_date`; falls back to `now()` if unparseable |
| `received_at` | string (ISO-8601 UTC) | No | Wall-clock time at normalization |
| `payload` | object | No | Full raw Socrata row (see §1.2) |

Kafka message key: `event_id` (ensures same event always routes to same partition).

### 1.2 Payload Fields (key columns)

| Field | Type | Nullable | Notes |
|-------|------|:--------:|-------|
| `unique_key` | string | No | Socrata row ID; primary dedup key |
| `created_date` | string (ISO-8601) | No | When complaint was filed |
| `closed_date` | string (ISO-8601) | Yes | Null if complaint still open |
| `complaint_type` | string | No | e.g., `"Noise - Residential"` |
| `descriptor` | string | No | Specific noise type |
| `borough` | string | No | NYC borough name |
| `latitude` | string | Yes | Decimal degrees; may be null |
| `longitude` | string | Yes | Decimal degrees; may be null |
| `status` | string | No | `"Open"` or `"Closed"` |
| `_ingestion_timestamp` | string (ISO-8601 UTC) | No | Injected at Bronze write time |

### 1.3 Rejection Rules (DLQ routing)

A message is routed to `noise311.dlq` instead of `noise311.raw` if:
- `created_date` is missing or cannot be parsed to a datetime
- Any exception is raised during envelope construction

DLQ messages include a `reason` field (`"malformed_event"` or `"kafka_publish_failed"`) and the full raw row for replay.

### 1.4 Delivery Semantics

| Property | Value |
|----------|-------|
| Delivery guarantee | At-least-once (checkpoint only advances on full-batch success) |
| Deduplication | Idempotent at Silver layer via `unique_key` |
| Ordering | Within-partition ordering by `event_id` key; global ordering not guaranteed |
| Late event threshold | 5 minutes (watermark delay in `EventProcessor`) |
| Dedup window | 10 minutes in-memory sliding window |

---

## 2. NYC Permitted Events

**Source:** NYC Open Data Socrata API (`tvpp-9vvx.json`)
**Kafka topic:** `events.raw`
**DLQ topic:** `events.dlq`
**Polling interval:** 30 minutes
**Checkpoint field:** `start_date_time`

### 2.1 Kafka Envelope Schema

Every message published to `events.raw` has this top-level structure:

| Field | Type | Nullable | Description |
|-------|------|:--------:|-------------|
| `event_id` | string (32-char hex) | No | SHA-256 hash of `source + event_id + event_name + start_date_time` |
| `source` | string | No | Always `"events"` |
| `timestamp` | string (ISO-8601 UTC) | No | Parsed from `start_date_time` |
| `received_at` | string (ISO-8601 UTC) | No | Wall-clock time at normalization |
| `payload` | object | No | Full raw Socrata row (see §2.2) |

Kafka message key: `event_id`.

### 2.2 Payload Fields (key columns)

| Field | Type | Nullable | Notes |
|-------|------|:--------:|-------|
| `event_id` | string | No | Socrata row ID; primary dedup key |
| `event_name` | string | Yes | Official event name (0.1% null) |
| `start_date_time` | string (ISO-8601) | No | Event start; local NYC time |
| `end_date_time` | string (ISO-8601) | No | Event end; local NYC time |
| `event_type` | string | No | `"Parade"`, `"Film"`, `"Athletic"`, etc. |
| `event_borough` | string | No | NYC borough |
| `event_location` | string | No | Street address or park name |
| `event_agency` | string | No | Permitting agency |
| `street_closure_type` | string | No | `"Full"`, `"Partial"`, `"No Closure"` |
| `_ingestion_timestamp` | string (ISO-8601 UTC) | No | Injected at Bronze write time |

### 2.3 Rejection Rules (DLQ routing)

A message is routed to `events.dlq` if:
- `start_date_time` cannot be parsed to a datetime
- Any exception is raised during envelope construction

### 2.4 Delivery Semantics

| Property | Value |
|----------|-------|
| Delivery guarantee | At-least-once (checkpoint only advances on full-batch success) |
| Deduplication | Idempotent at Silver layer via `event_id` |
| Ordering | Within-partition ordering; global ordering not guaranteed |
| Late event threshold | 5 minutes (watermark delay in `EventProcessor`) |
| Dedup window | 10 minutes in-memory sliding window |

---

## 3. Common Guarantees

These guarantees apply to both sources:

| Guarantee | Description |
|-----------|-------------|
| `_ingestion_timestamp` | Every Bronze NDJSON row contains this field (UTC ISO-8601), injected before writing to disk, independently of Kafka publishing |
| Deterministic `event_id` | Same source row always produces the same `event_id`; safe to re-ingest without creating phantom duplicates |
| Bronze persistence | Each polling cycle writes a full NDJSON snapshot to `data/bronze/<source>/YYYY/MM/snapshot_<ISO8601Z>.ndjson` before any Kafka publish attempt. Bronze is independent of Kafka availability. |
| Circuit breaker | After 5 consecutive API failures, the poller enters a 120-second cooldown before retrying |
| Pagination | Socrata responses are fetched in 2000-row pages; all pages are fetched before publishing to Kafka |
| Checkpoint safety | The cursor (last-seen timestamp) is only advanced when all Kafka publishes for a batch succeed |
