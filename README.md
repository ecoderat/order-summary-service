# order-summary-service

A demo service that produces a **low-latency REST API** for a customer’s **last 30 days** order summary, powered by **Kafka events**, **ClickHouse “current” tables**, and a **Redis read-through cache**.

**“Last 1 month” definition:** rolling **30 days** backwards from an `asOf` boundary. This project computes the window using **UTC start-of-day**:

* `window_to = today@00:00 UTC`
* `window_from = window_to - 30 days`

---

## Components

* `cmd/producer`
  Generates sample **customer** and **order** events into Kafka.

* `cmd/consumer`
  Consumes Kafka events and maintains **current-state** tables in ClickHouse. Uses Redis for **idempotency** and to enqueue **refresh signals** for cache updates.

* `cmd/cache-refresher`
  Listens to a Redis queue (or “dirty keys”) and proactively refreshes summary cache for hot/updated customers.

* `cmd/api`
  Exposes the REST endpoint for monthly (rolling 30d) order summary.

---

## Architecture

```
+------------------+        +-----------+        +------------------+
| cmd/producer      | -----> |   Kafka   | -----> |  cmd/consumer     |
| (event generator) |        | topics    |        | (stream ingest)   |
+------------------+        +-----------+        +------------------+
                                                     |         |
                                                     |         |
                                                     v         v
                                             +-------------+  +----------+
                                             | ClickHouse   |  | Redis    |
                                             | *_current     |  | dedupe   |
                                             | tables        |  | dirty/hot|
                                             +-------------+  +----------+
                                                     ^
                                                     |
                                         +------------------------+
                                         | cmd/cache-refresher    |
                                         | (refresh queue/dirty)  |
                                         +------------------------+

+------------------+    read-through cache     +----------+     DB fallback   +-------------+
| cmd/api (REST)   | <-----------------------> | Redis    | <---------------> | ClickHouse  |
| /monthly-summary |                           | cache    |                   | query        |
+------------------+                           +----------+                   +-------------+
```

---

## Event schema & Kafka

### Customer Event

```json
{
  "event_id": "uuid",
  "event_time": "2025-12-01T10:00:00Z",
  "event_type": "CUSTOMER_CREATED",
  "customer_id": "C123"
}
```

### Order Event

```json
{
  "event_id": "uuid",
  "event_time": "2025-12-10T14:30:00Z",
  "event_type": "ORDER_CREATED",
  "order_id": "O456",
  "customer_id": "C123",
  "total_amount": 1250.75,
  "currency": "TRY"
}
```

**Partition key:** `customer_id`
This keeps a customer’s events colocated (as much as Kafka guarantees), which aligns with customer-scoped caching and summary refresh.

---

## Data model (ClickHouse current tables)

The API reads from “current-state” tables:

### `customers_current`

* `customer_id`
* `created_at`
* `updated_at`
* `source_event_id`

### `orders_current`

* `order_id`
* `customer_id`
* `order_time`
* `total_amount`
* `currency`
* `source_event_id`

A common ClickHouse approach is to use `ReplacingMergeTree` with a version column (e.g., `updated_at` / `order_time`) to keep the latest record per key.

> The system also relies on consumer-side idempotency to avoid double-processing duplicates.

---

## ClickHouse

ClickHouse is the primary datastore for the **serving query** (the API reads from it on cache miss). It’s a good fit because the core workload is a **last-30-days aggregation** (`count` + `sum`) over order data.

### Why ClickHouse?

* **Fast aggregates on time windows** (columnar + scan/aggregate optimized)
* **Efficient storage** (compression + columnar layout)
* **Simple serving model**: query a “current-state” table instead of replaying events at read time

### How we store data

We maintain **current-state tables**:

* `customers_current`: latest customer record
* `orders_current`: latest record per `(customer_id, order_id)`

Events can be **duplicate** and **out-of-order**, so we use a two-layer approach:

1. **Redis idempotency** on `event_id` (skip exact duplicates)
2. **ClickHouse ReplacingMergeTree** with a version column derived from `event_time` so the latest record wins over time

Illustrative schemas:

```sql
CREATE TABLE customers_current
(
  customer_id     String,
  created_at      DateTime64(3, 'UTC'),
  updated_at      DateTime64(3, 'UTC'),
  source_event_id UUID
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (customer_id);

CREATE TABLE orders_current
(
  order_id        String,
  customer_id     String,
  order_time      DateTime64(3, 'UTC'),
  total_amount    Decimal(18,2),
  currency        LowCardinality(String),
  source_event_id UUID
)
ENGINE = ReplacingMergeTree(order_time)
ORDER BY (customer_id, order_id);
```

### Note on trade-offs

`ReplacingMergeTree` deduplication is **eventual** (fully materialized after merges). For strict correctness you can query with `FINAL` (slower) or use a different modeling approach; for this demo the eventual model is acceptable.

#### Alternative approach (keep full history):
If we are willing to store previous versions of updated events, we could use a plain MergeTree table (append-only) and resolve “latest record” logic at query time (e.g., using argMax / maxBy-style patterns). Taking it further, we could build a materialized view into an aggregated table (e.g., AggregatingMergeTree) that maintains {customer_id, order_count, total_spend} incrementally on each insert. Serving from that pre-aggregated table can be extremely fast—often approaching “cached” latency—at the cost of more storage, more write amplification, and more complex update/correction semantics (especially for late/out-of-order events).

---

## Consumer behavior: duplicates & out-of-order

### Idempotency (duplicate events)

The consumer stores a short-lived “processed marker” for `event_id` in Redis. If the same `event_id` appears again, it becomes a **no-op**, preventing double writes and incorrect aggregates.

### Out-of-order (late events)

All writes respect `event_time`. Late events are written with their actual timestamps and will be reflected in the last-30-days window if they fall within it.

### Cache refresh on updates

When the consumer upserts an order for a customer, it enqueues a **refresh signal** (e.g., a “dirty key”) in Redis. `cmd/cache-refresher` consumes these signals and rebuilds the customer summary cache.

---

## Cache strategy (Redis)

### Read-through cache

`cmd/api` uses Redis as a read-through cache:

* Cache hit → return immediately (`source="cache"`)
* Cache miss → query ClickHouse → set cache → return (`source="db"`)

### Cache key

Recommended format:

* `monthly_summary:{customer_id}:{date_key}`
  Where `date_key` is derived from `window_to` (UTC start-of-day) so all requests within the same day share the same cache namespace.

### TTL

Default summary cache TTL: **28 hours**
This keeps the “daily window” hot across typical usage patterns while naturally rolling over.

### Stampede control: lock + jitter

On cache miss, many requests may arrive concurrently for the same `{customer_id, date_key}`. The API mitigates this with:

* a **distributed lock** in Redis (only one request computes)
* **jittered retries** for non-lock holders:

  * waits a random **50–150ms** before re-checking cache
  * spreads retry load and reduces “thundering herd”

### Best-effort cache ops

Cache writes and lock releases are **best-effort** and use a short background timeout (e.g., ~300ms). Cache failures should not fail the request path.

---

## REST API

### Endpoint

`GET /v1/customers/{customerId}/monthly-order-summary`

### Example

```bash
curl "http://localhost:8080/v1/customers/C123/monthly-order-summary"
```

### Response

```json
{
  "customer_id": "C123",
  "window_from": "2025-11-15",
  "window_to": "2025-12-15",
  "order_count": 5,
  "total_spend": 3890.50,
  "currency": "TRY",
  "source": "cache"
}
```

**Status codes**

* `200 OK` on success
* `404 Not Found` if the customer does not exist
* `500 Internal Server Error` for unexpected errors

---

## Requirements

* Go 1.25+
* Docker / Docker Compose

---

## Running locally

### 1) Start infrastructure (Docker)

This starts Kafka + Redis + ClickHouse (and runs `deploy/clickhouse/init.sql`):

```bash
docker compose -f deploy/docker-compose.yml up
```

### 2) Build binaries (optional)

```bash
make build
```

### 3) Run applications (locally)

Run all apps (producer/consumer/api/cache-refresher) in separate terminals or via `make`:

```bash
make run
```

### 4) Generate events

```bash
go run ./cmd/producer -interval=2s -size=50 -customer-ratio=0.4
```

Hot-key scenario (single customer):

```bash
go run ./cmd/producer -interval=1s -size=200 -customer-ratio=0.2 -fixed-customer-id=C123
```

Duplicate/update-like traffic:

```bash
go run ./cmd/producer -interval=2s -size=100 -customer-ratio=0.3 -dup-percent=20 -order-upgrade-percent=30
```

---

## Configuration (ENV)

Selected variables (see `internal/config/config.go` for the full list):

* `KAFKA_BROKERS` (default: `localhost:9092`)
* `KAFKA_CUSTOMER_TOPIC` (default: `customer_events`)
* `KAFKA_ORDER_TOPIC` (default: `order_events`)
* `REDIS_ADDR` (default: `localhost:6379`)
* `CLICKHOUSE_ADDR` (default: `localhost:9000`)
* `HTTP_ADDR` (default: `:8080`)

---

## Tests

```bash
go test ./...
```

Suggested core test coverage:

* API summary service: cache hit/miss, customer not found, DB not found → empty summary, lock not acquired → retry cache hit
* Consumer idempotency (unit tests with fake Redis)
* Cache refresher: refresh signal triggers rebuild (unit tests with fakes)

---

## Notes & trade-offs

* The summary window is **rolling 30 days** (UTC day boundary), not a calendar month.
* Lock + jitter reduces DB stampedes at the cost of a small delay **only** for cache-miss requests that fail to acquire the lock.
* Cache/lock operations are **best-effort** to keep the API responsive under partial dependency failures.
* `total_spend` is returned as `float64` in this demo; production systems typically use decimal or minor units.

---

## Future work

* **Kubernetes deployment**: manifests/Helm chart for Kafka/Redis/ClickHouse and the four services, including ConfigMaps/Secrets and health probes.
* Delivery report handling and metrics (producer/consumer): richer observability, backpressure visibility, and SLO-friendly dashboards.
