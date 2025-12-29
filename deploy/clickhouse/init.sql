CREATE TABLE IF NOT EXISTS customers_current
(
  customer_id     String,
  created_at      DateTime64(3, 'UTC'),
  updated_at      DateTime64(3, 'UTC'),
  source_event_id UUID
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (customer_id);

CREATE TABLE IF NOT EXISTS orders_current
(
  order_id        String,
  customer_id     String,
  order_time      DateTime64(3, 'UTC'),
  total_amount    Decimal(18, 2),
  currency        LowCardinality(String),
  source_event_id UUID
)
ENGINE = ReplacingMergeTree(order_time)
ORDER BY (customer_id, order_id);
