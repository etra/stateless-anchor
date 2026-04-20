-- Trino SQL for the pipeline test table.
--
-- Apply via Trino (see test/query-engine/):
--   ./test/query-engine/run.sh
--   docker exec -i trino trino -f - < test/client/create_table.sql
--
-- Anchor itself does not create tables — it only writes into existing ones.
-- Field shape matches the records emitted by test/client/generate.py.
--
-- `ts` is a YYYYMMDD string (e.g. "20260419") and is the identity-
-- partition key. `event_id` is logically the primary key but is NOT
-- declared as an Iceberg identifier field here — Trino's SQL has no
-- direct syntax for that, and anchor is append-only today anyway. When
-- RowDelta commit support lands in iceberg-rust, set identifier fields
-- via PyIceberg or Spark and anchor will start emitting equality deletes.

CREATE SCHEMA IF NOT EXISTS rest.demo;

CREATE TABLE IF NOT EXISTS rest.demo.events (
    ts           VARCHAR     NOT NULL,
    event_id     VARCHAR     NOT NULL,
    event_time   TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    event_date   DATE,
    user_id      VARCHAR,
    event_type   VARCHAR,
    amount       DOUBLE,
    session_id   VARCHAR,
    user_agent   VARCHAR,
    country      VARCHAR,
    properties   ROW(browser VARCHAR, os VARCHAR, device VARCHAR),
    tags         ARRAY(VARCHAR),
    metrics      ARRAY(ROW(name VARCHAR, value DOUBLE))
)
WITH (
    partitioning   = ARRAY['ts'],
    format         = 'PARQUET',
    format_version = 2
);
