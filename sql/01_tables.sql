CREATE DATABASE IF NOT EXISTS transactions;

-- Persistent table. LowCardinality reduces repeated string storage; the primary
-- key and aggregate projection accelerate the state/category analytical query.
CREATE TABLE IF NOT EXISTS transactions.card_transactions
(
    trans_date_trans_time String,
    cc_num String CODEC(ZSTD(3)),
    merchant LowCardinality(String),
    category LowCardinality(String),
    amt Decimal(12, 2) CODEC(Delta, ZSTD(3)),
    first String,
    last String,
    gender LowCardinality(String),
    street String,
    city LowCardinality(String),
    state LowCardinality(FixedString(2)),
    zip UInt32,
    lat Float64,
    long Float64,
    city_pop UInt32,
    job LowCardinality(String),
    dob String,
    trans_num String,
    unix_time UInt64 CODEC(Delta, ZSTD(3)),
    merch_lat Float64,
    merch_long Float64,
    is_fraud UInt8,
    event_time DateTime MATERIALIZED toDateTime(unix_time)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (state, category, amt, unix_time, trans_num)
SETTINGS index_granularity = 8192;

ALTER TABLE transactions.card_transactions
    ADD PROJECTION IF NOT EXISTS max_transaction_by_state
    (
        SELECT
            state,
            argMax(category, tuple(amt, unix_time, trans_num)) AS category,
            max(amt) AS max_amount
        GROUP BY state
    );

-- Kafka table is a queue, not permanent storage. It consumes JSON produced by
-- producer.py. Nullable/default handling is performed before publishing.
CREATE TABLE IF NOT EXISTS transactions.transactions_kafka
(
    trans_date_trans_time String,
    cc_num String,
    merchant String,
    category String,
    amt Decimal(12, 2),
    first String,
    last String,
    gender String,
    street String,
    city String,
    state String,
    zip UInt32,
    lat Float64,
    long Float64,
    city_pop UInt32,
    job String,
    dob String,
    trans_num String,
    unix_time UInt64,
    merch_lat Float64,
    merch_long Float64,
    is_fraud UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_consumer_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions.transactions_consumer
TO transactions.card_transactions
AS SELECT
    trans_date_trans_time, cc_num, merchant, category, amt, first, last,
    gender, street, city, CAST(state, 'FixedString(2)') AS state, zip, lat,
    long, city_pop, job, dob, trans_num, unix_time, merch_lat, merch_long,
    is_fraud
FROM transactions.transactions_kafka;
