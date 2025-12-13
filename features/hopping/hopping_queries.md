# Hopping working KSQL sample

This records the KSQL and Schema Registry calls that ran successfully for a 5 minute HOPPING window with 1 minute advance.

## Prerequisites
- Schema Registry reachable at `http://localhost:18081`
- ksqlDB server reachable with CLI or HTTP API

## Register schemas

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": \"{\\\"type\\\":\\\"string\\\",\\\"name\\\":\\\"TransactionKey\\\"}\"}" http://localhost:18081/subjects/transactions-key/versions

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Transaction\\\",\\\"namespace\\\":\\\"com.example.transactions\\\",\\\"fields\\\":[{\\\"name\\\":\\\"transaction_id\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"user_id\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"amount\\\",\\\"type\\\":\\\"double\\\"},{\\\"name\\\":\\\"currency\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"transaction_time\\\",\\\"type\\\":\\\"long\\\"}]}\"}" http://localhost:18081/subjects/transactions-value/versions
```

## Create stream and hopping table

```sql
CREATE STREAM transactions_stream
WITH (
    KAFKA_TOPIC='transactions',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='AVRO',
    KEY_SCHEMA_FULL_NAME='TransactionKey',
    VALUE_SCHEMA_FULL_NAME='com.example.transactions.Transaction',
    PARTITIONS=1,
    REPLICAS=1
);

CREATE TABLE user_transaction_stats
WITH (
    KAFKA_TOPIC='user_transaction_stats',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='AVRO',
    PARTITIONS=1,
    REPLICAS=1
) AS
SELECT
    user_id,
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COLLECT_SET(currency) AS currencies
FROM transactions_stream
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
GROUP BY user_id
EMIT CHANGES;
```

## Sample inserts

```sql
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time) VALUES ('txn_001', 'user_A', 100.0, 'USD', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time) VALUES ('txn_002', 'user_B', 250.50, 'EUR', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time) VALUES ('txn_003', 'user_A', 75.25, 'USD', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time) VALUES ('txn_004', 'user_C', 300.0, 'JPY', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time) VALUES ('txn_005', 'user_A', 150.0, 'USD', UNIX_TIMESTAMP());
```

Run the statements in order (register schemas → create stream/table → insert sample rows). The table yields 5 minute windows advanced every minute with window bounds mapped to `window_start` and `window_end`.
