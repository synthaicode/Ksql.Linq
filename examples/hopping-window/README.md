# Hopping Window Example

This example demonstrates how to use **Hopping Windows** in KSQL with the Ksql.Linq library.

## What is a Hopping Window?

A hopping window is a time-based window that:
- Has a fixed size (e.g., 5 minutes)
- Advances by a specified interval (e.g., 1 minute)
- Creates overlapping windows when the advance interval is smaller than the window size

For example, with a 5-minute hopping window that advances by 1 minute:
- Window 1: 00:00 - 00:05
- Window 2: 00:01 - 00:06
- Window 3: 00:02 - 00:07
- etc.

This is useful for calculating moving averages, detecting trends, and analyzing time-series data with overlapping time periods.

## Use Case: Transaction Analytics

This example tracks user transactions and calculates statistics over hopping windows:
- Window size: 5 minutes
- Advance by: 1 minute
- Aggregations: count, sum, avg, min, max

Each transaction is included in multiple overlapping windows, allowing you to see how metrics evolve over time.

## Prerequisites

- .NET 8.0 SDK
- Docker and Docker Compose (for Kafka, KSQL, and Schema Registry)

## Setup

### 1. Start Kafka, KSQL, and Schema Registry

Use the provided Docker Compose file:

```bash
docker-compose up -d
```

### 2. Register Avro Schemas

Register the key and value schemas with Schema Registry:

```bash
# Register key schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"string\",\"name\":\"TransactionKey\"}"}' \
  http://localhost:18081/subjects/transactions-key/versions

# Register value schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"com.example.transactions\",\"fields\":[{\"name\":\"transaction_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"transaction_time\",\"type\":\"long\"}]}"}' \
  http://localhost:18081/subjects/transactions-value/versions
```

### 3. Create KSQL Stream and Table

Connect to KSQL CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Execute the following KSQL statements:

```sql
-- Create stream
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

-- Create hopping window table
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

### 4. Insert Sample Data

```sql
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time)
    VALUES ('txn_001', 'user_A', 100.0, 'USD', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time)
    VALUES ('txn_002', 'user_B', 250.50, 'EUR', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time)
    VALUES ('txn_003', 'user_A', 75.25, 'USD', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time)
    VALUES ('txn_004', 'user_C', 300.0, 'JPY', UNIX_TIMESTAMP());
INSERT INTO transactions_stream (transaction_id, user_id, amount, currency, transaction_time)
    VALUES ('txn_005', 'user_A', 150.0, 'USD', UNIX_TIMESTAMP());
```

### 5. Query Results

```sql
SELECT * FROM user_transaction_stats EMIT CHANGES;
```

## Running the Example

Run the C# example to see how Ksql.Linq generates the KSQL queries:

```bash
dotnet run
```

The example will:
1. Create a hopping window query using the Ksql.Linq fluent API
2. Generate the corresponding KSQL CREATE TABLE statement
3. Display the complete KSQL setup

## Key Concepts

### Hopping Window in Ksql.Linq

```csharp
var query = new KsqlQueryable<Transaction>()
    .Hopping(
        time: t => t.TransactionTime,
        windows: new HoppingWindows
        {
            Size = 5,           // Window size
            AdvanceBy = 1,      // How much to advance
            Unit = "MINUTES"    // Time unit
        },
        baseUnitSeconds: 10,
        grace: TimeSpan.FromSeconds(3),
        continuation: false)
    .GroupBy(t => t.UserId)
    .Select(g => new UserTransactionStats
    {
        UserId = g.Key,
        WindowStart = g.WindowStart(),
        TransactionCount = g.Count(),
        TotalAmount = g.Sum(t => t.Amount),
        AvgAmount = g.Average(t => t.Amount),
        MinAmount = g.Min(t => t.Amount),
        MaxAmount = g.Max(t => t.Amount)
    });
```

### Generated KSQL

The above C# code generates:

```sql
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE, GRACE PERIOD 3 SECONDS)
```

## Difference from Tumbling Windows

| Feature | Tumbling Window | Hopping Window |
|---------|----------------|----------------|
| Overlapping | No | Yes (when advance < size) |
| Window coverage | Gaps possible | Continuous coverage |
| Use case | Distinct time periods | Moving averages, trend analysis |
| Example | Daily reports | 5-min windows every 1 min |

## Grace Period

The grace period (3 seconds in this example) allows late-arriving events to be included in their proper windows, handling out-of-order data.

## Cleanup

```bash
docker-compose down -v
```

## Further Reading

- [KSQL Windowing Documentation](https://docs.ksqldb.io/en/latest/concepts/time-and-windows-in-ksqldb-queries/)
- [Hopping Window Use Cases](https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#hopping-time-windows)
