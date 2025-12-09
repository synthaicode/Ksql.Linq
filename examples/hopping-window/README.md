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

### 2. Register Avro Schemas with Schema Registry

**Why Schema Registry?**

Schema Registry is essential for:
- **Schema Evolution**: Manage schema changes over time safely
- **Data Validation**: Ensure data conforms to expected structure
- **Compatibility**: Maintain backward/forward compatibility
- **Efficiency**: Reduce payload size by storing schemas centrally

The producer uses **AutoRegisterSchemas = true**, so schemas are automatically registered on first use. However, for KSQL to work properly, we need to register them manually first with specific subject names.

**Register schemas:**

```bash
# Register value schema (complex record type)
# Note: Key uses KAFKA (String) format, so no Avro schema needed for key
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"com.example.transactions\",\"fields\":[{\"name\":\"transaction_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"transaction_time\",\"type\":\"long\"}]}"}' \
  http://localhost:18081/subjects/transactions-value/versions

# Verify value schema registration
curl http://localhost:18081/subjects/transactions-value/versions
```

**Key Format Choice:**

We use `KEY_FORMAT='KAFKA'` (String) instead of AVRO for the key because:
- Kafka keys are typically simple strings (transaction IDs, user IDs, etc.)
- AVRO keys require complex record schemas and can cause KSQL compatibility issues
- String keys are simpler, more performant, and widely supported
- Only the value payload uses AVRO for schema evolution benefits

### 3. Create KSQL Stream and Table

Connect to KSQL CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Execute the following KSQL statements:

```sql
-- Create stream
CREATE STREAM transactions_stream (
    transaction_id VARCHAR KEY,
    transaction_id_value VARCHAR,
    user_id VARCHAR,
    amount DOUBLE,
    currency VARCHAR,
    transaction_time BIGINT
)
WITH (
    KAFKA_TOPIC='transactions',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO',
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

### 4. Run the Producer (Option A)

Use the C# producer to generate continuous test data:

```bash
cd examples/hopping-window
dotnet run --project HoppingWindowProducer.csproj
```

The producer will:
- Generate random transactions for users (user_A, user_B, user_C, user_D, user_E)
- Send them to the `transactions` topic
- Continue until you press Ctrl+C

### 4. Insert Sample Data Manually (Option B)

Alternatively, you can insert data manually via KSQL CLI:

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

### 5. Run the Consumer

In a new terminal, run the consumer to see aggregated results:

```bash
cd examples/hopping-window
dotnet run --project HoppingWindowConsumer.csproj
```

The consumer will:
- Subscribe to the `user_transaction_stats` topic
- Display hopping window aggregations in real-time
- Execute pull queries to demonstrate direct table queries
- Continue until you press Ctrl+C

Example output:
```
[12:34:56.789] User: user_A, Window: 12:30:00 - 12:35:00, Count: 3, Total: $325.25, Avg: $108.42, Min: $75.25, Max: $150.00, Currencies: [USD]
[12:34:57.123] User: user_B, Window: 12:30:00 - 12:35:00, Count: 1, Total: $250.50, Avg: $250.50, Min: $250.50, Max: $250.50, Currencies: [EUR]
```

### 6. Query Results via KSQL CLI

You can also query results directly in KSQL:

```sql
-- Stream query (push query)
SELECT * FROM user_transaction_stats EMIT CHANGES;

-- Pull query for a specific user
SELECT * FROM user_transaction_stats WHERE user_id = 'user_A';
```

## Project Structure

This example includes three projects:

1. **HoppingWindowExample.csproj**: Demonstrates KSQL query generation with Ksql.Linq
   ```bash
   dotnet run --project HoppingWindowExample.csproj
   ```

2. **HoppingWindowProducer.csproj**: Generates and sends test transaction data
   ```bash
   dotnet run --project HoppingWindowProducer.csproj
   ```

3. **HoppingWindowConsumer.csproj**: Consumes and displays aggregated window results
   ```bash
   dotnet run --project HoppingWindowConsumer.csproj
   ```

## Complete Workflow

For a full end-to-end demo:

1. Start infrastructure: `docker-compose up -d`
2. Register schemas (see step 2 above)
3. Create KSQL stream and table (see step 3 above)
4. Terminal 1: Run the **Producer** to generate data
5. Terminal 2: Run the **Consumer** to see real-time aggregations
6. Optional: Run the **Example** to see KSQL query generation

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

## Producer Configuration Details

The producer is configured with best practices for production use:

```csharp
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9093",
    ClientId = "transaction-producer",
    // Enable idempotence for exactly-once semantics
    EnableIdempotence = true,
    // Retry settings
    MessageSendMaxRetries = 3,
    RetryBackoffMs = 1000,
    // Compression
    CompressionType = CompressionType.Snappy
};

var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:18081",
    RequestTimeoutMs = 30000,
    MaxCachedSchemas = 100
};

var avroSerializerConfig = new AvroSerializerConfig
{
    // Automatically register schemas if they don't exist
    AutoRegisterSchemas = true,
    // Use Topic strategy: creates subjects named {topic}-value
    // Key uses plain string (KAFKA format), so only value needs Avro
    SubjectNameStrategy = SubjectNameStrategy.Topic
};

// Build producer with String key and Avro value
using var producer = new ProducerBuilder<string, TransactionAvro>(producerConfig)
    .SetKeySerializer(Serializers.Utf8)  // Plain string for key
    .SetValueSerializer(new AvroSerializer<TransactionAvro>(schemaRegistry, avroSerializerConfig))
    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
    .Build();
```

**Key Configuration Points:**
- **KEY_FORMAT='KAFKA'**: Uses plain string for keys (transaction IDs)
- **VALUE_FORMAT='AVRO'**: Uses Avro for values with schema evolution
- **EnableIdempotence**: Prevents duplicate messages
- **AutoRegisterSchemas**: Automatically registers value schema (no key schema needed)
- **Graceful Shutdown**: Producer flushes remaining messages on Ctrl+C

## Consumer API Usage

### Real-time Stream Consumption (Push Query)

```csharp
await using var ctx = new HoppingWindowConsumerContext(config, loggerFactory);

// Subscribe to hopping window aggregations
await ctx.Set<UserTransactionStatsConsumer>()
    .ForEachAsync((stats, headers, meta) =>
    {
        Console.WriteLine($"User: {stats.UserId}, Count: {stats.TransactionCount}");
        return Task.CompletedTask;
    },
    cancellationToken: cancellationToken);
```

### Pull Queries (Point Lookups)

```csharp
// Query latest window for a specific user
var sql = @"
    SELECT user_id, transaction_count, total_amount
    FROM user_transaction_stats
    WHERE user_id = 'user_A'
    ORDER BY WINDOWSTART DESC
    LIMIT 1;";

var rows = await ctx.QueryRowsAsync(sql, TimeSpan.FromSeconds(5));
```

### Consumer Entity Definition

```csharp
[KsqlTopic("user_transaction_stats")]
public class UserTransactionStatsConsumer
{
    [KsqlKey(0)]
    public string UserId { get; set; }

    public DateTime WindowStart { get; set; }
    public DateTime WindowEnd { get; set; }
    public long TransactionCount { get; set; }
    public double TotalAmount { get; set; }
    // ... other properties
}
```

## Error Handling and Async Best Practices

### Producer Error Handling

The producer implements robust error handling:

```csharp
try
{
    var result = await producer.ProduceAsync(
        "transactions",
        new Message<string, TransactionAvro> { ... },
        cancellationToken);  // Pass CancellationToken
}
catch (ProduceException<string, TransactionAvro> ex)
{
    // Handle Kafka produce errors
    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
}
catch (OperationCanceledException)
{
    // Handle graceful shutdown
    break;
}
finally
{
    // Always flush before shutdown
    producer.Flush(TimeSpan.FromSeconds(10));
}
```

### Consumer Error Handling

The consumer handles cancellation and errors properly:

```csharp
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    await ctx.Set<UserTransactionStatsConsumer>()
        .ForEachAsync((stats, headers, meta) => { ... }, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Stopped gracefully");
}
```

### Key Async Patterns

1. **Always pass CancellationToken**: Enables graceful shutdown
2. **Flush before exit**: Ensures all messages are sent
3. **Handle ProduceException**: Kafka-specific errors
4. **Use using statements**: Proper resource disposal
5. **Catch OperationCanceledException**: Expected during shutdown

## Troubleshooting

### Schema Registry Issues

**Problem**: Producer fails with "Subject not found"
**Solution**: Ensure value schema is registered before starting producer:
```bash
curl http://localhost:18081/subjects/transactions-value/versions
```
Note: Key uses KAFKA (String) format, so no key schema registration needed.

**Problem**: "Failed to serialize" errors
**Solution**: Check that Avro schema matches C# class:
- Field names must match exactly (transaction_id, user_id, amount, currency, transaction_time)
- Field types must be compatible (double → double, string → string, long → long)

**Problem**: KSQL crashes when creating stream with `KEY_FORMAT='AVRO'`
**Solution**: Use `KEY_FORMAT='KAFKA'` for simple string keys. Avro keys are complex and rarely needed.

### Connection Issues

**Problem**: "Connection refused" to Kafka
**Solution**: Verify Docker containers are running:
```bash
docker-compose ps
docker-compose logs kafka
```

**Problem**: "Timeout" waiting for entity
**Solution**: Check KSQL table exists:
```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
SHOW TABLES;
DESCRIBE user_transaction_stats;
```

## Cleanup

```bash
docker-compose down -v
```

## Further Reading

- [KSQL Windowing Documentation](https://docs.ksqldb.io/en/latest/concepts/time-and-windows-in-ksqldb-queries/)
- [Hopping Window Use Cases](https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#hopping-time-windows)
- [Confluent Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Kafka Producer Best Practices](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#producer)
