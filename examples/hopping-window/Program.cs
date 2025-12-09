using System;
using System.Linq;
using Ksql.Linq;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Builders.Statements;

namespace HoppingWindowExample;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("=== Ksql.Linq Hopping Window Example ===\n");

        // Example 1: Create a hopping window query using the Ksql.Linq fluent API
        Console.WriteLine("Example 1: Creating a Hopping Window Query with Ksql.Linq");
        Console.WriteLine("-----------------------------------------------------------");

        var query = new KsqlQueryable<Transaction>()
            .Hopping(
                time: t => t.TransactionTime,
                windows: new HoppingWindows
                {
                    Size = 5,
                    AdvanceBy = 1,
                    Unit = "MINUTES"
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

        var model = query.Build();

        // Generate the KSQL CREATE TABLE statement
        var ksql = KsqlCreateWindowedStatementBuilder.Build(
            "user_transaction_stats",
            model,
            "5m");

        Console.WriteLine("Generated KSQL:");
        Console.WriteLine(ksql);
        Console.WriteLine();

        // Example 2: Show the actual KSQL statements needed for the complete setup
        Console.WriteLine("\nExample 2: Complete KSQL Setup for Hopping Window");
        Console.WriteLine("---------------------------------------------------");

        ShowCompleteKsqlSetup();

        Console.WriteLine("\n=== Example Complete ===");
    }

    static void ShowCompleteKsqlSetup()
    {
        Console.WriteLine(@"
-- Step 1: Create the source stream
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

-- Step 2: Create a hopping window table for aggregation
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

-- Step 3: Query the hopping window table
SELECT * FROM user_transaction_stats EMIT CHANGES;

-- Step 4: Insert sample data
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
");
    }
}
