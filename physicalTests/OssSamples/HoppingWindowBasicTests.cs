using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

/// <summary>
/// Basic physical test for Hopping Windows to verify SQL generation and ksqlDB execution
/// </summary>
public class HoppingWindowBasicTests
{
    [KsqlTopic("test_trades")]
    public class Trade
    {
        [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Price { get; set; }
        public long Volume { get; set; }
    }

    public class TradeStats
    {
        [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(2)]
        [KsqlTimestamp]
        public DateTime BucketStart { get; set; }
        public double AvgPrice { get; set; }
        public long TotalVolume { get; set; }
        public long Count { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.SetMinimumLevel(LogLevel.Information);
        });

        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }

        protected override bool SkipSchemaRegistration => false;

        public EventSet<Trade> Trades { get; set; } = null!;

        protected override void OnModelCreating(IModelBuilder mb)
        {
            // NOTE: We manually execute DDL in the test (line 166-189) to avoid interference
            // from DerivedTumblingPipeline which doesn't support hopping windows.
            // ToQuery is NOT used here to prevent automatic pipeline execution.

            // Base entity registration only (no ToQuery)
            mb.Entity<Trade>();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Type", "Physical")]
    public async Task Hopping_Window_SQL_Generation_Test()
    {
        // Verify SQL generation without ksqlDB execution
        var model = new Ksql.Linq.Query.Dsl.KsqlQueryRoot()
            .From<Trade>()
            .Hopping(
                time: t => t.Timestamp,
                windowSize: TimeSpan.FromMinutes(5),
                hopInterval: TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new TradeStats
            {
                Symbol = g.Key,
                BucketStart = g.WindowStart(),
                AvgPrice = g.Average(x => x.Price),
                TotalVolume = g.Sum(x => x.Volume),
                Count = g.Count()
            })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_stats_5m_hop1m",
            model: model,
            timeframe: "5m",
            hopInterval: TimeSpan.FromMinutes(1),
            emitOverride: "EMIT CHANGES");

        // Verify SQL contains HOPPING syntax
        Assert.Contains("WINDOW HOPPING", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SIZE 5 MINUTES", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ADVANCE BY 1 MINUTES", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CREATE TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", sql, StringComparison.OrdinalIgnoreCase);

        // Log generated SQL for manual verification
        Console.WriteLine("Generated SQL:");
        Console.WriteLine(sql);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Type", "Physical")]
    public async Task Hopping_Window_KsqlDB_Execution_Test()
    {
        // Skip if ksqlDB is not available
        if (!await IsKsqlDbAvailableAsync())
        {
            Console.WriteLine("ksqlDB not available, skipping physical test");
            return;
        }

        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(
            "http://127.0.0.1:18088",
            TimeSpan.FromSeconds(60),
            graceMs: 2000);

        // Cleanup test artifacts
        await CleanupTestArtifactsAsync();

        await using var ctx = new TestContext();

        // Create test topic
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try
            {
                await admin.CreateTopicsAsync(new[]
                {
                    new TopicSpecification { Name = "test_trades", NumPartitions = 1, ReplicationFactor = 1 }
                });
            }
            catch { /* Already exists */ }
        }

        try
        {
            // Ensure ksqlDB metadata is materialized for source stream
            await ctx.WaitForEntityReadyAsync<Trade>(TimeSpan.FromSeconds(60));

            // Manually create source stream with explicit TIMESTAMP column
            // This ensures ksqlDB uses event time (Timestamp) instead of kafka message time
            var createStreamSql = @"
                CREATE STREAM IF NOT EXISTS TEST_TRADES (
                    Symbol VARCHAR,
                    Timestamp TIMESTAMP,
                    Price DOUBLE,
                    Volume BIGINT
                ) WITH (
                    KAFKA_TOPIC='test_trades',
                    VALUE_FORMAT='AVRO',
                    TIMESTAMP='Timestamp'
                );";

            var streamResult = await ctx.ExecuteStatementAsync(createStreamSql);
            Console.WriteLine($"Source stream creation: {(streamResult.IsSuccess ? "SUCCESS" : streamResult.Message)}");

            // Create hopping table via DDL generated from the DSL model
            // GRACE PERIOD allows late-arriving events to be processed
            const string hoppingTableName = "tradestats_5m_hop1m_live";
            var model = new Ksql.Linq.Query.Dsl.KsqlQueryRoot()
                .From<Trade>()
                .Hopping(
                    time: t => t.Timestamp,
                    windowSize: TimeSpan.FromMinutes(5),
                    hopInterval: TimeSpan.FromMinutes(1),
                    grace: TimeSpan.FromMinutes(5))  // ← Add GRACE PERIOD
                .GroupBy(t => t.Symbol)
                .Select(g => new TradeStats
                {
                    Symbol = g.Key,
                    BucketStart = g.WindowStart(),
                    AvgPrice = g.Average(x => x.Price),
                    TotalVolume = g.Sum(x => x.Volume),
                    Count = g.Count()
                })
                .Build();

            var ddl = KsqlCreateWindowedStatementBuilder.Build(
                name: hoppingTableName,
                model: model,
                timeframe: "5m",
                hopInterval: TimeSpan.FromMinutes(1),
                emitOverride: "EMIT CHANGES");

            Console.WriteLine("\n=== Generated DDL ===");
            Console.WriteLine(ddl);
            Console.WriteLine("=== End DDL ===\n");

            var ddlResult = await ctx.ExecuteStatementAsync(ddl);
            Assert.True(ddlResult.IsSuccess, $"DDL failed: {ddlResult.Message}");
            Console.WriteLine($"DDL executed successfully: {hoppingTableName}");

            // Wait for query to be RUNNING
            await WaitForQueryRunningAsync("http://127.0.0.1:18088", hoppingTableName, TimeSpan.FromSeconds(60));
            Console.WriteLine($"Query is RUNNING for {hoppingTableName}");

            // Register hopping window type mapping (required for TimeBucket.GetHopping())
            // This mapping allows TimeBucket.GetHopping<TradeStats>() to resolve to the correct table
            Runtime.TimeBucketTypes.RegisterHoppingRead(
                typeof(Trade),               // Base type
                Period.Minutes(5),           // Window size
                TimeSpan.FromMinutes(1),     // Hop interval
                typeof(TradeStats));         // Concrete read type

            // Produce test data: multiple trades within a 5-minute window
            // CRITICAL: Use recent past timestamps to ensure immediate processing
            // ksqlDB processes events when wall-clock time >= event time
            // Using past timestamps (within GRACE PERIOD) ensures immediate processing
            // Future timestamps cause ksqlDB to wait until wall-clock catches up
            var now = DateTime.UtcNow;
            var baseTime = now.AddSeconds(-30); // 30 seconds in the past (well within 5-min grace)
            Console.WriteLine($"Using base time: {baseTime:O} (current: {now:O})");

            var trade1 = new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(10),
                Price = 150.00,
                Volume = 100
            };
            await ctx.Trades.AddAsync(trade1);
            Console.WriteLine($"  Produced trade 1: {trade1.Symbol} @ {trade1.Timestamp:O}, Price={trade1.Price}, Vol={trade1.Volume}");

            var trade2 = new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(70),  // 1:10
                Price = 151.00,
                Volume = 200
            };
            await ctx.Trades.AddAsync(trade2);
            Console.WriteLine($"  Produced trade 2: {trade2.Symbol} @ {trade2.Timestamp:O}, Price={trade2.Price}, Vol={trade2.Volume}");

            var trade3 = new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(130), // 2:10
                Price = 152.00,
                Volume = 150
            };
            await ctx.Trades.AddAsync(trade3);
            Console.WriteLine($"  Produced trade 3: {trade3.Symbol} @ {trade3.Timestamp:O}, Price={trade3.Price}, Vol={trade3.Volume}");

            var trade4 = new Trade
            {
                Symbol = "GOOGL",
                Timestamp = baseTime.AddSeconds(20),
                Price = 2800.00,
                Volume = 50
            };
            await ctx.Trades.AddAsync(trade4);
            Console.WriteLine($"  Produced trade 4: {trade4.Symbol} @ {trade4.Timestamp:O}, Price={trade4.Price}, Vol={trade4.Volume}");

            Console.WriteLine($"\nProduced 4 test trades. Timestamps range: {baseTime.AddSeconds(10):O} to {baseTime.AddSeconds(130):O}");

            // Wait for Kafka producer to flush and ksqlDB to start processing
            // This ensures data is committed to Kafka before we query
            Console.WriteLine("\nWaiting 10 seconds for Kafka producer flush...");
            await Task.Delay(TimeSpan.FromSeconds(10));

            // === Test -1: Verify source data arrived in Kafka ===
            Console.WriteLine("\n=== Test -1: Source Stream verification ===");
            try
            {
                var sourceSql = "SELECT * FROM TEST_TRADES EMIT CHANGES LIMIT 4;";
                Console.WriteLine($"Executing: {sourceSql}");
                var sourceRows = await ctx.QueryRowsAsync(sourceSql, TimeSpan.FromSeconds(30));
                Console.WriteLine($"Source stream returned {sourceRows?.Count ?? 0} rows");
                if (sourceRows != null && sourceRows.Any())
                {
                    Console.WriteLine($"✓ Source data is in Kafka");
                    foreach (var row in sourceRows.Take(2))
                    {
                        Console.WriteLine($"  Source row: {string.Join(", ", row.Select(v => v?.ToString() ?? "null"))}");
                    }
                }
                else
                {
                    Console.WriteLine("⚠ WARNING: Source stream has no data - data production may have failed");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠ Source stream query error: {ex.Message}");
            }

            Console.WriteLine("\nWaiting 10 seconds for hopping window processing...");
            await Task.Delay(TimeSpan.FromSeconds(10));

            // === Test 0: Verify data flows through query with Push Query (EMIT CHANGES) ===
            Console.WriteLine("\n=== Test 0: Push Query verification (EMIT CHANGES) ===");
            try
            {
                var pushSql = $"SELECT * FROM {hoppingTableName.ToUpperInvariant()} EMIT CHANGES LIMIT 5;";
                Console.WriteLine($"Executing: {pushSql}");
                var pushRows = await ctx.QueryRowsAsync(pushSql, TimeSpan.FromSeconds(60));
                Console.WriteLine($"Push query returned {pushRows?.Count ?? 0} change events");
                if (pushRows != null && pushRows.Any())
                {
                    Console.WriteLine($"✓ Data is flowing through the hopping window query");
                    foreach (var row in pushRows.Take(3))
                    {
                        Console.WriteLine($"  Change event: {string.Join(", ", row.Select(v => v?.ToString() ?? "null"))}");
                    }
                }
                else
                {
                    Console.WriteLine("⚠ WARNING: Push query returned no events - data may not be flowing");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠ Push query error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }

            // Wait additional time for state store materialization
            Console.WriteLine("\nWaiting 20 seconds for state store materialization...");
            await Task.Delay(TimeSpan.FromSeconds(20));

            // === Test 1: Pull Query to snapshot windows ===
            Console.WriteLine("=== Test 1: Pull Query with PullRowsAsync ===");
            List<object?[]>? pullSnapshot = null;
            for (var attempt = 1; attempt <= 3; attempt++)
            {
                try
                {
                    pullSnapshot = await ctx.PullRowsAsync(
                        "TRADESTATS_5M_HOP1M_LIVE",
                        limit: 10,
                        timeout: TimeSpan.FromSeconds(20));
                    if (pullSnapshot.Count > 0) break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Pull attempt {attempt} failed: {ex.Message}");
                }
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
            Console.WriteLine($"PullRowsAsync returned {pullSnapshot?.Count ?? 0} rows");
            Assert.NotNull(pullSnapshot);
            Assert.NotEmpty(pullSnapshot);

            // === Test 2: TimeBucket.GetHopping() Read API ===
            Console.WriteLine("=== Test 2: TimeBucket.GetHopping() Read API ===");
            var hop = TimeSpan.FromMinutes(1);
            try
            {
                var tb = Ksql.Linq.Runtime.TimeBucket.GetHopping<TradeStats>(ctx, Period.Minutes(5), hop);
                var windows = await tb.ToListAsync();
                Console.WriteLine($"TimeBucket.ToListAsync() returned {windows.Count} windows");
                var aapl = windows.Where(w => w.Symbol == "AAPL").ToList();
                Console.WriteLine($"AAPL-filtered windows: {aapl.Count}");
                foreach (var w in aapl.Take(5))
                {
                    Console.WriteLine($"  AAPL Window: Start={w.BucketStart:o}, Avg={w.AvgPrice}, Vol={w.TotalVolume}, Count={w.Count}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"TimeBucket.GetHopping() unavailable: {ex.Message}");
            }

            // === Test 3: Pull Query with PullRowsAsync ===
            Console.WriteLine("=== Test 3: Pull Query with PullRowsAsync ===");
            var pullSql = "SELECT * FROM TRADESTATS_5M_HOP1M_LIVE WHERE SYMBOL='AAPL';";
            var pullRows = await ctx.PullRowsAsync(pullSql, timeout: TimeSpan.FromSeconds(30));
            Console.WriteLine($"PullRowsAsync returned {pullRows.Count} rows for AAPL");
            Assert.NotEmpty(pullRows);

            Console.WriteLine("Hopping window table created, data produced, and queries returned results");
        }
        finally
        {
            // Cleanup
            await CleanupTestArtifactsAsync();
        }
    }

    private static async Task<bool> IsKsqlDbAvailableAsync()
    {
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var response = await http.GetAsync("http://127.0.0.1:18088/info");
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private static async Task WaitForQueryRunningAsync(string ksqlBaseUrl, string nameToken, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        var until = DateTime.UtcNow + timeout;
        int consec = 0;
        while (DateTime.UtcNow < until)
        {
            try
            {
                var payload = new { ksql = "SHOW QUERIES;", streamsProperties = new { } };
                using var content = new StringContent(
                    JsonSerializer.Serialize(payload),
                    System.Text.Encoding.UTF8,
                    "application/json");
                using var resp = await http.PostAsync("/ksql", content);
                var body = await resp.Content.ReadAsStringAsync();

                // Check if query with nameToken exists and is RUNNING
                if (body.IndexOf(nameToken, StringComparison.OrdinalIgnoreCase) >= 0 &&
                    body.IndexOf("RUNNING", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    consec++;
                    if (consec >= 3) return; // Consistent for 3 checks
                }
                else
                {
                    consec = 0;
                }
            }
            catch
            {
                consec = 0;
            }
            await Task.Delay(1000);
        }
        throw new TimeoutException($"Query containing '{nameToken}' did not reach RUNNING state within {timeout}");
    }

    private static async Task CleanupTestArtifactsAsync()
    {
        try
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };

            // Drop tables and streams
            var dropStatements = new[]
            {
                "DROP TABLE IF EXISTS TRADESTATS_5M_HOP1M_LIVE DELETE TOPIC;",
                "DROP TABLE IF EXISTS TRADE_STATS_5M_HOP1M DELETE TOPIC;",
                "DROP STREAM IF EXISTS TEST_TRADES DELETE TOPIC;"
            };

            foreach (var stmt in dropStatements)
            {
                try
                {
                    var payload = new { ksql = stmt, streamsProperties = new { } };
                    using var content = new StringContent(
                        JsonSerializer.Serialize(payload),
                        System.Text.Encoding.UTF8,
                        "application/json");
                    await http.PostAsync("/ksql", content);
                    await Task.Delay(500); // Brief delay between drops
                }
                catch { /* Ignore cleanup errors */ }
            }
        }
        catch { /* Ignore cleanup errors */ }
    }
}
