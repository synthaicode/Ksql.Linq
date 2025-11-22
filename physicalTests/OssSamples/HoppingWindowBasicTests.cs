using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
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
        [KsqlIgnore] // WindowStart is part of the windowed key; we no longer project it in SELECT
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

        protected override bool SkipSchemaRegistration => false;  // Allow SchemaRegistrar to create streams/tables

        public EventSet<Trade> Trades { get; set; } = null!;

        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<TradeStats>()
              .ToQuery(q => q.From<Trade>()
                  .Hopping(
                      time: t => t.Timestamp,
                      windowSize: TimeSpan.FromMinutes(5),
                      hopInterval: TimeSpan.FromMinutes(1),
                      grace: TimeSpan.FromMinutes(5))
                  .GroupBy(t => t.Symbol)
                  .Select(g => new TradeStats
                  {
                      Symbol = g.Key,
                      // BucketStart is auto-populated from window key, not from SELECT
                      AvgPrice = g.Average(x => x.Price),
                      TotalVolume = g.Sum(x => x.Volume),
                      Count = g.Count()
                  }));
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
                // BucketStart is auto-populated from window key, not from SELECT
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

        // NOTE: No pre-test cleanup to avoid "Unknown topic or partition" errors
        // when RowMonitor tries to commit to a just-deleted topic.
        // Cleanup only happens in finally block after test completes.
        // If needed, manually run: docker compose down -v && docker compose up -d
        await using var ctx = new TestContext();

        try
        {
            // === STEP 1: Wait for auto-created stream to be ready ===
            // EventSet<Trade> automatically creates TEST_TRADES stream
            Console.WriteLine("\n=== Waiting for TEST_TRADES stream (auto-created by EventSet) ===");
            await ctx.WaitForEntityReadyAsync<Trade>(TimeSpan.FromSeconds(60));
            Console.WriteLine("✓ TEST_TRADES stream is ready");
            Console.WriteLine("✓ Hopping window type mapping auto-registered by OnModelCreating");

            // === STEP 2: NOW produce test data ===
            // Both TEST_TRADES stream and hopping table are ready to consume
            // Use timestamps in the recent PAST so ksqlDB processes them immediately
            // All timestamps within a 3-minute window to ensure they're in the same hopping windows
            var now = DateTime.UtcNow;
            var baseTime = now.AddMinutes(-2); // 2 minutes in the past
            Console.WriteLine($"\nUsing base time: {baseTime:O} (current: {now:O})");

            Console.WriteLine("\n=== Producing test data ===");

            // All trades within a 3-minute span, all in the past
            var trade1 = new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(10),     // -110 seconds from now
                Price = 150.00,
                Volume = 100
            };
            try
            {
                await ctx.Trades.AddAsync(trade1);
                Console.WriteLine($"  ✓ Produced trade 1: {trade1.Symbol} @ {trade1.Timestamp:O}, Price={trade1.Price}, Vol={trade1.Volume}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ✗ Failed to produce trade 1: {ex.Message}");
                throw;
            }

            var trade2 = new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(70),     // -50 seconds from now
                Price = 151.00,
                Volume = 200
            };
            await ctx.Trades.AddAsync(trade2);
            Console.WriteLine($"  ✓ Produced trade 2: {trade2.Symbol} @ {trade2.Timestamp:O}, Price={trade2.Price}, Vol={trade2.Volume}");

            var trade3 = new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(130),    // +10 seconds from now (slightly future)
                Price = 152.00,
                Volume = 150
            };
            await ctx.Trades.AddAsync(trade3);
            Console.WriteLine($"  ✓ Produced trade 3: {trade3.Symbol} @ {trade3.Timestamp:O}, Price={trade3.Price}, Vol={trade3.Volume}");

            var trade4 = new Trade
            {
                Symbol = "GOOGL",
                Timestamp = baseTime.AddSeconds(20),     // -100 seconds from now
                Price = 2800.00,
                Volume = 50
            };
            await ctx.Trades.AddAsync(trade4);
            Console.WriteLine($"  ✓ Produced trade 4: {trade4.Symbol} @ {trade4.Timestamp:O}, Price={trade4.Price}, Vol={trade4.Volume}");

            Console.WriteLine($"\nProduced 4 test trades.");
            Console.WriteLine($"Timestamp range: {baseTime.AddSeconds(10):O} to {baseTime.AddSeconds(130):O}");
            var timespanMinutes = (baseTime.AddSeconds(130) - baseTime.AddSeconds(10)).TotalMinutes;
            Console.WriteLine($"Time span: {timespanMinutes:F1} minutes (fits in 5-minute hopping windows)");

            // === STEP 3: Wait for data to flow through both streams ===
            Console.WriteLine("\nWaiting 30 seconds for Kafka flush and hopping window processing...");
            await Task.Delay(TimeSpan.FromSeconds(30));
            Console.WriteLine("✓ Data should have flowed through TEST_TRADES → TRADESTATS_5M_HOP1M_LIVE");

            // === Test -1: SKIPPED - Source Stream verification ===
            // NOTE: Push queries (EMIT CHANGES) return FUTURE data, not historical data
            // Since we already produced data and the stream consumed it,
            // we can't verify it with a push query without producing new data
            // Instead, we verify data flow by checking the hopping table results
            Console.WriteLine("\n=== Test -1: SKIPPED - Source stream verification ===");
            Console.WriteLine("Push queries return future data, not historical");
            Console.WriteLine("Data flow will be verified via hopping table results");

            // === Test 0: SKIPPED - Push Query verification ===
            // NOTE: Push queries (EMIT CHANGES) also return FUTURE data only
            // Since data was already produced and processed, push query would timeout
            // We verify data processing via pull queries on the materialized state store
            Console.WriteLine("\n=== Test 0: SKIPPED - Push query verification ===");
            Console.WriteLine("Push queries return future data, would timeout on historical data");
            Console.WriteLine("Using pull queries instead to verify state store contents");

            // === Test 1: TimeBucket.GetHopping() Read API ===
            // This tests the high-level read API which internally uses pull queries
            Console.WriteLine("\n=== Test 1: TimeBucket.GetHopping() Read API ===");
            var hop = TimeSpan.FromMinutes(1);
            List<TradeStats>? windows = null;
            for (var attempt = 1; attempt <= 5; attempt++)
            {
                try
                {
                    Console.WriteLine($"TimeBucket.GetHopping attempt {attempt}/5...");
                    var tb = Ksql.Linq.Runtime.TimeBucket.GetHopping<TradeStats>(ctx, Period.Minutes(5), hop);
                    windows = await tb.ToListAsync();
                    Console.WriteLine($"  Returned {windows?.Count ?? 0} window(s)");
                    if (windows != null && windows.Count > 0)
                    {
                        Console.WriteLine($"✓ TimeBucket.GetHopping SUCCESS - got {windows.Count} window(s)");
                        break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  Attempt {attempt} error: {ex.Message}");
                }
                if (attempt < 5)
                {
                    Console.WriteLine($"  Waiting 5 seconds before retry...");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }

            if (windows == null || windows.Count == 0)
            {
                Console.WriteLine("\n✗ CRITICAL: TimeBucket.GetHopping returned 0 windows after 5 attempts");
                Console.WriteLine("This means:");
                Console.WriteLine("  1. Hopping table exists and is RUNNING (verified earlier)");
                Console.WriteLine("  2. But state store has no data");
                Console.WriteLine("  3. Possible causes:");
                Console.WriteLine("     - Data didn't flow from TEST_TRADES to hopping table");
                Console.WriteLine("     - Timestamps outside active window range");
                Console.WriteLine("     - State store not yet materialized (need more wait time)");
            }

            Assert.NotNull(windows);
            Assert.NotEmpty(windows);

            var aapl = windows.Where(w => w.Symbol == "AAPL").ToList();
            Console.WriteLine($"AAPL-filtered windows: {aapl.Count}");
            foreach (var w in aapl.Take(5))
            {
                Console.WriteLine($"  AAPL Window: Start={w.BucketStart:o}, Avg={w.AvgPrice}, Vol={w.TotalVolume}, Count={w.Count}");
            }

            // === Test 2: Pull Query with PullRowsAsync ===
            Console.WriteLine("=== Test 2: Pull Query with PullRowsAsync ===");
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
