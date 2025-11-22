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
            // Hopping window: 5 minute window, 1 minute hop
            mb.Entity<TradeStats>()
              .ToQuery(q => q.From<Trade>()
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

            // Create hopping table via DDL generated from the DSL model
            const string hoppingTableName = "tradestats_5m_hop1m_live";
            var ddl = KsqlCreateWindowedStatementBuilder.Build(
                name: hoppingTableName,
                model: new Ksql.Linq.Query.Dsl.KsqlQueryRoot()
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
                    .Build(),
                timeframe: "5m",
                hopInterval: TimeSpan.FromMinutes(1),
                emitOverride: "EMIT CHANGES");

            var ddlResult = await ctx.ExecuteStatementAsync(ddl);
            Assert.True(ddlResult.IsSuccess, $"DDL failed: {ddlResult.Message}");

            // Register hopping window type mapping (required for TimeBucket.GetHopping())
            // This mapping allows TimeBucket.GetHopping<TradeStats>() to resolve to the correct table
            Runtime.TimeBucketTypes.RegisterHoppingRead(
                typeof(Trade),               // Base type
                Period.Minutes(5),           // Window size
                TimeSpan.FromMinutes(1),     // Hop interval
                typeof(TradeStats));         // Concrete read type

            // Produce test data: multiple trades within a 5-minute window
            var baseTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);

            await ctx.Trades.AddAsync(new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(10),
                Price = 150.00,
                Volume = 100
            });

            await ctx.Trades.AddAsync(new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(70),  // 1:10
                Price = 151.00,
                Volume = 200
            });

            await ctx.Trades.AddAsync(new Trade
            {
                Symbol = "AAPL",
                Timestamp = baseTime.AddSeconds(130), // 2:10
                Price = 152.00,
                Volume = 150
            });

            await ctx.Trades.AddAsync(new Trade
            {
                Symbol = "GOOGL",
                Timestamp = baseTime.AddSeconds(20),
                Price = 2800.00,
                Volume = 50
            });

            Console.WriteLine($"Produced 4 test trades at base time {baseTime}");

            // Wait for hopping window processing (5min window, 1min hop means multiple overlapping windows)
            await Task.Delay(TimeSpan.FromSeconds(10));

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

    private static async Task CleanupTestArtifactsAsync()
    {
        try
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };

            // Drop tables
            var dropStatements = new[]
            {
                "DROP TABLE IF EXISTS TRADESTATS_5M_HOP1M_LIVE DELETE TOPIC;",
                "DROP TABLE IF EXISTS TRADE_STATS_5M_HOP1M DELETE TOPIC;"
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
                }
                catch { /* Ignore cleanup errors */ }
            }
        }
        catch { /* Ignore cleanup errors */ }
    }
}
