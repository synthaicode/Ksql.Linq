using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Builders.Statements;
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
        public DateTime WindowStart { get; set; }
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
                    WindowStart = g.WindowStart(),
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
                WindowStart = g.WindowStart(),
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
            // Execute EnsureCreatedAsync which should create the hopping window table
            await ctx.EnsureCreatedAsync();

            // Verify table was created by checking ksqlDB
            var tables = await ctx.ListTablesAsync();
            Assert.Contains(tables, t => t.Contains("TRADE", StringComparison.OrdinalIgnoreCase));

            // Produce test data
            var testTrade = new Trade
            {
                Symbol = "AAPL",
                Timestamp = DateTime.UtcNow,
                Price = 150.50,
                Volume = 1000
            };

            await ctx.Trades.AddAsync(testTrade);
            await Task.Delay(2000); // Allow time for processing

            Console.WriteLine("Hopping window table created and test data produced successfully");
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
                "DROP TABLE IF EXISTS TRADESTATS DELETE TOPIC;",
                "DROP TABLE IF EXISTS TRADE_STATS_5M_HOP1M DELETE TOPIC;"
            };

            foreach (var stmt in dropStatements)
            {
                try
                {
                    var content = new System.Text.Json.JsonSerializer().SerializeToUtf8Bytes(new { ksql = stmt });
                    await http.PostAsync("/ksql", new ByteArrayContent(content));
                }
                catch { /* Ignore cleanup errors */ }
            }
        }
        catch { /* Ignore cleanup errors */ }
    }
}
