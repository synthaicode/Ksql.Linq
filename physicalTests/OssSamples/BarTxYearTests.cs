using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Ksql.Linq.Core.Modeling;
using Xunit;
using PhysicalTestEnv;
using Xunit.Sdk;

namespace Ksql.Linq.Tests.Integration;

[Collection("KsqlExclusive")]
public class BarTxYearTests
{
    [KsqlTopic("deduprates")] 
    private sealed class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("bar")]
    private sealed class BarY
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] [KsqlTimestamp] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low  { get; set; }
        public double Close { get; set; }
        public int Year { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
        {
            // Surface detailed Streamiz + cache logs to console for diagnosis
            b.SetMinimumLevel(LogLevel.Trace);
            b.AddConsole();
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Trace);
            b.AddFilter("Ksql.Linq.Cache", LogLevel.Trace);
            b.AddFilter("Ksql.Linq.Infrastructure", LogLevel.Debug);
            b.AddFilter("Ksql.Linq.Runtime", LogLevel.Debug);
        });

        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://127.0.0.1:18081" },
            KsqlDbUrl = Environment.GetEnvironmentVariable("KSQLDB_URL") ?? "http://127.0.0.1:18088"
        }, _lf) { }

        public EventSet<Rate> Rates { get; set; } = null!;
        protected override bool SkipSchemaRegistration => false;

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BarY>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new BarY
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low  = g.Min(x => x.Bid),
                        Close = g.LatestByOffset(x => x.Bid),
                        Year = Sql.Year(g.WindowStart())
                    }));
        }
    }

    [KsqlTopic("bar_1m_live")]
    private sealed class Bar1mLiveEvent
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low  { get; set; }
        public double Close { get; set; }
        public int Year { get; set; }
    }

    private static class Sql
    {
        public static int Year(DateTime dt) => dt.Year;
    }

    [Fact]
    public async Task Min1_Smoke()
    {
        // Enable SerDes diagnostics for windowed keys
        Environment.SetEnvironmentVariable("PHYS_SERDES_DIAG", "1");
        await KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
        await EnsureKafkaTopicAsync("deduprates");

        // Attach runtime event sink to observe DDL/query readiness
        var sink = new PhysicalTestEnv.TestRuntimeEventSink();
        Ksql.Linq.Events.RuntimeEventBus.SetSink(sink);

        await using var ctx = new TestContext();

        var t0 = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var broker = "brk"; var symbol = "sym";

        // Removed: derived query RUNNING wait (query.run)

        // ForEachAsync 隕ｳ貂ｬ縺ｯ陦後ｏ縺ｪ縺・ｼ井ｻ墓ｧ倅ｸ翫・繧ｷ繝ｳ繝励ΝDDL縺ｯ謚第ｭ｢縺励↑縺・ｼ・
        // Produce ticks in the same minute (after starting observer to ensure we catch the changelog)
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(5),  Bid = 1.20 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(25), Bid = 1.28 });
        // Nudge CTAS to emit again in the same minute (update close)
        await Task.Delay(200);
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(30), Bid = 1.30 });

        // Removed: event-driven cache readiness and SerDes diagnostics waits

        // Prefer unified TimeBucket.ReadAsync which encapsulates wait + REST fallback
        var wait = TimeSpan.FromSeconds(600);
        using var cts = new CancellationTokenSource(wait);
        // Keyless probe: take a snapshot from cache to confirm RocksDB arrival (short timeout)
        using (var probeCts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            var probeList = await Ksql.Linq.TimeBucket
                .Get<BarY>(ctx, Ksql.Linq.Runtime.Period.Minutes(1))
                .ToListAsync(probeCts.Token);
            Console.WriteLine($"[test.probe] cache.rows={probeList.Count}");
        }
        var tolerance = TimeSpan.FromSeconds(1);
        var rows = await Ksql.Linq.TimeBucket.ReadAsync<BarY>(
            ctx,
            Ksql.Linq.Runtime.Period.Minutes(1),
            new[] { broker, symbol },
            t0,
            tolerance,
            cts.Token);

        var row = rows.SingleOrDefault(r => Math.Abs((r.BucketStart - t0).TotalSeconds) <= tolerance.TotalSeconds);
        Assert.NotNull(row);
        Assert.Equal(2025, row!.Year);

        await KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
    }

    private static async Task EnsureKafkaTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "127.0.0.1:39092";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            var md = admin.GetMetadata(topicName, TimeSpan.FromSeconds(2));
            if (md?.Topics != null && md.Topics.Any(t => string.Equals(t.Topic, topicName, StringComparison.OrdinalIgnoreCase) && t.Error.Code == ErrorCode.NoError))
                return;
        }
        catch { }
        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topicName, NumPartitions = partitions, ReplicationFactor = replicationFactor }
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex)
        {
            if (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)) return;
            throw;
        }
    }
}