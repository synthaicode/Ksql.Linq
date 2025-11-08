using Confluent.Kafka.Admin;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Runtime;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

public class ContinuationMinuteTimeBucketTests
{
    private const string KafkaBootstrap = "127.0.0.1:39092";
    private const string SchemaUrl = "http://127.0.0.1:18081";
    private const string KsqlUrl = "http://127.0.0.1:18088";

    [KsqlTopic("deduprates")]
    public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("barc_off")]
    public class BarOff
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    [KsqlTopic("barc_on")]
    public class BarOn
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    private sealed class CtxOff : KsqlContext
    {
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        public CtxOff() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = KafkaBootstrap },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = SchemaUrl },
            KsqlDbUrl = KsqlUrl
        }, _lf) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<BarOff>()
              .ToQuery(q => q.From<Rate>()
                .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new BarOff
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart(),
                    Open = g.EarliestByOffset(x => x.Bid),
                    High = g.Max(x => x.Bid),
                    Low = g.Min(x => x.Bid),
                    Close = g.LatestByOffset(x => x.Bid)
                }));
        }
    }

    private sealed class CtxOn : KsqlContext
    {
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        public CtxOn() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = KafkaBootstrap },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = SchemaUrl },
            KsqlDbUrl = KsqlUrl
        }, _lf) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<BarOn>()
              .ToQuery(q => q.From<Rate>()
                .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } }, continuation: true)
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new BarOn
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart(),
                    Open = g.EarliestByOffset(x => x.Bid),
                    High = g.Max(x => x.Bid),
                    Low = g.Min(x => x.Bid),
                    Close = g.LatestByOffset(x => x.Bid)
                }));
        }
    }

    private static DateTime BaseUtc() => new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private static async Task EnsureTopicAsync(string name)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaBootstrap }).Build();
        try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = name, NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Continuation_OFF_GapMinute_NoRow()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(KsqlUrl, TimeSpan.FromSeconds(180), graceMs: 2000);
        await EnsureTopicAsync("deduprates");
        await using var ctx = new CtxOff();

        var broker = "B1"; var symbol = "S1";
        var t0 = BaseUtc();
        // Seed first minute ticks (Open=100, High=105, Low=99, Close=101)
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(1), Bid = 100 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(20), Bid = 105 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(40), Bid = 99 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(55), Bid = 101 });

        // Wait short for materialization
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await Task.Delay(1000, cts.Token);

        var rows = await Ksql.Linq.TimeBucket.ReadAsync<BarOff>(ctx, Period.Minutes(1), new[] { broker, symbol }, cts.Token);
        Assert.Contains(rows, r => r.BucketStart == t0);
        Assert.DoesNotContain(rows, r => r.BucketStart == t0.AddMinutes(1));
    }

    [Fact(Skip = "Enable once continuation-based carry-forward is verified via TimeBucket")]
    [Trait("Category", "Integration")]
    public async Task Continuation_ON_GapMinute_Filled_With_PrevClose()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(KsqlUrl, TimeSpan.FromSeconds(180), graceMs: 2000);
        await EnsureTopicAsync("deduprates");
        await using var ctx = new CtxOn();

        var broker = "B1"; var symbol = "S1";
        var t0 = BaseUtc();
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(1), Bid = 100 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(20), Bid = 105 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(40), Bid = 99 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(55), Bid = 101 });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        // Poll until the filler minute is expected to appear
        BarOn? next = null;
        var deadline = DateTime.UtcNow.AddSeconds(110);
        while (DateTime.UtcNow < deadline)
        {
            var rows = await Ksql.Linq.TimeBucket.ReadAsync<BarOn>(ctx, Period.Minutes(1), new[] { broker, symbol }, cts.Token);
            next = rows.SingleOrDefault(r => r.BucketStart == t0.AddMinutes(1));
            if (next != null) break;
            await Task.Delay(1000, cts.Token);
        }
        Assert.NotNull(next); // gap minute is filled
        Assert.Equal(101d, next!.Open);
        Assert.Equal(101d, next.High);
        Assert.Equal(101d, next.Low);
        Assert.Equal(101d, next.Close);
    }
}


