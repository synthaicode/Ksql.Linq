using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Runtime;
using Microsoft.Extensions.Logging;
using Confluent.Kafka.Admin;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

public class ContinuationMinuteTests
{
    [KsqlTopic("rates_cont")] public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        [KsqlDecimal(18,2)] public decimal Bid { get; set; }
    }

    public class Bar
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] [KsqlTimestamp] public DateTime BucketStart { get; set; }
        [KsqlDecimal(18,4)] public decimal Open { get; set; }
        [KsqlDecimal(18,4)] public decimal High { get; set; }
        [KsqlDecimal(18,4)] public decimal Low { get; set; }
        [KsqlDecimal(18,4)] public decimal Close { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.SetMinimumLevel(LogLevel.Information);
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _lf) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<Bar>().ToQuery(q => q
                .From<Rate>()
                .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } }, continuation: false)
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new Bar
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

    private static DateTime Utc(int y, int M, int d, int h, int m, int s = 0)
        => DateTime.SpecifyKind(new DateTime(y, M, d, h, m, s), DateTimeKind.Utc);

    [Fact(Skip = "Continuation read-time carry-forward not implemented yet")]
    [Trait("Category", "Integration")]
    public async Task ContinuationOff_MissingMinute_HasNoRow()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        var ctx = new TestContext();
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "rates_cont", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
        }

        var baseTs = Utc(2025, 1, 1, 12, 0, 0);
        await ctx.Rates.AddAsync(new Rate { Broker = "B", Symbol = "S", Timestamp = baseTs, Bid = 100m });
        await ctx.Rates.AddAsync(new Rate { Broker = "B", Symbol = "S", Timestamp = baseTs.AddMinutes(2), Bid = 101m });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { "B", "S" }, cts.Token);
        Assert.DoesNotContain(list, b => b.BucketStart == baseTs.AddMinutes(1));
    }

    [Fact(Skip = "Continuation read-time carry-forward not implemented yet")]
    [Trait("Category", "Integration")]
    public async Task ContinuationOn_MissingMinute_FilledWithPrevClose()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        var ctx = new TestContextContinuationOn();
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "rates_cont", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
        }

        var baseTs = Utc(2025, 1, 1, 12, 0, 0);
        await ctx.Rates.AddAsync(new Rate { Broker = "B", Symbol = "S", Timestamp = baseTs, Bid = 100m });
        await ctx.Rates.AddAsync(new Rate { Broker = "B", Symbol = "S", Timestamp = baseTs.AddMinutes(2), Bid = 101m });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { "B", "S" }, cts.Token);
        var mid = Assert.Single(list.Where(b => b.BucketStart == baseTs.AddMinutes(1)));
        Assert.Equal(100m, mid.Open);
        Assert.Equal(100m, mid.High);
        Assert.Equal(100m, mid.Low);
        Assert.Equal(100m, mid.Close);
    }

    private sealed class TestContextContinuationOn : TestContext
    {
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<Bar>().ToQuery(q => q
                .From<Rate>()
                .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } }, continuation: true)
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new Bar
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
}


