using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;
using System.Linq;

using Ksql.Linq.Query.Builders.Statements;
namespace Ksql.Linq.Tests.Query.Analysis;

[Trait("Level", TestLevel.L3)]
public class TumblingToQueryEndToEndTests
{
    [KsqlTopic("rate")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        public System.DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class BarLive
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)]
        [KsqlTimestamp]
        public System.DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<BarLive>()
              .ToQuery(q => q.From<Rate>()
                .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1, 5 } })
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new BarLive
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart(),
                    Open = g.EarliestByOffset(x => x.Bid),
                    High = g.Max(x => x.Bid),
                    Low = g.Min(x => x.Bid),
                    KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                }));
        }
    }

    [Fact]
    public void BuildAll_Minutes_1m_5m_Live_From_ToQuery()
    {
        using var ctx = new TestContext();
        var em = ctx.GetEntityModels()[typeof(BarLive)];
        var model = em.QueryModel!;

        var map = KsqlCreateWindowedStatementBuilder.BuildAll(
            "bar",
            model,
            tf => $"bar_{tf}_live");

        Assert.True(map.ContainsKey("1m"));
        Assert.True(map.ContainsKey("5m"));

        var sql1m = map["1m"];
        var sql5m = map["5m"];

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql1m, "CREATE TABLE IF NOT EXISTS bar_1m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql1m, "WINDOW TUMBLING (SIZE 1 MINUTES)");

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql5m, "CREATE TABLE IF NOT EXISTS bar_5m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql5m, "WINDOW TUMBLING (SIZE 5 MINUTES)");

        // Aggregation fragments should appear
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql1m, "EARLIEST_BY_OFFSET");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql1m, "LATEST_BY_OFFSET");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql1m, "MAX(");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql1m, "MIN(");
    }
}


