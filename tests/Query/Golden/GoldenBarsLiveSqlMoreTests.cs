using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using System.Linq;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

public class GoldenBarsLiveSqlMoreTests
{
    [KsqlTopic("rate")] private class Rate
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
        [KsqlKey(3)] public System.DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private static KsqlQueryModel BuildModel()
    {
        return new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 15, 60 } })
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
            })
            .Build();
    }

    [Fact]
    public void Bars15mLive_Equals_Golden()
    {
        var model = BuildModel();
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_15m_live",
            model: model,
            timeframe: "15m",
            emitOverride: null,
            inputOverride: "bar_1s_rows");
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/bars_15m_live.sql", sql);
    }

    [Fact]
    public void Bars60mLive_Equals_Golden()
    {
        var model = BuildModel();
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_60m_live",
            model: model,
            timeframe: "60m",
            emitOverride: null,
            inputOverride: "bar_1s_rows");
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/bars_60m_live.sql", sql);
    }
}


