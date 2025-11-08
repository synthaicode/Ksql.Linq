using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using System.Linq;
using System;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query;

[Trait("Level", TestLevel.L3)]
public class BarScheduleExplainTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    private class Bar1dFinal
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)]
        [KsqlTimestamp]
        public DateTime Timestamp { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private class Bar1wkFinal
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)]
        [KsqlTimestamp]
        public DateTime Timestamp { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        public TestContext() : base(new KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Rate>(readOnly: true);

            modelBuilder.Entity<Bar1dFinal>()
                .ToQuery(q => q.From<Rate>()
                    .TimeFrame<MarketSchedule>((r, s) =>
                         r.Broker == s.Broker
                      && r.Symbol == s.Symbol
                      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                      dayKey: s => s.MarketDate)
                    .Tumbling(r => r.Timestamp, new Windows { Hours = new[] { 24 } })
                    .GroupBy(r => new { r.Broker, r.Symbol, r.Timestamp })
                    .Select(g => new Bar1dFinal
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        Timestamp = g.Key.Timestamp,
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));

            modelBuilder.Entity<Bar1wkFinal>()
                .ToQuery(q => q.From<Rate>()
                    .TimeFrame<MarketSchedule>((r, s) =>
                         r.Broker == s.Broker
                      && r.Symbol == s.Symbol
                      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                      dayKey: s => s.MarketDate)
                    .Tumbling(r => r.Timestamp, new Windows { Hours = new[] { 24 * 7 } })
                    .GroupBy(r => new { r.Broker, r.Symbol, r.Timestamp })
                    .Select(g => new Bar1wkFinal
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        Timestamp = g.Key.Timestamp,
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));
        }
    }

    [Fact]
    public void Daily_With_MarketSchedule_Generates_SQL_With_Expected_Structure()
    {
        var ctx = new TestContext();
        var models = ctx.GetEntityModels();
        var em = models[typeof(Bar1dFinal)];
        Assert.Equal(typeof(MarketSchedule), em.QueryModel!.BasedOnType);
        var sql = KsqlCreateWindowedStatementBuilder.Build("bar_1d_live", em.QueryModel!, "1d", emitOverride: "EMIT CHANGES");

        Assert.StartsWith("CREATE TABLE IF NOT EXISTS bar_1d_live", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WINDOW TUMBLING (SIZE 1 DAYS)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EARLIEST_BY_OFFSET(Bid)", sql);
        Assert.Contains("LATEST_BY_OFFSET(Bid)", sql);
        Assert.Contains("MAX(Bid)", sql);
        Assert.Contains("MIN(Bid)", sql);
    }

    [Fact]
    public void Weekly_With_MarketSchedule_Generates_SQL_With_Expected_Structure()
    {
        var ctx = new TestContext();
        var models = ctx.GetEntityModels();
        var em = models[typeof(Bar1wkFinal)];
        Assert.Equal(typeof(MarketSchedule), em.QueryModel!.BasedOnType);
        var sql = KsqlCreateWindowedStatementBuilder.Build("bar_1wk_live", em.QueryModel!, "7d", emitOverride: "EMIT CHANGES");

        Assert.StartsWith("CREATE TABLE IF NOT EXISTS bar_1wk_live", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WINDOW TUMBLING (SIZE 7 DAYS)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EARLIEST_BY_OFFSET(Bid)", sql);
        Assert.Contains("LATEST_BY_OFFSET(Bid)", sql);
        Assert.Contains("MAX(Bid)", sql);
        Assert.Contains("MIN(Bid)", sql);
    }
}


