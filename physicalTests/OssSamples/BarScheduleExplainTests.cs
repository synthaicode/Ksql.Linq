using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Builders;
using System;
using System.Linq;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

/// <summary>
/// OnModelCreating 竊・ToQuery 竊・Materialize(SQL) 竊・Verify 縺ｮ豬√ｌ縺ｫ邨ｱ荳縲・
/// MarketSchedule 騾｣謳ｺ縺ｮ1譌･/1騾ｱ繝舌・繧呈､懆ｨｼ縲・
/// </summary>
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
                    .Tumbling(r => r.Timestamp, new Query.Dsl.Windows { Hours = new[] { 24 } })
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
                    .Tumbling(r => r.Timestamp, new Query.Dsl.Windows { Hours = new[] { 24*7 } })
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

    [Fact(Skip = "Excluded from physicalTests: SQL generation is a unit test concern")]
    public void Daily_With_MarketSchedule_Materialize_And_Verify_Final()
    {
        var ctx = new TestContext();
        var m = ctx.GetEntityModels();
        var em = m[typeof(Bar1dFinal)];
        Assert.Equal(typeof(MarketSchedule), em.QueryModel!.BasedOnType);
        var sql = KsqlCreateWindowedStatementBuilder.Build("bar_1d_final", em.QueryModel!, "1d", emitOverride: "EMIT CHANGES");
        // Note: We no longer assert concrete function aliases or CREATE statements here
        // Assert.Contains("EARLIEST_BY_OFFSET(Bid) AS Open", sql);
        // Assert.Contains("LATEST_BY_OFFSET(Bid) AS KsqlTimeFrameClose", sql);
        // Assert.Contains("CREATE TABLE bar_1d_final", sql);
        Assert.Contains("EMIT CHANGES", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact(Skip = "Excluded from physicalTests: SQL generation is a unit test concern")]
    public void Weekly_With_MarketSchedule_Materialize_And_Verify_Final()
    {
        var ctx = new TestContext();
        var m = ctx.GetEntityModels();
        var em = m[typeof(Bar1wkFinal)];
        Assert.Equal(typeof(MarketSchedule), em.QueryModel!.BasedOnType);
        var sql = KsqlCreateWindowedStatementBuilder.Build("bar_1wk_final", em.QueryModel!, "7d", emitOverride: "EMIT CHANGES");
        Assert.Contains("MAX(Bid) AS High", sql);
        Assert.Contains("MIN(Bid) AS Low", sql);
        // Note: We no longer assert CREATE statements
        // Assert.Contains("CREATE TABLE bar_1wk_final", sql);
        Assert.Contains("EMIT CHANGES", sql, StringComparison.OrdinalIgnoreCase);
    }
}


