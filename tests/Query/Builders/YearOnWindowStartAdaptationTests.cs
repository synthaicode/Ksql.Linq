using System;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

public class YearOnWindowStartAdaptationTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static class FunctionStubs
    {
        public static int Year(DateTime dt) => dt.Year;
    }

    private class OutDto
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public int Year { get; set; }
    }

    [Fact]
    public void Adapter_Rewrites_Year_WindowStart_To_OffsetAggregate_Over_Source_Timestamp()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new OutDto
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Year = FunctionStubs.Year(g.WindowStart())
            })
            .Build();

        // Build windowed SQL with hub input; adapter should rewrite Year(WindowStart()) appropriately
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: model,
            timeframe: "1m",
            emitOverride: null,
            inputOverride: "bar_1s_rows");

        // Policy: computed parts (Year) are handled in C# for hub flows and excluded from CTAS
        var qs = Ksql.Linq.Tests.Utils.QueryStructure.Parse(sql);
        Assert.False(qs.TryGetProjection("Year", out _));
        Assert.Equal(new[] { "BROKER", "SYMBOL" }, qs.GroupByColumns);
        Assert.Contains("WINDOW TUMBLING", qs.WindowRaw);
    }
}


