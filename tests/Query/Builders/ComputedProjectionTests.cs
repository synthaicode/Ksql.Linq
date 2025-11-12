using System;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;
using System.Linq;

namespace Ksql.Linq.Tests.Query.Builders;

public class ComputedProjectionTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class BarWithRange
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Range { get; set; }
    }

    [Fact]
    public void Builder_Emits_Computed_Arithmetic_From_CSharp_Projection()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new BarWithRange
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Range = g.Max(x => x.Bid) - g.Min(x => x.Bid)
            })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: model,
            timeframe: "1m");

        var qs = Ksql.Linq.Tests.Utils.QueryStructure.Parse(sql);
        Assert.Equal("TABLE", qs.CreateType);
        Assert.Equal("bar_1m_live", qs.TargetName);
        Assert.True(qs.HasEmitChanges);
        // GROUP BY structure
        Assert.Equal(new[] { "BROKER", "SYMBOL" }, qs.GroupByColumns);
        // Window clause structure
        Assert.Contains("WINDOW TUMBLING", qs.WindowRaw);
        Assert.Contains("1 MINUTES", qs.WindowRaw);
        // Projection structure: alias must exist and be computed
        Assert.True(qs.TryGetProjection("Range", out var rangeExpr));
        Assert.Contains("MAX(", rangeExpr, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("MIN(", rangeExpr, StringComparison.OrdinalIgnoreCase);
    }
}


