using System;
using System.Linq;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Hub.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Integration;

public class HubComputedDateExclusionTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class OutDto
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public int Wk { get; set; }
        public int Dow { get; set; }
    }

    private static int WeekOfYear(DateTime dt) => int.Parse(dt.ToString("w"));
    private static int DayOfWeek(DateTime dt) => (int)dt.DayOfWeek;

    [Fact]
    public void HubComputedDate_MarkedComputed_And_Excluded_From_CTAS()
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
                Wk = WeekOfYear(g.WindowStart()),
                Dow = DayOfWeek(g.WindowStart())
            })
            .Build();

        // Apply adapter and analyze metadata
        var adapted = model.Clone();
        if (adapted.SelectProjection != null)
            adapted.SelectProjection = HubRowsProjectionAdapter.Adapt(adapted.SelectProjection);
        var meta = ProjectionMetadataAnalyzer.Build(adapted, isHubInput: true);
        adapted.SelectProjectionMetadata = meta;

        // Computed classification
        var wk = meta.Members.First(m => m.Alias == nameof(OutDto.Wk));
        var dow = meta.Members.First(m => m.Alias == nameof(OutDto.Dow));
        Assert.Equal(ProjectionMemberKind.Computed, wk.Kind);
        Assert.Equal(ProjectionMemberKind.Computed, dow.Kind);

        // Build hub CTAS and ensure computed aliases are excluded
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: adapted,
            timeframe: "1m",
            inputOverride: "bar_1s_rows");
        var qs = QueryStructure.Parse(sql);
        Assert.False(qs.TryGetProjection("Wk", out _));
        Assert.False(qs.TryGetProjection("Dow", out _));
    }
}



