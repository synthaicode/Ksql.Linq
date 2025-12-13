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

/// POLICY TEST
/// Contract: In hub flows, aggregate functions are executed in KSQL; other (non-aggregate) computations
/// are handled on the C# side by consumers. This test verifies both classification (metadata) and that
/// only aggregates are mapped to hub columns in SQL (no hub override applied to computed members).
public class HubAggregateVsComputedPolicyTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public int Lots { get; set; }
    }

    private class OutDto
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public double AvgBid { get; set; }
        public int SumLots { get; set; }
        public int Cnt { get; set; }
        public int Yr { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    private static int Year(DateTime dt) => dt.Year;

    [Fact]
    public void AggregatesInKsql_ComputedInCSharp_Policy_Is_Respected()
    {
        // Build a projection containing both aggregate outputs and computed (non-aggregate) outputs
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new OutDto
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid),
                AvgBid = g.Average(x => x.Bid),
                SumLots = g.Sum(x => x.Lots),
                Cnt = g.Count(),
                Yr = Year(g.WindowStart()),            // computed (policy: C#側)
                Label = g.Key.Broker + "-" + g.Key.Symbol // computed (policy: C#側)
            })
            .Build();

        // Hub-side adaptation
        var adapted = model.Clone();
        if (adapted.SelectProjection != null)
            adapted.SelectProjection = HubRowsProjectionAdapter.Adapt(adapted.SelectProjection);

        // Metadata classification
        var meta = ProjectionMetadataAnalyzer.Build(adapted, isHubInput: true);
        adapted.SelectProjectionMetadata = meta;

        // Aggregates must be marked Aggregate; non-aggregates Computed
        AssertMemberKind(meta, nameof(OutDto.Open), ProjectionMemberKind.Aggregate, "OPEN");
        AssertMemberKind(meta, nameof(OutDto.High), ProjectionMemberKind.Aggregate, "HIGH");
        AssertMemberKind(meta, nameof(OutDto.Low), ProjectionMemberKind.Aggregate, "LOW");
        AssertMemberKind(meta, nameof(OutDto.Close), ProjectionMemberKind.Aggregate, "CLOSE");

        var yr = meta.Members.First(m => m.Alias == nameof(OutDto.Yr));
        var label = meta.Members.First(m => m.Alias == nameof(OutDto.Label));
        Assert.Equal(ProjectionMemberKind.Computed, yr.Kind);
        Assert.Equal(ProjectionMemberKind.Computed, label.Kind);

        // Build SQL for hub input
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: adapted,
            timeframe: "1m",
            inputOverride: "bar_1s_rows");

        var qs = QueryStructure.Parse(sql);

        // Aggregates in SQL (KSQL側)
        Assert.True(qs.TryGetProjection("Open", out var openExpr) && openExpr.IndexOf("OPEN", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.True(qs.TryGetProjection("High", out var highExpr) && highExpr.IndexOf("HIGH", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.True(qs.TryGetProjection("Low", out var lowExpr) && lowExpr.IndexOf("LOW", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.True(qs.TryGetProjection("Close", out var closeExpr) && closeExpr.IndexOf("CLOSE", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.True(qs.TryGetProjection("AvgBid", out var avgExpr) &&
                    (avgExpr.IndexOf("AVG(", StringComparison.OrdinalIgnoreCase) >= 0 ||
                     (avgExpr.IndexOf("SUM(", StringComparison.OrdinalIgnoreCase) >= 0 &&
                      avgExpr.IndexOf("CNT", StringComparison.OrdinalIgnoreCase) >= 0)));
        Assert.True(qs.TryGetProjection("SumLots", out var sumExpr) && sumExpr.IndexOf("SUM(", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.True(qs.TryGetProjection("Cnt", out var cntExpr) && cntExpr.IndexOf("COUNT(", StringComparison.OrdinalIgnoreCase) >= 0);

        // Non-aggregates must NOT be hub-override targets（C#側で処理する契約）。
        // 少なくとも hub 列名（OPEN/HIGH/LOW/CLOSE）を含まないことを確認し、
        // 集計引数上書きが誤適用されていないことを担保する。
        // Computed members are excluded from CTAS in hub flows
        Assert.False(qs.TryGetProjection("Yr", out _));

        Assert.False(qs.TryGetProjection("Label", out _));
    }

    private static void AssertMemberKind(ProjectionMetadata meta, string alias, ProjectionMemberKind kind, string resolved)
    {
        var m = meta.Members.First(x => string.Equals(x.Alias, alias, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(kind, m.Kind);
        Assert.Equal(resolved, m.ResolvedColumnName);
    }
}

