using System;
using System.Linq;
using System.Collections.Generic;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Hub.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Integration;

/// NOTE:
/// This end-to-end test intentionally validates contracts, not formatting.
/// - It checks metadata resolution (C# hub-side adaptation -> ProjectionMetadata)
///   and the resulting SQL structure from builders in one place so the relationship is clear.
/// - Assertions use structural parsing and alias-based lookups (QueryStructure) to resist
///   harmless refactors (spacing, minor reordering).
/// - If you change builder conventions (aliases, aggregate rewrite policy), update this
///   test alongside the adapter/analyzer to keep the metadata竊粘QL contract aligned.
public class HubEndToEndTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public int Lots { get; set; }
    }

    private class BarAll
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
        // Date parts for hub flows are computed on the C# side by consumers, not in KSQL CTAS.
    }

    [Fact]
    public void Hub_EndToEnd_MetadataAndSql_Contracts()
    {
        // 1) Define a hub-style projection in C# (LINQ). This mirrors app usage.
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new BarAll
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
                Cnt = g.Count()
            })
            .Build();

        // 2) Apply hub-side C# adapter (same as pipeline does) before metadata analysis.
        var adapted = model.Clone();
        if (adapted.SelectProjection != null)
            adapted.SelectProjection = HubRowsProjectionAdapter.Adapt(adapted.SelectProjection);

        // Validate C# LINQ adaptation: ensure key members are produced by expected methods
        Assert.NotNull(adapted.SelectProjection);
        var init = adapted.SelectProjection!.Body as System.Linq.Expressions.MemberInitExpression;
        Assert.NotNull(init);
        AssertBindingIsMethod(init!, nameof(BarAll.BucketStart), "WindowStart");
        AssertBindingIsMethod(init!, nameof(BarAll.Open), "EarliestByOffset");
        AssertBindingIsMethod(init!, nameof(BarAll.High), "Max");
        AssertBindingIsMethod(init!, nameof(BarAll.Low), "Min");
        AssertBindingIsMethod(init!, nameof(BarAll.Close), "LatestByOffset");

        // 3) Build metadata (hub input)
        var meta = ProjectionMetadataAnalyzer.Build(adapted, isHubInput: true);
        adapted.SelectProjectionMetadata = meta;
        HubSelectPolicy.BuildOverridesAndExcludes(meta,
            out var overrides,
            out var exclude,
            availableColumns: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "OPEN", "HIGH", "LOW", "CLOSE" });
        adapted.Extras["hub/availableColumns"] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "OPEN", "HIGH", "LOW", "CLOSE" };
        adapted.Extras["select/overrides"] = overrides;
        adapted.Extras["select/exclude"] = exclude;
        adapted.Extras["projectionMetadata"] = meta;

        // 4) Build SQL for hub input; builders consume metadata to target hub columns
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: adapted,
            timeframe: "1m",
            inputOverride: "bar_1s_rows");

        // 5) Metadata assertions (C# side) 窶・Resolved columns and aggregate kind
        Assert.True(meta.IsHubInput);
        AssertAggregateResolved(meta, "Open", "OPEN");
        AssertAggregateResolved(meta, "High", "HIGH");
        AssertAggregateResolved(meta, "Low", "LOW");
        AssertAggregateResolved(meta, "Close", "CLOSE");

        // 6) SQL structure assertions (builder side)
        var qs = QueryStructure.Parse(sql);
        Assert.Equal("TABLE", qs.CreateType);
        Assert.Equal("bar_1m_live", qs.TargetName);
        Assert.Equal(new[] { "BROKER", "SYMBOL" }, qs.GroupByColumns);
        Assert.Contains("WINDOW TUMBLING", qs.WindowRaw);

        // OHLC + common aggregates present and pointing to hub columns by alias
        Assert.True(qs.TryGetProjection("Open", out var openExpr));
        Assert.Contains("EARLIEST_BY_OFFSET(", openExpr, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OPEN", openExpr, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("High", out var highExpr));
        Assert.Contains("MAX(", highExpr, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("HIGH", highExpr, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Low", out var lowExpr));
        Assert.Contains("MIN(", lowExpr, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LOW", lowExpr, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Close", out var closeExpr));
        Assert.Contains("LATEST_BY_OFFSET(", closeExpr, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CLOSE", closeExpr, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("AvgBid", out var avgExpr));
        var avgExprUpper = avgExpr.ToUpperInvariant();
        Assert.True(
            avgExprUpper.Contains("AVG(") ||
            avgExprUpper.Contains("SUM(AVGBID"),
            $"AvgBid projection should aggregate hub values, actual: {avgExpr}");

        Assert.True(qs.TryGetProjection("SumLots", out var sumExpr));
        Assert.Contains("SUM(", sumExpr, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Cnt", out var cntExpr));
        Assert.Contains("COUNT(", cntExpr, StringComparison.OrdinalIgnoreCase);

    }


    private static void AssertAggregateResolved(ProjectionMetadata meta, string alias, string expectedColumn)
    {
        var m = meta.Members.First(x => string.Equals(x.Alias, alias, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(ProjectionMemberKind.Aggregate, m.Kind);
        Assert.Equal(expectedColumn, m.ResolvedColumnName);
    }

    private static void AssertBindingIsMethod(System.Linq.Expressions.MemberInitExpression init, string memberName, string expectedMethod)
    {
        var binding = init.Bindings
            .OfType<System.Linq.Expressions.MemberAssignment>()
            .First(b => string.Equals(b.Member.Name, memberName, StringComparison.OrdinalIgnoreCase));
        var expr = Ksql.Linq.Query.Builders.Common.ExpressionUtils.UnwrapConvert(binding.Expression);
        var call = expr as System.Linq.Expressions.MethodCallExpression;
        Assert.NotNull(call);
        var name = call!.Method.IsGenericMethod
            ? call.Method.GetGenericMethodDefinition().Name
            : call.Method.Name;
        Assert.Equal(expectedMethod, name);
    }
}


