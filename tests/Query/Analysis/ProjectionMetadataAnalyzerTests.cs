using System;
using System.Linq;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class ProjectionMetadataAnalyzerTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public double Ask { get; set; }
    }

    [Fact]
    public void Build_Aggregates_MapToAliasAsResolved()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new
            {
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid)
            })
            .Build();

        var meta = ProjectionMetadataAnalyzer.Build(model, isHubInput: true);
        var open = meta.Members.First(m => m.Alias == "Open");
        var high = meta.Members.First(m => m.Alias == "High");
        Assert.Equal(ProjectionMemberKind.Aggregate, open.Kind);
        Assert.Equal("OPEN", open.ResolvedColumnName);
        Assert.Equal("EarliestByOffset", open.AggregateFunctionName);
        Assert.Equal("BID", open.SourceMemberPath);
        Assert.Equal(ProjectionMemberKind.Aggregate, high.Kind);
        Assert.Equal("HIGH", high.ResolvedColumnName);
        Assert.Equal("Max", high.AggregateFunctionName);
        Assert.Equal("BID", high.SourceMemberPath);
    }

    [Fact]
    public void Build_ComputedExpression_FallbacksToAlias()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new
            {
                Range = g.Max(x => x.Bid) - g.Min(x => x.Bid)
            })
            .Build();

        var meta = ProjectionMetadataAnalyzer.Build(model, isHubInput: true);
        var range = meta.Members.First(m => m.Alias == "Range");
        Assert.Equal(ProjectionMemberKind.Computed, range.Kind);
        Assert.Equal("RANGE", range.ResolvedColumnName);
    }

    [Fact]
    public void Build_GroupKeyMembers_ClassifiedAsKey()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new
            {
                g.Key.Broker,
                g.Key.Symbol
            })
            .Build();

        var meta = ProjectionMetadataAnalyzer.Build(model, isHubInput: false);
        var broker = meta.Members.First(m => m.Alias == "Broker");
        var symbol = meta.Members.First(m => m.Alias == "Symbol");

        Assert.Equal(ProjectionMemberKind.Key, broker.Kind);
        Assert.Equal("BROKER", broker.ResolvedColumnName);
        Assert.Equal("BROKER", broker.SourceMemberPath);

        Assert.Equal(ProjectionMemberKind.Key, symbol.Kind);
        Assert.Equal("SYMBOL", symbol.ResolvedColumnName);
        Assert.Equal("SYMBOL", symbol.SourceMemberPath);
    }
}
