using System;
using System.Linq;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class HubAdapterMetadataTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public int Lots { get; set; }
    }

    private class Bar
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    [Fact]
    public void Adapter_Rewrites_And_Metadata_Resolves_OHLC_To_HubColumns()
    {
        // Build a hub-style projection including WindowStart and OHLC
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new Bar
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid)
            })
            .Build();

        // Adapt the projection for hub inputs via C#蛛ｴ繧｢繝繝励ち
        var adapted = model.Clone();
        if (adapted.SelectProjection != null)
        {
            adapted.SelectProjection = HubRowsProjectionAdapter.Adapt(adapted.SelectProjection);
        }

        // Analyze metadata for hub input
        var meta = ProjectionMetadataAnalyzer.Build(adapted, isHubInput: true);

        Assert.True(meta.IsHubInput);

        // OHLC members must be marked Aggregate and resolve to hub columns
        AssertAggregateResolved(meta, "Open", "OPEN");
        AssertAggregateResolved(meta, "High", "HIGH");
        AssertAggregateResolved(meta, "Low", "LOW");
        AssertAggregateResolved(meta, "Close", "CLOSE");
    }

    private static void AssertAggregateResolved(ProjectionMetadata meta, string alias, string expectedColumn)
    {
        var m = meta.Members.First(x => string.Equals(x.Alias, alias, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(ProjectionMemberKind.Aggregate, m.Kind);
        Assert.Equal(expectedColumn, m.ResolvedColumnName);
    }
}

