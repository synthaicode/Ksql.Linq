using System;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;
using System.Linq;

namespace Ksql.Linq.Tests.Query.Builders;

public class HubAggregateOverrideBuilderTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
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
    public void Builder_Uses_Metadata_To_Override_Aggregate_Args_For_Hub()
    {
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

        // Attach projection metadata indicating hub input
        var meta = ProjectionMetadataAnalyzer.Build(model, isHubInput: true);
        model.SelectProjectionMetadata = meta;
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: model,
            timeframe: "1m",
            emitOverride: null,
            inputOverride: "bar_1s_rows",
            options: null);

        // Expect aggregates to target hub-projected columns without requiring source alias qualifiers
        SqlAssert.ContainsNormalized(sql, "EARLIEST_BY_OFFSET(OPEN) AS Open");
        SqlAssert.ContainsNormalized(sql, "MAX(HIGH) AS High");
        SqlAssert.ContainsNormalized(sql, "MIN(LOW) AS Low");
        SqlAssert.ContainsNormalized(sql, "LATEST_BY_OFFSET(CLOSE) AS Close");
        SqlAssert.ContainsNormalized(sql, "GROUP BY BROKER, SYMBOL");
        SqlAssert.ContainsNormalized(sql, "WINDOW TUMBLING (SIZE 1 MINUTES)");
        SqlAssert.EndsWithSemicolon(sql);
    }
}


