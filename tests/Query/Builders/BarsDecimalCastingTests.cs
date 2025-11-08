using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Hub.Analysis;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Ksql.Linq.Query.Analysis;

namespace Ksql.Linq.Tests.Query.Builders;

public class BarsDecimalCastingTests
{
    [KsqlTopic("rate")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public int Lots { get; set; }
    }

    private class BarLiveDouble
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public int Lots { get; set; }
    }

    // Hints-only type for CAST rules
    private class BarLiveDecimalHint
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        [KsqlDecimal(18, 4)] public decimal Open { get; set; }
        [KsqlDecimal(18, 4)] public decimal High { get; set; }
        [KsqlDecimal(18, 4)] public decimal Low { get; set; }
        [KsqlDecimal(18, 4)] public decimal Close { get; set; }
        public int Lots { get; set; }
    }

    [Fact]
    public void Live_Select_Casts_Ohlc_To_Decimal_18_4()
    {
        // Global defaults intentionally different to verify attribute wins
        DecimalPrecisionConfig.Configure(18, 2, overrides: (System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, KsqlDslOptions.DecimalSetting>>?)null);

        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(o => o.Timestamp, new Windows { Minutes = new[] { 5 } })
            .GroupBy(o => new { o.Broker, o.Symbol })
            .Select(g => new BarLiveDouble
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid),
                Lots = g.Max(x => x.Lots)
            })
            .Build();

        var sql = BuildWithHubRewrites(
            name: "bar_5m_live",
            model: model,
            timeframe: "5m",
            emitOverride: null,
            inputOverride: "bar_1s_rows",
            options: new RenderOptions { ResultType = typeof(BarLiveDecimalHint) });

        // Keys must not be CAST
        Assert.DoesNotContain("CAST(o.Broker", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CAST(o.Symbol", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CAST(o.BucketStart", sql, StringComparison.OrdinalIgnoreCase);

        // OHLC should be cast to DECIMAL(18, 4)
        SqlAssert.ContainsNormalized(sql, "CAST(EARLIEST_BY_OFFSET(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 4)) AS Open");
        SqlAssert.ContainsNormalized(sql, "CAST(MAX(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 4)) AS High");
        SqlAssert.ContainsNormalized(sql, "CAST(MIN(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 4)) AS Low");
        SqlAssert.ContainsNormalized(sql, "CAST(LATEST_BY_OFFSET(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 4)) AS Close");

        // Non-decimal columns should not be cast
        SqlAssert.ContainsNormalized(sql, "MAX(lots) AS Lots");

        // Window clause order and EMIT
        SqlAssert.AssertOrderNormalized(sql,
            "FROM bar_1s_rows",
            "WINDOW TUMBLING (SIZE 5 MINUTES)",
            "GROUP BY BROKER, SYMBOL");
        SqlAssert.EndsWithSemicolon(sql);
    }

    private class BarLiveDouble2
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    private class BarLiveDecimalHint2
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] public DateTime BucketStart { get; set; }
        [KsqlDecimal(18, 2)] public decimal Open { get; set; }
        [KsqlDecimal(18, 2)] public decimal High { get; set; }
        [KsqlDecimal(18, 2)] public decimal Low { get; set; }
        [KsqlDecimal(18, 2)] public decimal Close { get; set; }
    }

    [Fact]
    public void Live_Select_Respects_PerProperty_Decimal_18_2()
    {
        DecimalPrecisionConfig.Configure(18, 4, overrides: (System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, KsqlDslOptions.DecimalSetting>>?)null);

        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(o => o.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(o => new { o.Broker, o.Symbol })
            .Select(g => new BarLiveDouble2
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

        var sql = BuildWithHubRewrites(
            name: "bar_1m_live",
            model: model,
            timeframe: "1m",
            emitOverride: null,
            inputOverride: "bar_1s_rows",
            options: new RenderOptions { ResultType = typeof(BarLiveDecimalHint2) });

        SqlAssert.ContainsNormalized(sql, "CAST(EARLIEST_BY_OFFSET(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 2)) AS Open");
        SqlAssert.ContainsNormalized(sql, "CAST(MAX(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 2)) AS High");
        SqlAssert.ContainsNormalized(sql, "CAST(MIN(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 2)) AS Low");
        SqlAssert.ContainsNormalized(sql, "CAST(LATEST_BY_OFFSET(" );
        SqlAssert.ContainsNormalized(sql, "AS DECIMAL(18, 2)) AS Close");
    }

    private static string BuildWithHubRewrites(string name, KsqlQueryModel model, string timeframe, string? emitOverride, string? inputOverride, RenderOptions? options)
    {
        var clone = model.Clone();
        if (!string.IsNullOrWhiteSpace(inputOverride) && inputOverride.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase))
        {
            if (clone.SelectProjection != null)
                clone.SelectProjection = HubRowsProjectionAdapter.Adapt(clone.SelectProjection);
            var meta = ProjectionMetadataAnalyzer.Build(clone, isHubInput: true);
            clone.SelectProjectionMetadata = meta;
            HubSelectPolicy.BuildOverridesAndExcludes(meta,
                out var selectOverrides,
                out var selectExclude);
            clone.Extras["select/overrides"] = selectOverrides;
            clone.Extras["select/exclude"] = selectExclude;
        }
        var sql = KsqlCreateWindowedStatementBuilder.Build(name, clone, timeframe, emitOverride, inputOverride, options);
        return sql;
    }
}


