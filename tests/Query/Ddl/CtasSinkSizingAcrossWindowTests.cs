using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Planning;
using Ksql.Linq.Query.Hub.Analysis;
using System;
using System.Linq.Expressions;
using Xunit;

namespace Ksql.Linq.Tests.Query.Ddl;

public class CtasSinkSizingAcrossWindowTests
{
    private sealed class Tick
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public double Bid { get; set; }
        public DateTime Timestamp { get; set; }
    }

    [Theory]
    [InlineData("1s")]
    [InlineData("1m")]
    [InlineData("5m")]
    [InlineData("1h")]
    [InlineData("1d")]
    [InlineData("1wk")]
    [InlineData("1mo")]
    public void Ctas_WithClause_Includes_Partitions_Replicas_Retention(string timeframe)
    {
        var qm = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Tick) },
            SelectProjection = (Expression<Func<Tick, object>>)(o => new { Broker = o.Broker, Symbol = o.Symbol, Close = o.Bid }),
            GroupByExpression = (Expression<Func<Tick, object>>)(o => new { o.Broker, o.Symbol })
        };
        // Ensure non-aggregate lifting is possible (not strictly required for WITH assertion)
        var overrides = new System.Collections.Generic.Dictionary<string, HubProjectionOverride>(StringComparer.OrdinalIgnoreCase)
        {
            ["Close"] = HubProjectionOverride.ForAggregate("BID", "LatestByOffset")
        };
        qm.Extras["select/overrides"] = overrides;

        var sql = DdlPlanner.BuildWindowedCtas(
            name: $"bar_{timeframe}_live",
            model: qm,
            timeframe: timeframe,
            graceSeconds: 1,
            inputOverride: "bar_1s_rows",
            partitions: 1,
            replicas: 1,
            retentionMs: 604800000);

        Assert.Contains(" WITH (", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PARTITIONS=1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("REPLICAS=1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RETENTION_MS=604800000", sql, StringComparison.OrdinalIgnoreCase);
    }
}