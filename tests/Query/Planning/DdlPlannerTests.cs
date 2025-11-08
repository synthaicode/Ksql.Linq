using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Planning;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Ksql.Linq.Tests.Query.Planning;

public class DdlPlannerTests
{
    private sealed class Tick
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public double Bid { get; set; }
        public DateTime Timestamp { get; set; }
    }

    [Fact]
    public void BuildWindowedCtas_InjectsGrace_WhenRequested()
    {
        var qm = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Tick) },
            // SELECT Broker, Symbol, Latest Bid as Close
            SelectProjection = (Expression<Func<Tick, object>>)(o => new { Broker = o.Broker, Symbol = o.Symbol, Close = o.Bid }),
            GroupByExpression = (Expression<Func<Tick, object>>)(o => new { o.Broker, o.Symbol })
        };
        qm.Windows.Add("1m");

        var sql = DdlPlanner.BuildWindowedCtas(
            name: "bar_1m_live",
            model: qm,
            timeframe: "1m",
            graceSeconds: 1,
            inputOverride: "bar_1s_rows",
            partitions: 1,
            replicas: 1,
            retentionMs: 604800000);

        Assert.Contains("WINDOW TUMBLING (SIZE 1 MINUTES", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GRACE PERIOD 1 SECONDS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(" AS Close", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WITH (", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PARTITIONS=1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("REPLICAS=1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.True(sql.IndexOf("RETENTION_MS=604800000", StringComparison.OrdinalIgnoreCase) >= 0, $"SQL:{sql}");
    }

    [Fact]
    public void BuildWindowedCtas_EmitsWindowStartAlias()
    {
        var model = new KsqlQueryRoot()
            .From<Tick>()
            .Tumbling(t => t.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(t => new { t.Broker, t.Symbol })
            .Select(g => new
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid)
            })
            .Build();

        var sql = DdlPlanner.BuildWindowedCtas(
            name: "bar_1m_live",
            model,
            timeframe: "1m",
            graceSeconds: 2,
            inputOverride: "bar_1s_rows",
            partitions: 1,
            replicas: 1,
            retentionMs: 604800000);

        Assert.Contains("WINDOWSTART AS BucketStart", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildWindowedCtas_DoesNotDuplicateGrace_WhenPresent()
    {
        var qm = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Tick) },
            SelectProjection = (Expression<Func<Tick, object>>)(o => new { Broker = o.Broker, Symbol = o.Symbol, Close = o.Bid }),
            GroupByExpression = (Expression<Func<Tick, object>>)(o => new { o.Broker, o.Symbol })
        };
        // Request grace twice (via model and parameter) -> only one injection expected
        qm.GraceSeconds = 2;
        var sql = DdlPlanner.BuildWindowedCtas("bar_1m_live", qm, "1m", graceSeconds: 2, inputOverride: "bar_1s_rows");
        var idx = sql.IndexOf("GRACE PERIOD", StringComparison.OrdinalIgnoreCase);
        Assert.True(idx >= 0, "Expected GRACE PERIOD to be present");
        var next = sql.IndexOf("GRACE PERIOD", idx + 1, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(-1, next); // no duplicate
    }

    [Fact]
    public void BuildRowsLastCtas_OmitsRetention_ForNonWindowTable()
    {
        var sql = DdlPlanner.BuildRowsLastCtas(
            targetName: "bar_rows_last",
            sourceRowsName: "bar_1s_rows",
            keyColumns: new[] { "broker", "symbol" },
            valueColumns: new[] { "open", "high" },
            partitions: 1,
            replicas: 1,
            retentionMs: 604800000);

        Assert.DoesNotContain("RETENTION_MS", sql, StringComparison.OrdinalIgnoreCase);
    }
}
