using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Abstractions;
using System;
using System.Linq.Expressions;
using Ksql.Linq;
using Ksql.Linq.Tests.Utils;
using Xunit;
namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", TestLevel.L3)]
public class KsqlCreateWindowedStatementBuilderTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTable]
    private class RateTable
    {
        [KsqlKey(0)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("deduprates")]
    private class DedupRate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Ts { get; set; }
        public double Bid { get; set; }
    }

    [Fact]
    public void Build_Includes_Window_Tumbling_1m()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build("bar_1m_live", model, "1m");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW TUMBLING (SIZE 1 MINUTES)");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "CREATE TABLE IF NOT EXISTS bar_1m_live");
    }

    [Fact]
    public void Build_Live_Table_Uses_EmitChanges()
    {
        var model = new KsqlQueryRoot()
            .From<DedupRate>()
            .Tumbling(r => r.Ts, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            "bar_1m_live",
            model,
            "1m",
            "EMIT CHANGES",
            "deduprates_1s_rows");

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE TABLE IF NOT EXISTS bar_1m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "FROM deduprates_1s_rows");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "EMIT CHANGES");
    }

    [Fact]
    public void Build_From_With_Alias_Inserts_Window_After_Alias()
    {
        var model = new KsqlQueryRoot()
            .From<DedupRate>()
            .Tumbling(r => r.Ts, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            "bar_1m_live",
            model,
            "1m",
            null,
            "deduprates_1s_rows");
        Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            sql,
            "FROM deduprates_1s_rows",
            "window tumbling"
        );
    }

    [Fact]
    public void BuildAll_Generates_Per_Window()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1, 5 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var map = KsqlCreateWindowedStatementBuilder.BuildAll(
            "bar",
            model,
            tf => $"bar_{tf}_live");

        Assert.True(map.ContainsKey("1m"));
        Assert.True(map.ContainsKey("5m"));
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(map["1m"], "bar_1m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(map["5m"], "bar_5m_live");
    }

    [Fact]
    public void Build_NoWindow_Creates_Stream()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Select(r => r)
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("rates", model);
        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE STREAM IF NOT EXISTS rates");
    }

    [Fact]
    public void Build_WithWindow_Creates_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart(), Open = g.EarliestByOffset(x => x.Bid) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build("bar_1m", model, "1m");
        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE TABLE IF NOT EXISTS bar_1m");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW TUMBLING");
    }

    [Fact]
    public void DetermineType_Tumbling_Returns_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, BucketStart = g.WindowStart() })
            .Build();
        Assert.Equal(StreamTableType.Table, model.DetermineType());
    }

    [Fact]
    public void DetermineType_NoAggregation_Returns_Stream()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Select(r => r)
            .Build();
        Assert.Equal(StreamTableType.Stream, model.DetermineType());
    }

    [Fact]
    public void BuildHopping_InsertsWindowBeforeJoinAndKeepsAlias()
    {
        Expression<Func<Rate, DedupRate, bool>> join = (o, i) => o.Symbol == i.Symbol;
        Expression<Func<Rate, DedupRate, object>> groupBy = (o, i) => new { o.Broker, o.Symbol };
        Expression<Func<Rate, DedupRate, object>> select = (o, i) => new
        {
            o.Broker,
            o.Symbol,
            Start = WindowExtensions.WindowStart<object, object>(null!)
        };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Rate), typeof(DedupRate) },
            JoinCondition = join,
            GroupByExpression = groupBy,
            SelectProjection = select,
            TimeKey = nameof(Rate.Timestamp),
            Hopping = new HoppingWindowSpec
            {
                Size = TimeSpan.FromMinutes(5),
                Advance = TimeSpan.FromMinutes(1)
            },
            PrimarySourceRequiresAlias = true
        };

        var sql = KsqlCreateWindowedStatementBuilder.BuildHopping("rate_5m_live", model);

        SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING ( SIZE 5 MINUTES , ADVANCE BY 1 MINUTES )");
        Assert.True(sql.IndexOf("WINDOW HOPPING", StringComparison.OrdinalIgnoreCase) <
                    sql.IndexOf("JOIN", StringComparison.OrdinalIgnoreCase));
        SqlAssert.ContainsNormalized(sql, "FROM RATE o WINDOW HOPPING");
        SqlAssert.ContainsNormalized(sql, "JOIN DEDUPRATE i");
    }

    [Fact]
    public void BuildHopping_ThrowsWithoutGroupBy()
    {
        Expression<Func<Rate, object>> select = r => new { Start = r.Timestamp };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Rate) },
            SelectProjection = select,
            TimeKey = nameof(Rate.Timestamp),
            Hopping = new HoppingWindowSpec
            {
                Size = TimeSpan.FromMinutes(5),
                Advance = TimeSpan.FromMinutes(1)
            }
        };

        Assert.Throws<InvalidOperationException>(() => KsqlCreateWindowedStatementBuilder.BuildHopping("h_no_group", model));
    }

    [Fact]
    public void BuildHopping_ThrowsWithoutWindowStart()
    {
        Expression<Func<Rate, object>> select = r => new { r.Broker, r.Symbol };
        Expression<Func<Rate, object>> group = r => new { r.Broker };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Rate) },
            GroupByExpression = group,
            SelectProjection = select,
            TimeKey = nameof(Rate.Timestamp),
            Hopping = new HoppingWindowSpec
            {
                Size = TimeSpan.FromMinutes(5),
                Advance = TimeSpan.FromMinutes(1)
            }
        };

        var sql = KsqlCreateWindowedStatementBuilder.BuildHopping("h_no_ws", model);
        SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING");
        SqlAssert.DoesNotContainNormalized(sql, "WINDOWSTART AS");
    }
}





