using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using System;
using System.Linq;
using Xunit;
using Ksql.Linq.Tests.Utils;

namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", TestLevel.L3)]
public class KsqlCreateHoppingStatementBuilderTests
{
    private class Trade
    {
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Price { get; set; }
    }

    // ========================================
    // Tumbling焼き直しテスト #1
    // 元: Build_Includes_Window_Tumbling_1m()
    // ========================================
    [Fact]
    public void Build_Includes_Window_Hopping_5m_1m()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(
                time: t => t.Timestamp,
                windowSize: TimeSpan.FromMinutes(5),
                hopInterval: TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new
            {
                g.Key,
                WindowStart = g.WindowStart(),
                AvgPrice = g.Average(x => x.Price)
            })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_avg_5m_hop1m",
            model: model,
            timeframe: "5m",
            hopInterval: TimeSpan.FromMinutes(1));

        // 検証: HOPPING構文が含まれる
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "SIZE 5 MINUTES");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "ADVANCE BY 1 MINUTES");

        // 検証: TABLE生成（aggregateなのでTABLE）
        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE TABLE IF NOT EXISTS trade_avg_5m_hop1m");
    }

    // ========================================
    // Tumbling焼き直しテスト #2
    // 元: Build_Live_Table_Uses_EmitChanges()
    // ========================================
    [Fact]
    public void Build_Hopping_Live_Table_Uses_EmitChanges()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(t => t.Timestamp, TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(2))
            .GroupBy(t => t.Symbol)
            .Select(g => new { g.Key, Avg = g.Average(x => x.Price) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_10m_hop2m_live",
            model: model,
            timeframe: "10m",
            emitOverride: "EMIT CHANGES",
            inputOverride: null,
            hopInterval: TimeSpan.FromMinutes(2));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "EMIT CHANGES");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING");
    }

    // ========================================
    // Tumbling焼き直しテスト #3
    // 元: DetermineType_Tumbling_Returns_Table()
    // ========================================
    [Fact]
    public void DetermineType_Hopping_Returns_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(t => t.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new { g.Key, Avg = g.Average(x => x.Price) })
            .Build();

        // Hoppingもaggregateなので、TABLEを返すはず
        Assert.Equal(StreamTableType.Table, model.DetermineType());
    }

    // ========================================
    // 新規テスト: Hop > Windowの検証
    // ========================================
    [Fact]
    public void Hopping_HopGreaterThanWindow_ThrowsException()
    {
        var query = new KsqlQueryRoot().From<Trade>();

        Assert.Throws<ArgumentException>(() =>
            query.Hopping(
                time: t => t.Timestamp,
                windowSize: TimeSpan.FromMinutes(5),
                hopInterval: TimeSpan.FromMinutes(10)));  // ← Hop > Window
    }

    // ========================================
    // 新規テスト: CREATE TABLE生成確認
    // ========================================
    [Fact]
    public void Build_Hopping_Creates_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(t => t.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new
            {
                g.Key,
                WindowStart = g.WindowStart(),
                AvgPrice = g.Average(x => x.Price)
            })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_5m_hop1m",
            model: model,
            timeframe: "5m",
            hopInterval: TimeSpan.FromMinutes(1));

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(sql, "CREATE TABLE IF NOT EXISTS trade_5m_hop1m");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING");
    }

    // ========================================
    // 新規テスト: 異なるhop間隔の検証
    // ========================================
    [Fact]
    public void Build_Hopping_Different_Intervals()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(t => t.Timestamp, TimeSpan.FromHours(1), TimeSpan.FromMinutes(15))
            .GroupBy(t => t.Symbol)
            .Select(g => new { g.Key, Avg = g.Average(x => x.Price) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_1h_hop15m",
            model: model,
            timeframe: "1h",
            hopInterval: TimeSpan.FromMinutes(15));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "SIZE 1 HOURS");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(sql, "ADVANCE BY 15 MINUTES");
    }
}
