using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Ddl;

[Trait("Level", "L3")]
public class CtasSinkPartitionsReplicasTests
{
    [KsqlTopic("rate")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public System.DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static KsqlQueryModel BuildModel()
    {
        // Minimal model: FROM + WINDOW only・・ELECT *・峨〒WITH蜿･縺ｮ讒区・繧呈､懆ｨｼ縺吶ｋ
        var m = new KsqlQueryModel { SourceTypes = new[] { typeof(Rate) } };
        m.Windows.Add("1m");
        return m;
    }

    [Fact]
    public void WindowedCtas_WithExtras_EmitsSinkPartitionsAndReplicas()
    {
        var model = BuildModel();
        // Simulate pipeline attaching sink sizing via Extras
        model.Extras["sink/partitions"] = 1;
        model.Extras["sink/replicas"] = 1;
        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "bar_1m_live",
            model: model,
            timeframe: "1m",
            emitOverride: null,
            inputOverride: "bar_1s_rows");

        var upper = sql.ToUpperInvariant();
        Assert.Contains("WITH (", upper);
        Assert.Contains("PARTITIONS=1", upper);
        Assert.Contains("REPLICAS=1", upper);
    }
}

