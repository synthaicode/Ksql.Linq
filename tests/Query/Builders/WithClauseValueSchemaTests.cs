using System;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", TestLevel.L3)]
public class WithClauseValueSchemaTests
{
    private class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class Xform
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Close { get; set; }
    }

    [Fact]
    public void Builder_Includes_ValueSchemaFullName_In_WithClause_When_Provided_In_Extras()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new Xform
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Close = g.LatestByOffset(x => x.Bid)
            })
            .Build();

        // Provide value schema fullname via Extras to simulate pipeline injection
        var fullname = "my.ns.xform_1m_live_valueAvro";
        model.Extras["valueSchemaFullName"] = fullname;

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "xform_1m_live",
            model: model,
            timeframe: "1m");

        SqlAssert.ContainsNormalized(sql, "VALUE_AVRO_SCHEMA_FULL_NAME='my.ns.xform_1m_live_valueAvro'");
        SqlAssert.ContainsNormalized(sql, "CREATE TABLE IF NOT EXISTS xform_1m_live");
        SqlAssert.ContainsNormalized(sql, "WINDOW TUMBLING (SIZE 1 MINUTES)");
        SqlAssert.EndsWithSemicolon(sql);
    }
}


