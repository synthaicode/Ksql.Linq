using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders.Utilities;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders.Utils;

public class DecimalCastUtilsTests
{
    private class PriceRow
    {
        [KsqlKey(1)] public string Symbol { get; set; } = string.Empty;
        [KsqlDecimal(18, 4)] public decimal Price { get; set; }
    }

    [Fact]
    public void InjectDecimalCasts_CastsDecimalAliases_Idempotent()
    {
        var sql = "CREATE TABLE t AS SELECT MAX(o.Price) AS Price, o.Symbol AS Symbol FROM foo o GROUP BY Symbol EMIT CHANGES;";
        var model = new EntityModel { EntityType = typeof(PriceRow) };

        var injected = DecimalCastUtils.InjectDecimalCasts(sql, model);
        Assert.Contains("CAST(MAX(o.Price) AS DECIMAL(18, 4)) AS Price", injected);
        Assert.Contains("o.Symbol AS Symbol", injected);

        // Idempotent when called twice
        var second = DecimalCastUtils.InjectDecimalCasts(injected, model);
        Assert.Equal(injected, second);
    }
}

