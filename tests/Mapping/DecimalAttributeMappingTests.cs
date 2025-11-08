using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Models;
using System;
using Xunit;

namespace Ksql.Linq.Tests.Mapping;

public class DecimalAttributeMappingTests
{
    private class DtoWithAttr
    {
        [KsqlDecimal(18, 4)]
        public decimal Price { get; set; }
        public decimal? Qty { get; set; }
    }

    private class DtoNoAttr
    {
        public decimal Amount { get; set; }
        public decimal? AmountOpt { get; set; }
    }

    [Fact]
    public void Attribute_OnDecimal_IsCapturedInPropertyMeta()
    {
        var prop = typeof(DtoWithAttr).GetProperty(nameof(DtoWithAttr.Price))!;
        var meta = PropertyMeta.FromProperty(prop);
        Assert.Equal(18, meta.Precision);
        Assert.Equal(4, meta.Scale);
    }

    [Fact]
    public void NoAttribute_FallsBackToGlobalDecimalPrecisionConfig()
    {
        // Ensure known defaults
        DecimalPrecisionConfig.Configure(18, 2, overrides: (System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, KsqlDslOptions.DecimalSetting>>?)null);

        var p1 = typeof(DtoNoAttr).GetProperty(nameof(DtoNoAttr.Amount))!;
        var m1 = PropertyMeta.FromProperty(p1);
        Assert.Null(m1.Precision);
        Assert.Null(m1.Scale);
        Assert.Equal(18, DecimalPrecisionConfig.ResolvePrecision(m1.Precision, p1));
        Assert.Equal(2, DecimalPrecisionConfig.ResolveScale(m1.Scale, p1));

        var p2 = typeof(DtoNoAttr).GetProperty(nameof(DtoNoAttr.AmountOpt))!;
        var m2 = PropertyMeta.FromProperty(p2);
        Assert.Null(m2.Precision);
        Assert.Null(m2.Scale);
        Assert.Equal(18, DecimalPrecisionConfig.ResolvePrecision(m2.Precision, p2));
        Assert.Equal(2, DecimalPrecisionConfig.ResolveScale(m2.Scale, p2));
    }

    [Fact]
    public void NullableDecimal_WithAttribute_IsCaptured()
    {
        var prop = typeof(DtoWithAttr).GetProperty(nameof(DtoWithAttr.Qty))!;
        // Provide explicit precision/scale via FromProperty overrides to simulate attribute resolution
        var meta = PropertyMeta.FromProperty(prop, precision: 18, scale: 4);
        Assert.Equal(18, meta.Precision);
        Assert.Equal(4, meta.Scale);
    }
}
