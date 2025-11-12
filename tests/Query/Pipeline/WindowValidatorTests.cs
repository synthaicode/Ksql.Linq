using System;
using Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Ksql.Linq.Tests.Query.Pipeline;

public class WindowValidatorTests
{
    [Fact]
    public void Validate_Populates_Grace_Chain()
    {
        var result = new ExpressionAnalysisResult();
        result.Windows.AddRange(new[] { "1m", "5m", "1h" });
        result.BaseUnitSeconds = 1;
        result.GraceSeconds = 2;

        WindowValidator.Validate(result);

        Assert.True(result.GracePerTimeframe.TryGetValue("1m", out var g1) && g1 == 3);
        Assert.True(result.GracePerTimeframe.TryGetValue("5m", out var g2) && g2 == 4);
        Assert.True(result.GracePerTimeframe.TryGetValue("1h", out var g3) && g3 == 5);
    }

    [Fact]
    public void Validate_Throws_When_Base_Not_Dividing_60()
    {
        var result = new ExpressionAnalysisResult();
        result.Windows.Add("1m");
        result.BaseUnitSeconds = 7; // invalid
        var ex = Assert.Throws<InvalidOperationException>(() => WindowValidator.Validate(result));
        Assert.Contains("Base unit", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_Throws_When_Window_Not_Multiple_Of_Base()
    {
        var result = new ExpressionAnalysisResult();
        result.Windows.Add("7s");
        result.BaseUnitSeconds = 5;
        var ex = Assert.Throws<InvalidOperationException>(() => WindowValidator.Validate(result));
        Assert.Contains("multiple of base", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_Throws_When_Seconds_Over_Minute_Not_Whole_Minute()
    {
        var result = new ExpressionAnalysisResult();
        result.Windows.Add("90s"); // >= 60s but not minute multiple
        result.BaseUnitSeconds = 1;
        var ex = Assert.Throws<InvalidOperationException>(() => WindowValidator.Validate(result));
        Assert.Contains("Windows ≥ 1 minute", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}

