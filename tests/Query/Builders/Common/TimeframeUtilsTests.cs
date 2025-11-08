using System;
using Ksql.Linq.Query.Builders.Common;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders.Common;

public class TimeframeUtilsTests
{
    [Theory]
    [InlineData(1, "Minutes", "1m")]
    [InlineData(5, "Hours", "5h")]
    [InlineData(7, "Days", "7d")]
    [InlineData(2, "Months", "2mo")]
    public void Normalize_Formats_Correctly(int value, string unit, string expected)
    {
        var token = TimeframeUtils.Normalize(value, unit);
        Assert.Equal(expected, token);
    }

    [Theory]
    [InlineData("30s", 30)]
    [InlineData("1m", 60)]
    [InlineData("2h", 7200)]
    [InlineData("1d", 86400)]
    [InlineData("1wk", 604800)]
    [InlineData("1mo", 2592000)]
    public void ToSeconds_Works(string tf, int expected)
    {
        var seconds = TimeframeUtils.ToSeconds(tf);
        Assert.Equal(expected, seconds);
    }

    [Fact]
    public void Compare_Orders_By_Duration()
    {
        var ordered = new[] { "1mo", "1m", "30s", "1wk", "1h", "1d" };
        Array.Sort(ordered, TimeframeUtils.Compare);
        Assert.Equal(new[] { "30s", "1m", "1h", "1d", "1wk", "1mo" }, ordered);
    }

    [Theory]
    [InlineData("1m", "WINDOW TUMBLING (SIZE 1 MINUTES)")]
    [InlineData("5h", "WINDOW TUMBLING (SIZE 5 HOURS)")]
    [InlineData("1wk", "WINDOW TUMBLING (SIZE 7 DAYS)")]
    [InlineData("2mo", "WINDOW TUMBLING (SIZE 2 MONTHS)")]
    public void ToKsqlWindowClause_Renders_Correctly(string tf, string expected)
    {
        var clause = TimeframeUtils.ToKsqlWindowClause(tf);
        Assert.Equal(expected, clause);
    }

    [Theory]
    [InlineData("5m", 300_000L)]
    [InlineData("1h", 3_600_000L)]
    [InlineData("1wk", 604_800_000L)]
    [InlineData("1mo", 2_592_000_000L)]
    public void TryToMilliseconds_Converts(string tf, long expected)
    {
        Assert.True(TimeframeUtils.TryToMilliseconds(tf, out var ms));
        Assert.Equal(expected, ms);
    }

    [Theory]
    [InlineData("15mo", 15, "mo")]
    [InlineData("7d", 7, "d")]
    [InlineData("30s", 30, "s")]
    public void Decompose_Splits_Value_And_Unit(string tf, int val, string unit)
    {
        var (v, u) = TimeframeUtils.Decompose(tf);
        Assert.Equal(val, v);
        Assert.Equal(unit, u);
    }
}
