using System;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

public class DateFunctionTranslationTests
{
    private class Rec
    {
        public DateTime Timestamp { get; set; }
    }

    private class OutDto
    {
        public int Y { get; set; }
        public int Mo { get; set; }
        public int D { get; set; }
        public int H { get; set; }
        public int Mi { get; set; }
        public int S { get; set; }
        public DateTime Dp1 { get; set; }
        public DateTime Dp2 { get; set; }
        public DateTime Dp3 { get; set; }
        public DateTime Dp4 { get; set; }
        public string TsStr { get; set; } = string.Empty;
    }

    private static class FunctionStubs
    {
        public static int Year(DateTime dt) => dt.Year;
        public static int Month(DateTime dt) => dt.Month;
        public static int Day(DateTime dt) => dt.Day;
        public static int Hour(DateTime dt) => dt.Hour;
        public static int Minute(DateTime dt) => dt.Minute;
        public static int Second(DateTime dt) => dt.Second;

        public static DateTime AddDays(DateTime dt, int d) => dt.AddDays(d);
        public static DateTime AddHours(DateTime dt, int h) => dt.AddHours(h);
        public static DateTime AddMinutes(DateTime dt, int m) => dt.AddMinutes(m);
        public static DateTime AddSeconds(DateTime dt, int s) => dt.AddSeconds(s);
    }

    [Fact]
    public void Builder_Translates_DateParts_And_Adders_To_Supported_Ksql_Shapes()
    {
        var model = new KsqlQueryRoot()
            .From<Rec>()
            .Select(r => new OutDto
            {
                Y = FunctionStubs.Year(r.Timestamp),
                Mo = FunctionStubs.Month(r.Timestamp),
                D = FunctionStubs.Day(r.Timestamp),
                H = FunctionStubs.Hour(r.Timestamp),
                Mi = FunctionStubs.Minute(r.Timestamp),
                S = FunctionStubs.Second(r.Timestamp),
                Dp1 = FunctionStubs.AddDays(r.Timestamp, 1),
                Dp2 = FunctionStubs.AddHours(r.Timestamp, 2),
                Dp3 = FunctionStubs.AddMinutes(r.Timestamp, 3),
                Dp4 = FunctionStubs.AddSeconds(r.Timestamp, 4),
                TsStr = r.Timestamp.ToString()
            })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("date_parts_stream", model);
        var qs = QueryStructure.Parse(sql);

        Assert.Equal("STREAM", qs.CreateType);
        Assert.Equal("date_parts_stream", qs.TargetName);
        Assert.True(qs.HasEmitChanges);

        Assert.True(qs.TryGetProjection("Y", out var y));
        Assert.Contains("TIMESTAMPTOSTRING", y, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'yyyy'", y, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Mo", out var mo));
        Assert.Contains("TIMESTAMPTOSTRING", mo, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'MM'", mo, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("D", out var d));
        Assert.Contains("TIMESTAMPTOSTRING", d, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'dd'", d, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("H", out var h));
        Assert.Contains("TIMESTAMPTOSTRING", h, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'HH'", h, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Mi", out var mi));
        Assert.Contains("TIMESTAMPTOSTRING", mi, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'mm'", mi, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("S", out var s));
        Assert.Contains("TIMESTAMPTOSTRING", s, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'ss'", s, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Dp1", out var d1));
        Assert.Contains("DATEADD('day', 1, Timestamp)", d1, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Dp2", out var d2));
        Assert.Contains("DATEADD('hour', 2, Timestamp)", d2, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Dp3", out var d3));
        Assert.Contains("DATEADD('minute', 3, Timestamp)", d3, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Dp4", out var d4));
        Assert.Contains("DATEADD('second', 4, Timestamp)", d4, StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("TsStr", out var tsStr));
        Assert.Contains("FORMAT_TIMESTAMP(CAST(Timestamp AS TIMESTAMP)", tsStr, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'yyyy-MM-dd''T''HH:mm:ssXXX'", tsStr, StringComparison.OrdinalIgnoreCase);
    }
}


