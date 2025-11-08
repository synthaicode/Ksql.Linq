using System;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

public class NonTumblingDateFunctionTranslationTests
{
    private class Rec
    {
        public DateTime Timestamp { get; set; }
    }

    private class OutDto
    {
        public int Wk { get; set; }
        public int Dow { get; set; }
    }

    private static int WeekOfYear(DateTime dt) => int.Parse(dt.ToString("w"));
    private static int DayOfWeek(DateTime dt) => (int)dt.DayOfWeek;

    [Fact]
    public void NonTumbling_DateFunctions_Are_Handled_In_Ksql()
    {
        var model = new KsqlQueryRoot()
            .From<Rec>()
            .Select(r => new OutDto
            {
                Wk = WeekOfYear(r.Timestamp),
                Dow = DayOfWeek(r.Timestamp)
            })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("date_nontumbling", model);
        var qs = QueryStructure.Parse(sql);
        Assert.Equal("STREAM", qs.CreateType);
        Assert.Equal("date_nontumbling", qs.TargetName);
        Assert.True(qs.TryGetProjection("Wk", out var wkExpr));
        // WeekOfYear mapping uses FORMAT_TIMESTAMP(..., 'w', 'UTC') normalized to an INT
        Assert.Contains("FORMAT_TIMESTAMP", wkExpr, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'w'", wkExpr, System.StringComparison.OrdinalIgnoreCase);

        Assert.True(qs.TryGetProjection("Dow", out var dowExpr));
        // DayOfWeek maps to DAYOFWEEK function
        Assert.Contains("DAYOFWEEK", dowExpr, System.StringComparison.OrdinalIgnoreCase);
    }
}


