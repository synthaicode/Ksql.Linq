using Ksql.Linq.Query.Builders.Clauses;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Tests.Utils;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", TestLevel.L3)]
public class NumericSqlCultureRegressionTests
{
    public static IEnumerable<object[]> NumericExpressions()
    {
        foreach (var culture in new[] { "en-US", "fr-FR" })
        {
            yield return new object[] { culture, nameof(NumericRow.Price), 1.5m, "1.5" };
            yield return new object[] { culture, nameof(NumericRow.DoubleValue), 1.5d, "1.5" };
            yield return new object[] { culture, nameof(NumericRow.FloatValue), 1.5f, "1.5" };
            yield return new object[] { culture, nameof(NumericRow.Quantity), 15, "15" };
        }
    }

    [Theory]
    [MemberData(nameof(NumericExpressions))]
    public void Where_NumericPredicate_UsesProtocolLiteral(string culture, string property, object value, string literal)
    {
        UnderCulture(culture, () =>
        {
            var member = Expression.Property(Expression.Parameter(typeof(NumericRow), "row"), property);
            var predicate = Expression.GreaterThan(member, Expression.Constant(value));
            Assert.Equal($"({property} > {literal})", new WhereClauseBuilder().BuildCondition(predicate));
        });
    }

    [Theory]
    [MemberData(nameof(NumericExpressions))]
    public void GroupBy_ArithmeticConstant_UsesProtocolLiteral(string culture, string property, object value, string literal)
    {
        UnderCulture(culture, () =>
        {
            var member = Expression.Property(Expression.Parameter(typeof(NumericRow), "row"), property);
            var key = Expression.Add(member, Expression.Constant(value));
            Assert.Equal($"{property} + {literal}", new GroupByClauseBuilder().Build(key));
        });
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("fr-FR")]
    public void SelectAndHaving_SharedNumericFormatting_UsesProtocolLiteral(string culture)
    {
        UnderCulture(culture, () =>
        {
            Expression<Func<NumericRow, object>> projection = row => new { Adjusted = row.DoubleValue + 1.5d };
            Expression<Func<IGrouping<int, NumericRow>, bool>> having = group => group.Sum(row => row.Price) > 1.5m;
            Assert.Equal("(DOUBLEVALUE + 1.5) AS Adjusted", new SelectClauseBuilder().Build(projection.Body));
            Assert.Equal("(SUM(Price) > 1.5)", new HavingClauseBuilder().Build(having.Body));
        });
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("fr-FR")]
    public void SafeToString_PreservesNonnumericBehavior(string culture)
    {
        UnderCulture(culture, () =>
        {
            Assert.Equal("NULL", BuilderValidation.SafeToString(null));
            Assert.Equal("''", BuilderValidation.SafeToString(string.Empty));
            Assert.Equal("'text'", BuilderValidation.SafeToString("text"));
            Assert.Equal("true", BuilderValidation.SafeToString(true));
            Assert.Equal("false", BuilderValidation.SafeToString(false));
            var date = new DateTime(2026, 11, 6, 13, 14, 15);
            Assert.Equal(date.ToString(), BuilderValidation.SafeToString(date));
            Assert.Equal("Friday", BuilderValidation.SafeToString(DayOfWeek.Friday));
        });
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("fr-FR")]
    public void GroupBy_NonnumericConstants_PreserveExistingRendering(string culture)
    {
        UnderCulture(culture, () =>
        {
            Expression<Func<NumericRow, string>> key = row => row.Name ?? "fallback";
            Assert.Equal("COALESCE(Name, fallback)", new GroupByClauseBuilder().Build(key.Body));
        });
    }

    private static void UnderCulture(string name, Action assertion)
    {
        // CurrentCulture follows this execution context; do not mutate process defaults.
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
            assertion();
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    private sealed class NumericRow
    {
        public decimal Price { get; set; }
        public double DoubleValue { get; set; }
        public float FloatValue { get; set; }
        public int Quantity { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
