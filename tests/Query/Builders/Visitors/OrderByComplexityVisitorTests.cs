using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Visitors;
using System;
using System.Linq.Expressions;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders.Visitors;

[Trait("Level", TestLevel.L3)]
public class OrderByComplexityVisitorTests
{
    private class Sample
    {
        public int Id { get; set; }
        public int Score { get; set; }
    }

    [Fact]
    public void Visit_ComplexExpression_SetsFlag()
    {
        Expression<Func<Sample, int>> expr = e => e.Id + e.Score * 2;
        var visitor = new OrderByComplexityVisitor();
        visitor.Visit(expr.Body);
        Assert.True(visitor.HasComplexExpressions);
    }

    [Fact]
    public void Visit_SimpleMemberExpression_FlagRemainsFalse()
    {
        Expression<Func<Sample, int>> expr = e => e.Id;
        var visitor = new OrderByComplexityVisitor();
        visitor.Visit(expr.Body);
        Assert.False(visitor.HasComplexExpressions);
    }
}

