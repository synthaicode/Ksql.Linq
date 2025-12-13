using System;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Visitors;
using Ksql.Linq.Query.Hub.Analysis;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders.Visitors;

public class SelectExpressionVisitorAggregateOverrideTests
{
    private class Rate
    {
        public double Bid { get; set; }
    }

    private class AggregateResult
    {
        public double RoundAvg1 { get; set; }
    }

    [Fact]
    public void VisitNew_UsesAggregateOverrideForAggregateMember()
    {
        Expression<Func<IGrouping<string, Rate>, AggregateResult>> select =
            g => new AggregateResult
            {
                RoundAvg1 = g.Sum(x => x.Bid)
            };

        var paramToSource = new System.Collections.Generic.Dictionary<string, string> { ["g"] = "rows" };
        var overrides = new System.Collections.Generic.Dictionary<string, HubProjectionOverride>
        {
            ["RoundAvg1"] = HubProjectionOverride.ForAggregate("SUMBID", "Sum")
        };

        var visitor = new SelectExpressionVisitor(paramToSource, overrides, "rows");
        visitor.Visit(select.Body);
        var sql = visitor.GetResult();

        Assert.Contains("SUM(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SUMBID", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LATEST_BY_OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void VisitNew_ComputedAggregateKeepsAggregateCallWhenOverrideMarkedAggregate()
    {
        Expression<Func<IGrouping<string, Rate>, AggregateResult>> select =
            g => new AggregateResult
            {
                RoundAvg1 = Math.Round(g.Average(x => x.Bid), 1)
            };

        var paramToSource = new System.Collections.Generic.Dictionary<string, string> { ["g"] = "rows" };
        var overrides = new System.Collections.Generic.Dictionary<string, HubProjectionOverride>
        {
            ["RoundAvg1"] = HubProjectionOverride.ForAggregateOnly("SUMBID", "Sum")
        };

        var visitor = new SelectExpressionVisitor(paramToSource, overrides, "rows");
        visitor.Visit(select.Body);
        var sql = visitor.GetResult();

        Assert.Contains("ROUND(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SUM", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CNT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RoundAvg1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LATEST_BY_OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }
}
