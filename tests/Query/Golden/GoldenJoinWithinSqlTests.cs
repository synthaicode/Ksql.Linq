using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

public class GoldenJoinWithinSqlTests
{
    private class Order { public int Id { get; set; } public int CustomerId { get; set; } }
    private class Customer { public int Id { get; set; } public bool IsActive { get; set; } public string Name { get; set; } = string.Empty; }

    [Fact]
    public void JoinWithin_Default300_Equals_Golden()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id) // no Within -> default 300 seconds
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("JOIN_DEFAULT", model);
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/join_within_default.sql", sql);
    }

    [Fact]
    public void JoinWithin_Explicit300_Equals_Golden()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Join<Customer>((o, c) => o.CustomerId == c.Id)
            .Within(System.TimeSpan.FromSeconds(300))
            .Select((o, c) => new { o.Id, c.Name })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("JOIN_EXPLICIT", model);
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/join_within_explicit_300s.sql", sql);
    }
}


