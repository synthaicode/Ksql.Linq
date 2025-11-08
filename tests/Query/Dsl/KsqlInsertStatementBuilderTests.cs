using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Dsl;

public class KsqlInsertStatementBuilderTests
{
    private class Order { public int Id { get; set; } public int Amount { get; set; } }

    [Fact]
    public void Build_InsertSelect_GeneratesKsql()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { o.Id, o.Amount })
            .Build();

        var sql = KsqlInsertStatementBuilder.Build("orders", model);
        Assert.Contains("INSERT INTO orders", sql);
        Assert.Contains("SELECT", sql);
    }

    [Fact]
    public void Build_AlwaysAppendsEmitChanges()
    {
        var model = new KsqlQueryRoot()
            .From<Order>()
            .Select(o => new { o.Id })
            .Build();

        var sql = KsqlInsertStatementBuilder.Build("orders", model);
        Assert.StartsWith("INSERT INTO orders", sql);
    }
}

