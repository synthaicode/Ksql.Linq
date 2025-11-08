using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

public class GoldenPartitionBySqlTests
{
    private class KeyedOrder { [KsqlKey] public int Id { get; set; } public int CustomerId { get; set; } }

    [Fact]
    public void PartitionBy_DeduplicatesAndKeepsOrder_Equals_Golden()
    {
        var model = new KsqlQueryRoot()
            .From<KeyedOrder>()
            .Select(o => new { o.Id, o.CustomerId })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build(
            "PARTITION_VARIANT",
            model,
            partitionBy: "o.CustomerId, o.Id, o.CustomerId");

        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/partition_by_variants.sql", sql);
    }
}



