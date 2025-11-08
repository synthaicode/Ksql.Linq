using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using System.Linq;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

public class GoldenKeyPathStyleSqlTests
{
    private class TableEntity
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        public int Qty { get; set; }
    }

    private static KsqlQueryModel BuildModel()
    {
        return new KsqlQueryRoot()
            .From<TableEntity>()
            .GroupBy(t => new { t.Broker, t.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, Total = g.Sum(x => x.Qty) })
            .Build();
    }

    [Fact]
    public void KeyPath_None_Equals_Golden()
    {
        var sql = KsqlCreateStatementBuilder.Build("KEYPATH_NONE", BuildModel(), options: new RenderOptions { KeyPathStyle = KeyPathStyle.None });
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/keypath_none.sql", sql);
    }

    [Fact]
    public void KeyPath_Dot_Equals_Golden()
    {
        var sql = KsqlCreateStatementBuilder.Build("KEYPATH_DOT", BuildModel(), options: new RenderOptions { KeyPathStyle = KeyPathStyle.Dot });
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/keypath_dot.sql", sql);
    }

    [Fact]
    public void KeyPath_Arrow_Equals_Golden()
    {
        var sql = KsqlCreateStatementBuilder.Build("KEYPATH_ARROW", BuildModel(), options: new RenderOptions { KeyPathStyle = KeyPathStyle.Arrow });
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/keypath_arrow.sql", sql);
    }
}


