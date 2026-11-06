using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Clauses;
using Ksql.Linq.Query.Builders.Common;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", "L3")]
public class KsqlFragmentRegressionTests
{
    [Theory]
    [InlineData("Description")]
    [InlineData("SelectedValue")]
    [InlineData("UpdatedAt")]
    [InlineData("UnionCode")]
    [InlineData("ScriptName")]
    public void WhereBuild_IdentifierSubstring_PreservesSql(string property)
    {
        var parameter = Expression.Parameter(typeof(Row), "row");
        var predicate = Expression.Equal(Expression.Property(parameter, property), Expression.Constant("ordinary"));
        Assert.Equal($"({property} = 'ordinary')", new WhereClauseBuilder().Build(predicate));
    }

    [Theory]
    [InlineData("SELECT")]
    [InlineData("DROP DELETE INSERT UPDATE EXEC EXECUTE UNION SCRIPT")]
    [InlineData("-- /* */ ;-- sp_ xp_")]
    [InlineData("O'Brien; DROP --")]
    [InlineData("quote ' SELECT ' end")]
    [InlineData("x' OR true --")]
    [InlineData("backslash \\' quote")]
    public void WhereBuild_LiteralContent_PreservesEscapedSql(string value)
    {
        var parameter = Expression.Parameter(typeof(Row), "row");
        var predicate = Expression.Equal(Expression.Property(parameter, nameof(Row.Name)), Expression.Constant(value));
        Assert.Equal($"(Name = '{value.Replace("'", "''")}')", new WhereClauseBuilder().Build(predicate));
    }

    [Theory]
    [InlineData("Name = 'ordinary'")]
    [InlineData("sp_price = 1")]
    [InlineData("SP_Price = 1")]
    [InlineData("xp_value = 1")]
    [InlineData("XP_Value = 1")]
    [InlineData("EXEC = 1")]
    [InlineData("EXECUTE = 1")]
    [InlineData("UPDATE = 1")]
    [InlineData("UNION = 1")]
    [InlineData("price@DROP = 1")]
    [InlineData("\"DROP\" = 'value'")]
    [InlineData("`DELETE` = 'value'")]
    [InlineData("\"a\"\"UNION\" = 'value'")]
    [InlineData("`a``SCRIPT` = 'value'")]
    [InlineData("Name = 'it''s; DROP -- safe'")]
    public void Build_KsqlIdentifierAndLiteralRoles_PreserveOutputBytes(string sql)
    {
        Assert.Equal(sql, new OutputBuilder(sql).Build(Expression.Constant(true)));
    }

    [Theory]
    [InlineData("DROP TABLE orders")]
    [InlineData("delete FROM orders")]
    [InlineData("INSERT INTO orders")]
    [InlineData("SCRIPT")]
    [InlineData("Name = 'x' -- comment")]
    [InlineData("Name = 'x' /* comment */")]
    [InlineData("Name = 'x' */")]
    [InlineData("Name = 'x'; DROP TABLE orders")]
    [InlineData("Name = 'unterminated")]
    [InlineData("\"unterminated")]
    [InlineData("`unterminated")]
    public void Build_ReservedCommandsOrUnsupportedRawFragmentSyntax_AreRejected(string sql)
    {
        Assert.Throws<InvalidOperationException>(() => new OutputBuilder(sql).Build(Expression.Constant(true)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void WhereBuild_ActualLinqSelect_IsRejectedAsSubquery(bool queryable)
    {
        Expression<Func<Row, bool>> predicate = queryable
            ? row => new[] { 1 }.AsQueryable().Select(value => value).Contains(row.Id)
            : row => new[] { 1 }.Select(value => value).Contains(row.Id);
        var error = Assert.Throws<InvalidOperationException>(() => new WhereClauseBuilder().Build(predicate.Body));
        Assert.Contains("Subqueries are not supported", error.Message);
    }

    private sealed class OutputBuilder : BuilderBase
    {
        private readonly string _sql;
        public OutputBuilder(string sql) => _sql = sql;
        public override KsqlBuilderType BuilderType => KsqlBuilderType.Where;
        protected override string BuildInternal(Expression expression) => _sql;
    }

    private sealed class Row
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Description { get; set; } = "";
        public string SelectedValue { get; set; } = "";
        public string UpdatedAt { get; set; } = "";
        public string UnionCode { get; set; } = "";
        public string ScriptName { get; set; } = "";
    }
}
