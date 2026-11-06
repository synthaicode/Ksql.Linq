using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Visitors;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Builders.Clauses;

/// <summary>
/// Builder for WHERE clause content.
/// Rationale: separation-of-concerns; generate only predicate content without keywords.
/// Example output: "condition1 AND condition2" (excluding WHERE)
/// </summary>
internal class WhereClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Where;
    private readonly System.Collections.Generic.IDictionary<string, string>? _paramToAlias;

    public WhereClauseBuilder() { }
    public WhereClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToAlias)
    {
        _paramToAlias = paramToAlias;
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = _paramToAlias == null ? new WhereExpressionVisitor() : new WhereExpressionVisitor(_paramToAlias);
        visitor.Visit(expression);
        return visitor.GetResult();
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // WHERE-specific validation
        ValidateNoAggregateInWhere(expression);
        ValidateNoSelectStatements(expression);
    }

    /// <summary>
    /// Disallow aggregate functions in WHERE clause
    /// </summary>
    private static void ValidateNoAggregateInWhere(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);

        if (visitor.HasAggregates)
        {
            throw new InvalidOperationException(
                "Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.");
        }
    }

    /// <summary>
    /// Disallow SELECT statements within WHERE clause
    /// </summary>
    private static void ValidateNoSelectStatements(Expression expression)
    {
        var visitor = new SelectDetectionVisitor();
        visitor.Visit(expression);
        if (visitor.HasSelect)
        {
            throw new InvalidOperationException(
                "Subqueries are not supported in WHERE clause in KSQL");
        }
    }

    private sealed class SelectDetectionVisitor : ExpressionVisitor
    {
        public bool HasSelect { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(Queryable.Select) &&
                (node.Method.DeclaringType == typeof(Queryable) ||
                 node.Method.DeclaringType == typeof(Enumerable)))
                HasSelect = true;
            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// Build only conditions (without WHERE prefix)
    /// </summary>
    public string BuildCondition(Expression expression)
    {
        return BuildInternal(expression);
    }
}
