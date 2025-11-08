using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Hub.Analysis;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Visitors;
using System;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Builders.Clauses;

/// <summary>
/// Builder for SELECT clause content.
/// Rationale: separation-of-concerns; generate only clause content without keywords.
/// Example output: "col1, col2 AS alias" (excluding SELECT)
/// </summary>
internal class SelectClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Select;
    private readonly System.Collections.Generic.IDictionary<string, string>? _paramToSource;
    private readonly System.Collections.Generic.Dictionary<string, (System.Type Type, int? Precision, int? Scale)>? _aliasTypeHints;
    private readonly System.Collections.Generic.ISet<string>? _excludeAliases;
    private readonly System.Collections.Generic.IDictionary<string, HubProjectionOverride>? _aggregateArgOverridesByOutputAlias;
    private readonly string _sourceAliasForOverrides = "o";

    public SelectClauseBuilder() { }
    public SelectClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource)
    {
        _paramToSource = paramToSource;
    }
    public SelectClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource, System.Collections.Generic.ISet<string> excludeAliases)
    {
        _paramToSource = paramToSource;
        _excludeAliases = excludeAliases;
    }
    public SelectClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource,
        System.Collections.Generic.Dictionary<string, (System.Type Type, int? Precision, int? Scale)> aliasTypeHints)
    {
        _paramToSource = paramToSource;
        _aliasTypeHints = aliasTypeHints;
    }
    public SelectClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource,
        System.Collections.Generic.IDictionary<string, HubProjectionOverride> aggregateArgOverridesByOutputAlias,
        string sourceAliasForOverrides)
    {
        _paramToSource = paramToSource;
        _aggregateArgOverridesByOutputAlias = aggregateArgOverridesByOutputAlias;
        if (sourceAliasForOverrides != null)
            _sourceAliasForOverrides = sourceAliasForOverrides;
    }
    public SelectClauseBuilder(System.Collections.Generic.IDictionary<string, string> paramToSource,
        System.Collections.Generic.IDictionary<string, HubProjectionOverride> aggregateArgOverridesByOutputAlias,
        string sourceAliasForOverrides,
        System.Collections.Generic.ISet<string> excludeAliases)
    {
        _paramToSource = paramToSource;
        _aggregateArgOverridesByOutputAlias = aggregateArgOverridesByOutputAlias;
        _excludeAliases = excludeAliases;
        if (sourceAliasForOverrides != null)
            _sourceAliasForOverrides = sourceAliasForOverrides;
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // No dependency on other builders
    }

    protected override string BuildInternal(Expression expression)
    {
        SelectExpressionVisitor visitor;
        if (_paramToSource == null)
        {
            visitor = new SelectExpressionVisitor();
        }
        else if (_aliasTypeHints != null)
        {
            visitor = new SelectExpressionVisitor(_paramToSource, _aliasTypeHints);
        }
        else if (_aggregateArgOverridesByOutputAlias != null && _excludeAliases != null)
        {
            visitor = new SelectExpressionVisitor(_paramToSource, _aggregateArgOverridesByOutputAlias, _sourceAliasForOverrides, _excludeAliases);
        }
        else if (_aggregateArgOverridesByOutputAlias != null)
        {
            visitor = new SelectExpressionVisitor(_paramToSource, _aggregateArgOverridesByOutputAlias, _sourceAliasForOverrides);
        }
        else if (_excludeAliases != null)
        {
            visitor = new SelectExpressionVisitor(_paramToSource, _excludeAliases);
        }
        else
        {
            visitor = new SelectExpressionVisitor(_paramToSource);
        }
        visitor.Visit(expression);

        var result = visitor.GetResult();

        // Return * when empty
        return string.IsNullOrWhiteSpace(result) ? "*" : result;
    }

    public string BuildWithParamMap(System.Linq.Expressions.Expression expression, System.Collections.Generic.IDictionary<string, string> paramToSource)
    {
        var visitor = new SelectExpressionVisitor(paramToSource);
        visitor.Visit(expression);

        var result = visitor.GetResult();
        return string.IsNullOrWhiteSpace(result) ? "*" : result;
    }

    public string BuildWithParamMapAndExclude(System.Linq.Expressions.Expression expression,
        System.Collections.Generic.IDictionary<string, string> paramToSource,
        System.Collections.Generic.ISet<string> excludeAliases)
    {
        var visitor = new SelectExpressionVisitor(paramToSource, excludeAliases);
        visitor.Visit(expression);
        var result = visitor.GetResult();
        return string.IsNullOrWhiteSpace(result) ? "*" : result;
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // SELECT-specific validation
        BuilderValidation.ValidateNoNestedAggregates(expression);

        if (expression is MethodCallExpression)
        {
            // Check for mixing aggregate and non-aggregate functions
            if (ContainsAggregateFunction(expression) && ContainsNonAggregateColumns(expression))
            {
                throw new InvalidOperationException(
                    "SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY");
            }
        }
    }

    /// <summary>
    /// Check for presence of aggregate functions
    /// </summary>
    private static bool ContainsAggregateFunction(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);
        return visitor.HasAggregates;
    }

    /// <summary>
    /// Check for presence of non-aggregate columns
    /// </summary>
    private static bool ContainsNonAggregateColumns(Expression expression)
    {
        var visitor = new NonAggregateColumnVisitor();
        visitor.Visit(expression);
        return visitor.HasNonAggregateColumns;
    }
}


