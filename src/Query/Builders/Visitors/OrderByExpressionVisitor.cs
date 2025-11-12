using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Builders.Visitors;
internal class OrderByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _orderClauses = new();

    public string GetResult()
    {
        return _orderClauses.Count > 0 ? string.Join(", ", _orderClauses) : string.Empty;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        switch (methodName)
        {
            case "OrderBy":
                ProcessOrderByCall(node, "ASC");
                break;

            case "OrderByDescending":
                ProcessOrderByCall(node, "DESC");
                break;

            case "ThenBy":
                ProcessThenByCall(node, "ASC");
                break;

            case "ThenByDescending":
                ProcessThenByCall(node, "DESC");
                break;

            default:
                // Process other method calls recursively
                base.VisitMethodCall(node);
                break;
        }

        return node;
    }

    /// <summary>
    /// Handle OrderBy/OrderByDescending
    /// </summary>
    private void ProcessOrderByCall(MethodCallExpression node, string direction)
    {
        // Process previous method chain first
        if (node.Object != null)
        {
            Visit(node.Object);
        }

        // Process the current OrderBy
        if (node.Arguments.Count >= 2)
        {
            var keySelector = ExtractLambdaExpression(node.Arguments[1]);
            if (keySelector != null)
            {
                var body = Ksql.Linq.Query.Builders.Common.ExpressionUtils.UnwrapConvert(keySelector.Body);
                if (body is MethodCallExpression mc)
                {
                    var name = mc.Method.Name;
                    if (string.Equals(name, "RowTime", StringComparison.Ordinal))
                    {
                        // allowed implicitly
                    }
                    else
                    {
                        var mapping = Ksql.Linq.Query.Builders.Functions.KsqlFunctionRegistry.GetMapping(name);
                        if (mapping == null || !mapping.AllowedInOrderBy)
                            throw new InvalidOperationException($"Function '{name}' is not supported in ORDER BY clause");
                    }
                }
                var columnName = ExtractColumnName(body);
                _orderClauses.Add($"{columnName} {direction}");
            }
        }
    }

    /// <summary>
    /// Handle ThenBy/ThenByDescending
    /// </summary>
    private void ProcessThenByCall(MethodCallExpression node, string direction)
    {
        // Process previous method chain first
        if (node.Object != null)
        {
            Visit(node.Object);
        }

        // Process the current ThenBy
        if (node.Arguments.Count >= 2)
        {
            var keySelector = ExtractLambdaExpression(node.Arguments[1]);
            if (keySelector != null)
            {
                var body = Ksql.Linq.Query.Builders.Common.ExpressionUtils.UnwrapConvert(keySelector.Body);
                if (body is MethodCallExpression mc)
                {
                    var name = mc.Method.Name;
                    if (string.Equals(name, "RowTime", StringComparison.Ordinal))
                    {
                        // allowed implicitly
                    }
                    else
                    {
                        var mapping = Ksql.Linq.Query.Builders.Functions.KsqlFunctionRegistry.GetMapping(name);
                        if (mapping == null || !mapping.AllowedInOrderBy)
                            throw new InvalidOperationException($"Function '{name}' is not supported in ORDER BY clause");
                    }
                }
                var columnName = ExtractColumnName(body);
                _orderClauses.Add($"{columnName} {direction}");
            }
        }
    }

    /// <summary>
    /// Extract lambda expression
    /// </summary>
    private static LambdaExpression? ExtractLambdaExpression(Expression expr)
    {
        return expr switch
        {
            UnaryExpression unary when unary.Operand is LambdaExpression lambda => lambda,
            LambdaExpression lambda => lambda,
            _ => null
        };
    }

    /// <summary>
    /// Extract column name
    /// </summary>
    private string ExtractColumnName(Expression expr)
    {
        expr = Ksql.Linq.Query.Builders.Common.ExpressionUtils.UnwrapConvert(expr);
        return expr switch
        {
            MemberExpression member => GetMemberName(member),
            MethodCallExpression method => ProcessOrderByFunction(method),
            _ => throw new InvalidOperationException($"Unsupported ORDER BY expression: {expr.GetType().Name}")
        };
    }

    /// <summary>
    /// Get member name
    /// </summary>
    private static string GetMemberName(MemberExpression member)
    {
        // Use the deepest property name for nested properties
        return member.Member.Name;
    }

    /// <summary>
    /// Handle ORDER BY functions
    /// </summary>
    private string ProcessOrderByFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;
        if (string.Equals(methodName, "RowTime", StringComparison.Ordinal))
            return "ROWTIME";
        // Restrict ORDER BY via registry-flagged allowlist
        var mapping = Ksql.Linq.Query.Builders.Functions.KsqlFunctionRegistry.GetMapping(methodName);
        if (mapping == null || !mapping.AllowedInOrderBy)
            throw new InvalidOperationException($"Function '{methodName}' is not supported in ORDER BY clause");
        return Ksql.Linq.Query.Builders.Functions.KsqlFunctionTranslator.TranslateMethodCall(methodCall);
    }

    /// <summary>
    /// Handle simple functions
    /// </summary>
    private string ProcessSimpleFunction(string ksqlFunction, MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        return $"{ksqlFunction}({columnName})";
    }
}

/// <summary>
