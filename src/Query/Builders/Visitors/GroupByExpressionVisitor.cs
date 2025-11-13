using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Ksql.Linq.Query.Builders.Visitors;
internal class GroupByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _keys = new();
    private readonly IDictionary<string, string>? _paramToSource;
    private readonly bool _forcePrefixAll;

    public GroupByExpressionVisitor(bool forcePrefixAll = false) { _forcePrefixAll = forcePrefixAll; }
    public GroupByExpressionVisitor(IDictionary<string, string> paramToSource, bool forcePrefixAll = false)
    {
        _paramToSource = paramToSource;
        _forcePrefixAll = forcePrefixAll;
    }

    public string GetResult()
    {
        return _keys.Count > 0 ? string.Join(", ", _keys) : string.Empty;
    }

    protected override Expression VisitNew(NewExpression node)
    {
        foreach (var arg in node.Arguments)
        {
            if (arg is NewExpression nested)
            {
                Visit(nested);
            }
            else if (arg.NodeType == ExpressionType.Call && arg is MethodCallExpression call)
            {
                var func = ProcessGroupByFunction(call);
                _keys.Add(func);
            }
            else
            {
                var key = ProcessKeyExpression(arg);
                _keys.Add(key);
            }
        }

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        var key = ProcessKeyExpression(node);
        _keys.Add(key);
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        // Handle type conversions (e.g., Convert)
        if (node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
        {
            return Visit(node.Operand);
        }

        return base.VisitUnary(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        if (IsAllowedGroupByFunction(methodName))
        {
            var functionCall = ProcessGroupByFunction(node);
            _keys.Add(functionCall);
            return node;
        }

        throw new InvalidOperationException(
            $"Function '{methodName}' is not allowed in GROUP BY clause");
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        var expression = ProcessBinaryExpression(node);
        _keys.Add(expression);
        return node;
    }

    private string ProcessKeyExpression(Expression expr)
    {
        if (expr is ConstantExpression)
        {
            throw new InvalidOperationException("Constant expression is not supported in GROUP BY");
        }

        return ProcessExpression(expr);
    }

    private string ProcessExpression(Expression expr)
    {
        if (expr is MemberExpression member)
            return GetMemberName(member);

        if (expr is ConstantExpression constant)
            return constant.Value?.ToString() ?? "NULL";

        if (expr is UnaryExpression unary &&
            (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            return ProcessExpression(unary.Operand);

        if (expr is MethodCallExpression method)
            return ProcessGroupByFunction(method);

        if (expr is BinaryExpression binary)
            return ProcessBinaryExpression(binary);

        throw new InvalidOperationException($"Expression type '{expr.GetType().Name}' is not supported in GROUP BY");
    }

    private string ProcessBinaryExpression(BinaryExpression binary)
    {
        var left = ProcessExpression(binary.Left);
        var right = ProcessExpression(binary.Right);

        if (binary.NodeType == ExpressionType.Coalesce)
        {
            return $"COALESCE({left}, {right})";
        }

        var op = GetOperator(binary.NodeType);
        return $"{left} {op} {right}";
    }

    private static string GetOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in GROUP BY")
        };
    }

    /// <summary>
    /// Get member name
    /// </summary>
    private string GetMemberName(MemberExpression member)
    {
        if (member.Member is PropertyInfo prop)
        {
            Expression? expr = member;
            while (expr is MemberExpression m)
                expr = m.Expression;

            string prefix = string.Empty;
            if (expr is ParameterExpression pe &&
                _paramToSource != null &&
                _paramToSource.TryGetValue(pe.Name ?? string.Empty, out var source) &&
                !string.IsNullOrWhiteSpace(source))
            {
                prefix = source;
            }

            var name = KsqlNameUtils.Sanitize(prop.Name).ToUpperInvariant();

            // If force-all is disabled and the property is not a key, return bare name for compatibility
            if (!_forcePrefixAll && prop.GetCustomAttribute<KsqlKeyAttribute>() == null)
            {
                return prop.Name;
            }

            return string.IsNullOrWhiteSpace(prefix) ? name : $"{prefix}.{name}";
        }

        // Use the deepest property name for nested properties
        return member.Member.Name;
    }

    /// <summary>
    /// Determine allowed functions in GROUP BY
    /// </summary>
    private static bool IsAllowedGroupByFunction(string methodName)
    {
        var mapping = Functions.KsqlFunctionRegistry.GetMapping(methodName);
        return mapping?.AllowedInGroupBy == true;
    }

    /// <summary>
    /// Process GROUP BY functions
    /// </summary>
    private string ProcessGroupByFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        // If registry says it's allowed, delegate to the generic translator for consistency
        var mapping = Functions.KsqlFunctionRegistry.GetMapping(methodName);
        if (mapping?.AllowedInGroupBy == true)
        {
            return Functions.KsqlFunctionTranslator.TranslateMethodCall(methodCall);
        }

        throw new InvalidOperationException($"Unsupported GROUP BY function: {methodName}");
    }

    /// <summary>
    /// Extract column name
    /// </summary>
    private string ExtractColumnName(Expression expression)
    {
        expression = Ksql.Linq.Query.Builders.Common.ExpressionUtils.UnwrapConvert(expression);
        return expression switch
        {
            MemberExpression member => GetMemberName(member),
            _ => throw new InvalidOperationException($"Cannot extract column name from {expression.GetType().Name}")
        };
    }

    /// <summary>
    /// Extract constant value
    /// </summary>
    private string ExtractConstantValue(Expression expression)
    {
        var value = Ksql.Linq.Query.Builders.Common.ExpressionUtils.ExtractConstantValue(expression, nullFallback: "NULL", defaultFallback: null);
        if (value is null)
            throw new InvalidOperationException($"Expected constant value but got {expression.GetType().Name}");
        return value;
    }
}
