using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Dsl;

namespace Ksql.Linq.Query.Hub.Analysis;

/// <summary>
/// Builds a mapping from projection aliases to underlying hub column names by inspecting the Select projection.
/// </summary>
internal static class HubProjectionAnalyzer
{
    private static readonly HashSet<string> SupportedAggregates = new(StringComparer.OrdinalIgnoreCase)
    {
        "EARLIESTBYOFFSET",
        "LATESTBYOFFSET",
        "MAX",
        "MIN",
        "SUM",
        "AVERAGE",
        "AVG"
    };

    public static IReadOnlyDictionary<string, string> BuildColumnMap(KsqlQueryModel model, IEnumerable<string>? fallbackNames = null)
    {
        var projection = model.SelectProjection;
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (projection != null)
        {
            AnalyzeProjectionBody(projection.Body, map);
        }

        if (fallbackNames != null)
        {
            foreach (var name in fallbackNames)
            {
                if (string.IsNullOrWhiteSpace(name)) continue;
                if (!map.ContainsKey(name))
                {
                    map[name] = Sanitize(name);
                }
            }
        }

        return map;
    }

    private static void AnalyzeProjectionBody(Expression body, IDictionary<string, string> map)
    {
        switch (body)
        {
            case MemberInitExpression init:
                foreach (var binding in init.Bindings.OfType<MemberAssignment>())
                {
                    var alias = binding.Member.Name;
                    TryAddColumn(alias, binding.Expression, map);
                }
                break;
            case NewExpression nex when nex.Members != null:
                for (var i = 0; i < nex.Members.Count; i++)
                {
                    var alias = nex.Members[i].Name;
                    var expr = nex.Arguments[i];
                    TryAddColumn(alias, expr, map);
                }
                break;
            default:
                // Unsupported projection shape; nothing to map.
                break;
        }
    }

    private static void TryAddColumn(string alias, Expression expression, IDictionary<string, string> map)
    {
        if (string.IsNullOrWhiteSpace(alias)) return;
        map[alias] = ExtractColumnName(alias, expression) ?? Sanitize(alias);
    }

    private static string? ExtractColumnName(string alias, Expression expression)
    {
        expression = ExpressionUtils.UnwrapConvert(expression);

        switch (expression)
        {
            case MethodCallExpression mc when IsSupportedAggregate(mc):
                // Aggregates map to hub columns named after the alias (Open, High, etc.)
                return Sanitize(alias);
            case MemberExpression member:
                return ExtractPropertyPath(member, member.GetRootParameter()) ?? Sanitize(alias);
            case MemberInitExpression mi:
            case NewExpression:
            case ConditionalExpression:
            case BinaryExpression:
            case ConstantExpression:
            case ParameterExpression:
                return Sanitize(alias);
            case UnaryExpression unary:
                return ExtractColumnName(alias, unary.Operand);
            case LambdaExpression lambda:
                return ExtractColumnName(alias, lambda.Body);
        }

        return null;
    }

    private static bool IsSupportedAggregate(MethodCallExpression mc)
    {
        var name = mc.Method.Name;
        if (SupportedAggregates.Contains(name))
            return true;

        if (mc.Method.IsGenericMethod)
        {
            var genericName = mc.Method.GetGenericMethodDefinition().Name;
            return SupportedAggregates.Contains(genericName);
        }

        return false;
    }

    private static string? ExtractPropertyPath(Expression expression, ParameterExpression? parameter)
    {
        var path = new Stack<string>();
        var current = expression;

        while (current is MemberExpression member)
        {
            path.Push(Sanitize(member.Member.Name));
            current = member.Expression!;
        }

        if (parameter == null)
        {
            parameter = expression switch
            {
                MemberExpression m => m.GetRootParameter(),
                _ => null
            };
        }

        if (current == parameter)
        {
            return string.Join('.', path).ToUpperInvariant();
        }

        return null;
    }

    private static string Sanitize(string value)
    {
        return KsqlNameUtils.Sanitize(value).ToUpperInvariant();
    }

    private static ParameterExpression? GetRootParameter(this MemberExpression member)
    {
        Expression? current = member;
        while (current is MemberExpression me)
        {
            current = me.Expression;
        }
        return current as ParameterExpression;
    }
}