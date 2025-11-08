using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Functions;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;

namespace Ksql.Linq.Query.Analysis;

internal static class ProjectionMetadataAnalyzer
{
    private static readonly HashSet<string> SupportedAggregates = CreateSupportedAggregates();

    public static ProjectionMetadata Build(KsqlQueryModel model, bool isHubInput)
    {
        var members = new List<ProjectionMember>();
        var projection = model.SelectProjection;
        if (projection != null)
        {
            AnalyzeProjectionBody(projection.Body, members);
        }

        // Do not force a source alias; downstream builders decide whether an alias is required.
        return new ProjectionMetadata(members, isHubInput);
    }

    private static void AnalyzeProjectionBody(Expression body, List<ProjectionMember> output)
    {
        switch (body)
        {
            case MemberInitExpression init:
                foreach (var binding in init.Bindings.OfType<MemberAssignment>())
                {
                    var alias = binding.Member.Name;
                    output.Add(BuildMember(alias, binding.Expression));
                }
                break;
            case NewExpression nex when nex.Members != null:
                for (var i = 0; i < nex.Members.Count; i++)
                {
                    var alias = nex.Members[i].Name;
                    var expr = nex.Arguments[i];
                    output.Add(BuildMember(alias, expr));
                }
                break;
            default:
                // Unsupported projection shape; treat as computed anonymous value
                output.Add(BuildMember("value", body));
                break;
        }
    }

    private static ProjectionMember BuildMember(string alias, Expression expression)
    {
        var expr = ExpressionUtils.UnwrapConvert(expression);
        var (kind, resolvedColumn, exprText, funcName, sourcePath) = ClassifyDetailed(alias, expr);
        var type = expr.Type;
        var isNullable = !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
        return new ProjectionMember(
            Alias: alias,
            Expression: expression,
            ExpressionText: exprText ?? string.Empty,
            Kind: kind,
            ResolvedColumnName: resolvedColumn,
            AggregateFunctionName: funcName,
            SourceMemberPath: sourcePath,
            ResultType: type,
            IsNullable: isNullable);
    }

    private static (ProjectionMemberKind kind, string? resolvedColumn, string? exprText, string? funcName, string? sourcePath) ClassifyDetailed(string alias, Expression expr)
    {
        switch (expr)
        {
            case MethodCallExpression mc when IsSupportedAggregate(mc):
                return (
                    ProjectionMemberKind.Aggregate,
                    Sanitize(alias),
                    null,
                    ExtractAggregateFunctionName(mc),
                    ExtractAggregateSourcePath(mc));
            case MemberExpression member:
                return ClassifyMemberExpression(alias, member);
            case ParameterExpression:
                return (ProjectionMemberKind.Value, Sanitize(alias), null, null, null);
            case ConstantExpression:
            case BinaryExpression:
            case ConditionalExpression:
            case NewExpression:
            case MemberInitExpression:
                return (ProjectionMemberKind.Computed, Sanitize(alias), null, null, null);
            case UnaryExpression u:
                return ClassifyDetailed(alias, u.Operand);
            case LambdaExpression l:
                return ClassifyDetailed(alias, l.Body);
            default:
                return (ProjectionMemberKind.Computed, Sanitize(alias), null, null, null);
        }
    }

    private static (ProjectionMemberKind kind, string? resolvedColumn, string? exprText, string? funcName, string? sourcePath) ClassifyMemberExpression(string alias, MemberExpression member)
    {
        var path = ExtractPropertyPath(member, member.GetRootParameter());
        if (string.IsNullOrEmpty(path))
        {
            return (ProjectionMemberKind.Value, Sanitize(alias), null, null, null);
        }

        var canonical = NormalizePath(path);

        if (string.IsNullOrEmpty(canonical))
        {
            return (ProjectionMemberKind.Value, Sanitize(alias), null, null, null);
        }

        if (IsKeyPath(canonical))
        {
            var trimmed = TrimKeyPrefix(canonical);
            var resolved = string.IsNullOrEmpty(trimmed) ? Sanitize(alias) : GetLeafSegment(trimmed);
            var source = string.IsNullOrEmpty(trimmed) ? null : trimmed;
            return (ProjectionMemberKind.Key, resolved, null, null, source);
        }

        return (ProjectionMemberKind.Value, canonical, null, null, canonical);
    }

    private static string? ExtractAggregateFunctionName(MethodCallExpression mc)
    {
        var name = mc.Method.Name;
        if (!string.IsNullOrWhiteSpace(name)) return name;
        if (mc.Method.IsGenericMethod)
            return mc.Method.GetGenericMethodDefinition().Name;
        return null;
    }

    private static string? ExtractAggregateSourcePath(MethodCallExpression mc)
    {
        foreach (var arg in mc.Arguments)
        {
            var lambda = ExtractLambdaFromArgument(arg);
            if (lambda == null)
                continue;

            var body = ExpressionUtils.UnwrapConvert(lambda.Body);
            if (body is MemberExpression me)
            {
                var extracted = ExtractPropertyPath(me, me.GetRootParameter());
                return extracted != null ? NormalizePath(extracted) : null;
            }
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
            if (SupportedAggregates.Contains(genericName))
                return true;
        }
        // Also allow extension methods that already map to ksqlDB aggregate names
        var normalized = name.Replace("_", string.Empty).ToUpperInvariant();
        return SupportedAggregates.Contains(normalized);
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

    private static string Sanitize(string value) => KsqlNameUtils.Sanitize(value).ToUpperInvariant();

    private static ParameterExpression? GetRootParameter(this MemberExpression member)
    {
        Expression? current = member;
        while (current is MemberExpression me) current = me.Expression;
        return current as ParameterExpression;
    }

    private static bool IsKeyPath(string path) =>
        path.Equals("KEY", StringComparison.Ordinal) ||
        path.StartsWith("KEY.", StringComparison.Ordinal);

    private static string TrimKeyPrefix(string path) =>
        path.Equals("KEY", StringComparison.Ordinal)
            ? string.Empty
            : path.StartsWith("KEY.", StringComparison.Ordinal) ? path.Substring("KEY.".Length) : path;

    private static string GetLeafSegment(string path)
    {
        var separator = path.LastIndexOf('.');
        return separator >= 0 ? path[(separator + 1)..] : path;
    }

    private static string NormalizePath(string value) => value.Replace("`", string.Empty);

    private static LambdaExpression? ExtractLambdaFromArgument(Expression arg) =>
        arg switch
        {
            LambdaExpression le => le,
            UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression le } => le,
            ConstantExpression { Value: LambdaExpression le } => le,
            _ => null
        };

    private static HashSet<string> CreateSupportedAggregates()
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        if (categories.TryGetValue("Aggregate", out var aggregateFunctions))
        {
            foreach (var name in aggregateFunctions)
            {
                if (string.IsNullOrWhiteSpace(name))
                    continue;
                set.Add(name);
                var normalized = name.Replace("_", string.Empty).ToUpperInvariant();
                set.Add(normalized);
            }
        }
        return set;
    }
}
