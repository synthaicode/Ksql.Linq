using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Functions;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;

namespace Ksql.Linq.Query.Hub.Analysis;

/// <summary>
/// Hub-specific selection policy: decides which projection aliases should have
/// their aggregate arguments overridden to hub columns and which aliases should
/// be excluded (computed) from CTAS in hub flows.
/// </summary>
internal static class HubSelectPolicy
{
    public static void BuildOverridesAndExcludes(
        ProjectionMetadata meta,
        out Dictionary<string, HubProjectionOverride> overrides,
        out HashSet<string> excludeAliases,
        System.Collections.Generic.ISet<string>? availableColumns = null)
    {
        overrides = new Dictionary<string, HubProjectionOverride>(StringComparer.OrdinalIgnoreCase);
        excludeAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var m in meta.Members)
        {
            if (string.IsNullOrWhiteSpace(m.Alias))
                continue;

            if (m.Kind == ProjectionMemberKind.Aggregate)
            {
                var target = ResolveTargetColumn(m);
                if (!string.IsNullOrWhiteSpace(target))
                {
                    overrides[m.Alias] = HubProjectionOverride.ForAggregate(target!, m.AggregateFunctionName);
                    try { Console.WriteLine($"[hub.override] alias={m.Alias} target={target} func={m.AggregateFunctionName} kind={m.Kind}"); } catch { }
                }
                continue;
            }
            // 非集計（Computed）は CTAS へ昇格させない（Rows のみ）。
            if (m.Kind == ProjectionMemberKind.Computed)
            {
                var alias = m.Alias ?? m.ResolvedColumnName;
                if (!string.IsNullOrWhiteSpace(alias))
                    excludeAliases.Add(alias!);
                continue;
            }
        }
    }

    private static bool ShouldPromoteComputedAlias(ProjectionMember member, System.Collections.Generic.ISet<string>? availableColumns)
    {
        // 新方針: Computed は昇格させない（常に Rows 側で完結）。
        return false;
    }

    private static string? ResolveTargetColumn(ProjectionMember member)
    {
        if (!string.IsNullOrWhiteSpace(member.ResolvedColumnName))
            return member.ResolvedColumnName;
        if (!string.IsNullOrWhiteSpace(member.Alias))
            return KsqlNameUtils.Sanitize(member.Alias).ToUpperInvariant();
        return null;
    }

    private static bool ContainsAggregate(System.Linq.Expressions.Expression expression)
    {
        var visitor = new AggregateProbeVisitor();
        visitor.Visit(expression);
        return visitor.FoundAggregate;
    }

    private sealed class AggregateProbeVisitor : System.Linq.Expressions.ExpressionVisitor
    {
        public bool FoundAggregate { get; private set; }

        public override System.Linq.Expressions.Expression? Visit(System.Linq.Expressions.Expression? node)
        {
            if (FoundAggregate || node == null)
                return node;

            if (node is System.Linq.Expressions.MethodCallExpression mc)
            {
                var name = mc.Method.Name;
                if (!string.IsNullOrWhiteSpace(name) && KsqlFunctionRegistry.IsAggregateFunction(name))
                {
                    FoundAggregate = true;
                    return node;
                }
            }

            return base.Visit(node);
        }
    }
}

internal sealed record HubProjectionOverride(string TargetColumn, string? AggregateFunctionName, bool AggregateOnly)
{
    public static HubProjectionOverride ForAggregate(string targetColumn, string? aggregateFunctionName) =>
        new(targetColumn, aggregateFunctionName, AggregateOnly: false);

    public static HubProjectionOverride ForAggregateOnly(string targetColumn, string? aggregateFunctionName) =>
        new(targetColumn, aggregateFunctionName, AggregateOnly: true);
}
