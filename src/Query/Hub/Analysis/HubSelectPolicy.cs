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

            // Special-case: WindowStartRaw is a logical column that is restored from windowed keys/BucketStart
            // (TimeBucket/TableCache). Do not push C# computations into KSQL.
            if (string.Equals(m.Alias, "WindowStartRaw", StringComparison.OrdinalIgnoreCase))
            {
                excludeAliases.Add(m.Alias);
                continue;
            }

            if (m.Kind == ProjectionMemberKind.Aggregate)
            {
                // Special-case: Average in hub flows should be derived from hub SUM(<leaf>) + CNT
                // because some ksqlDB versions are strict about AVG usage (and nested usage in particular).
                // Keep other aggregate mappings conservative unless availableColumns-based inference is present.
                var isAvg =
                    string.Equals(m.AggregateFunctionName, "Average", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(m.AggregateFunctionName, "Avg", StringComparison.OrdinalIgnoreCase);

                var inferred = isAvg ? TryInferHubTargetColumn(m.Expression, availableColumns) : null;
                var target = !string.IsNullOrWhiteSpace(m.Alias)
                    ? KsqlNameUtils.Sanitize(m.Alias).ToUpperInvariant()
                    : ResolveTargetColumn(m);
                if (!string.IsNullOrWhiteSpace(inferred))
                    target = inferred;
                if (!string.IsNullOrWhiteSpace(target))
                {
                    overrides[m.Alias] = HubProjectionOverride.ForAggregate(target!, m.AggregateFunctionName);
                    try { Console.WriteLine($"[hub.override] alias={m.Alias} target={target} func={m.AggregateFunctionName} kind={m.Kind}"); } catch { }
                }
                continue;
            }

            // Computed でも内部に集計を含む場合はハブ列を使った集計に置き換える
            if (m.Kind == ProjectionMemberKind.Computed && ContainsAggregate(m.Expression))
            {
                var inferred = TryInferHubTargetColumn(m.Expression, availableColumns);
                var target = !string.IsNullOrWhiteSpace(inferred)
                    ? inferred
                    : (!string.IsNullOrWhiteSpace(m.ResolvedColumnName)
                        ? m.ResolvedColumnName
                        : (!string.IsNullOrWhiteSpace(m.Alias) ? KsqlNameUtils.Sanitize(m.Alias).ToUpperInvariant() : null));
                if (!string.IsNullOrWhiteSpace(target))
                {
                    var agg = string.IsNullOrWhiteSpace(m.AggregateFunctionName) ? "AVG" : m.AggregateFunctionName;
                    overrides[m.Alias] = HubProjectionOverride.ForAggregate(target!, agg);
                    try { Console.WriteLine($"[hub.override.computed] alias={m.Alias} target={target} func={agg}"); } catch { }
                }
            }

            // Policy: Computed members without aggregates are handled in C# (excluded from CTAS for hub flows).
            if (m.Kind == ProjectionMemberKind.Computed && !ContainsAggregate(m.Expression))
            {
                excludeAliases.Add(m.Alias);
            }
        }
    }

    private static string? TryInferHubTargetColumn(System.Linq.Expressions.Expression expression, System.Collections.Generic.ISet<string>? availableColumns)
    {
        if (!TryExtractFirstAggregate(expression, out var functionName, out var sourceLeaf))
            return null;

        static string Up(string s) => string.IsNullOrWhiteSpace(s) ? string.Empty : KsqlNameUtils.Sanitize(s).ToUpperInvariant();

        var leaf = Up(sourceLeaf);
        var fn = string.IsNullOrWhiteSpace(functionName) ? string.Empty : functionName;

        // Candidate hub columns (prefer canonical aggregates over computed aliases)
        var candidates = new System.Collections.Generic.List<string>();
        if (!string.IsNullOrWhiteSpace(leaf))
        {
            if (string.Equals(fn, "Average", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(fn, "Avg", StringComparison.OrdinalIgnoreCase))
            {
                // Average is represented as SUM(<leaf>) / SUM(CNT) when available.
                candidates.Add($"SUM{leaf}");
            }
            else if (string.Equals(fn, "Sum", StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add($"SUM{leaf}");
            }
            else if (string.Equals(fn, "Max", StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add($"MAX{leaf}");
            }
            else if (string.Equals(fn, "Min", StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add($"MIN{leaf}");
            }
            else if (string.Equals(fn, "LatestByOffset", StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add($"LAST{leaf}");
            }
            else if (string.Equals(fn, "EarliestByOffset", StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add($"FIRST{leaf}");
            }
        }

        // For COUNT(*) / COUNT(x), the canonical column name is CNT.
        if (string.Equals(fn, "Count", StringComparison.OrdinalIgnoreCase))
            candidates.Add("CNT");

        if (availableColumns == null || availableColumns.Count == 0)
            return candidates.Count > 0 ? candidates[0] : null;

        static bool Has(System.Collections.Generic.ISet<string> cols, string name)
        {
            foreach (var c in cols)
                if (string.Equals(c, name, StringComparison.OrdinalIgnoreCase))
                    return true;
            return false;
        }

        // For Average, require both SUM<leaf> and CNT to avoid generating SUM(CNT) on missing column.
        if (candidates.Count > 0 && candidates[0].StartsWith("SUM", StringComparison.OrdinalIgnoreCase))
        {
            var sumCol = candidates[0];
            if (Has(availableColumns, sumCol) && Has(availableColumns, "CNT"))
                return sumCol;
        }

        foreach (var c in candidates)
            if (Has(availableColumns, c))
                return c;

        return null;
    }

    private static bool TryExtractFirstAggregate(
        System.Linq.Expressions.Expression expression,
        out string functionName,
        out string sourceLeaf)
    {
        var found = false;
        var fn = string.Empty;
        var leaf = string.Empty;

        var visitor = new AggregateExtractVisitor(mc =>
        {
            if (found) return;
            found = true;
            fn = mc.Method.Name;
            leaf = TryExtractLeafFromAggregate(mc) ?? string.Empty;
        });

        visitor.Visit(expression);
        functionName = fn;
        sourceLeaf = leaf;
        return found;
    }

    private sealed class AggregateExtractVisitor : System.Linq.Expressions.ExpressionVisitor
    {
        private readonly Action<System.Linq.Expressions.MethodCallExpression> _onFound;

        public AggregateExtractVisitor(Action<System.Linq.Expressions.MethodCallExpression> onFound)
        {
            _onFound = onFound;
        }

        public override System.Linq.Expressions.Expression? Visit(System.Linq.Expressions.Expression? node)
        {
            if (node is System.Linq.Expressions.MethodCallExpression mc)
            {
                var name = mc.Method.Name;
                if (!string.IsNullOrWhiteSpace(name) && KsqlFunctionRegistry.IsAggregateFunction(name))
                {
                    _onFound(mc);
                    return node;
                }
            }
            return base.Visit(node);
        }
    }

    private static string? TryExtractLeafFromAggregate(System.Linq.Expressions.MethodCallExpression mc)
    {
        foreach (var arg in mc.Arguments)
        {
            var lambda = arg switch
            {
                System.Linq.Expressions.LambdaExpression le => le,
                System.Linq.Expressions.UnaryExpression { NodeType: System.Linq.Expressions.ExpressionType.Quote, Operand: System.Linq.Expressions.LambdaExpression le } => le,
                System.Linq.Expressions.ConstantExpression { Value: System.Linq.Expressions.LambdaExpression le } => le,
                _ => null
            };
            if (lambda == null)
                continue;

            var body = ExpressionUtils.UnwrapConvert(lambda.Body);
            if (body is System.Linq.Expressions.MemberExpression me)
                return me.Member.Name;
        }
        return null;
    }

    private static bool ShouldPromoteComputedAlias(ProjectionMember member, System.Collections.Generic.ISet<string>? availableColumns)
    {
        // 現状は昇格しない（将来拡張用）
        return false;
    }

    private static string? ResolveTargetColumn(ProjectionMember member)
    {
        if (!string.IsNullOrWhiteSpace(member.Alias))
            return KsqlNameUtils.Sanitize(member.Alias).ToUpperInvariant();
        if (!string.IsNullOrWhiteSpace(member.ResolvedColumnName))
            return member.ResolvedColumnName;
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
