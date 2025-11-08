using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Ksql.Linq.Query.Dsl;

namespace Ksql.Linq.Query.Hub.Adapters;

/// <summary>
/// Adapts SelectProjection for hub rows (*_1s_rows) so that CTAS contains only aggregate functions
/// and does not embed C#-side calculations. It preserves non-target bindings and replaces
/// BucketStart/Year/OHLC with WINDOWSTART and offset-aggregates against hub columns.
/// </summary>
internal static class HubRowsProjectionAdapter
{
    public static LambdaExpression Adapt(LambdaExpression original)
    {
        if (original == null) throw new ArgumentNullException(nameof(original));

        // Expecting parameter g of type IGrouping<TKey, TSource>
        var g = original.Parameters.FirstOrDefault()
                ?? throw new InvalidOperationException("SelectProjection must have a parameter");
        var groupingType = g.Type;
        if (!groupingType.IsGenericType || groupingType.GetGenericTypeDefinition() != typeof(IGrouping<,>))
            return original; // non-grouping projections are out-of-scope; keep as-is

        var tKey = groupingType.GetGenericArguments()[0];
        var tSrc = groupingType.GetGenericArguments()[1];
        var body = original.Body;

        if (body is not MemberInitExpression init)
        {
            // Cannot safely adapt anonymous/simple projections; keep as-is
            return original;
        }

        // Helper builders (only WindowStart is needed after removing special-cases)
        var x = Expression.Parameter(tSrc, "x");
        Expression WindowStart()
        {
            var extType = typeof(Ksql.Linq.WindowExtensions);
            var method = extType.GetMethod("WindowStart", BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(tSrc, tKey);
            return Expression.Call(null, method, g);
        }

        // Build replacement bindings for known members if they exist on the DTO
        var bindings = init.Bindings.OfType<MemberAssignment>().ToList();
        var targetType = init.Type;
        MemberAssignment? ReplaceIf(string name, Func<MemberInfo, Expression> exprFactory)
        {
            var mem = targetType.GetMember(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)
                .FirstOrDefault();
            if (mem == null) return null;
            var expr = exprFactory(mem);
            return Expression.Bind(mem, expr);
        }

        // No aggregate-specific helpers; aggregation mapping is handled by policy/visitor.

        // Create new bindings list, replacing target members when present
        var newBindings = new System.Collections.Generic.List<MemberBinding>(bindings.Count);
        foreach (var b in bindings)
        {
            var name = b.Member.Name;
            if (name.Equals("BucketStart", StringComparison.OrdinalIgnoreCase))
            {
                var repl = ReplaceIf(name, _ => WindowStart());
                newBindings.Add(repl ?? b);
            }
            // Removed special-case handling for Year: keep adapter minimal and let policy/visitor handle overrides.
            // Remove OHLC special-casing here. Mapping to hub columns is handled
            // by HubSelectPolicy + SelectExpressionVisitor via overrides/excludes.
            else
            {
                newBindings.Add(b); // keep original for other members・・roker/Symbol 遲会ｼ・
            }
        }

        var replaced = Expression.MemberInit(init.NewExpression, newBindings);
        return Expression.Lambda(original.Type, replaced, g);
    }
}