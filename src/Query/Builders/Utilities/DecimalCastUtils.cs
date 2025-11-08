using Ksql.Linq.Core.Abstractions;
using System;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Ksql.Linq.Query.Builders.Utilities;

internal static class DecimalCastUtils
{
    /// <summary>
    /// Inject CAST(... AS DECIMAL(p,s)) for decimal aliases in SELECT list of a CTAS/CSAS SQL.
    /// - Prefers reflection over the resolved EntityModel type to read per-property precision/scale.
    /// - Falls back to AdditionalSettings projection names/types when reflection info is not available.
    /// - Skips when the expression already starts with CAST( ... ).
    /// </summary>
    public static string InjectDecimalCasts(string sql, EntityModel model)
    {
        try
        {
            var entityType = model?.EntityType;
            var props = entityType?.GetProperties(BindingFlags.Public | BindingFlags.Instance) ?? Array.Empty<PropertyInfo>();
            if (props.Length > 0)
            {
                foreach (var p in props)
                {
                    var underlying = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;
                    if (underlying != typeof(decimal)) continue;
                    var meta = Ksql.Linq.Core.Models.PropertyMeta.FromProperty(p);
                    var prec = Ksql.Linq.Configuration.DecimalPrecisionConfig.ResolvePrecision(meta.Precision, p);
                    var scale = Ksql.Linq.Configuration.DecimalPrecisionConfig.ResolveScale(meta.Scale, p);
                    var alias = p.Name;
                    sql = ReplaceInSelect(sql, alias, (expr) => $"CAST({expr} AS DECIMAL({prec}, {scale})) AS {alias}");
                }
            }
            else
            {
                // Fallback: use projection/type metadata when reflection is unavailable
                var names = model?.AdditionalSettings.TryGetValue("projection", out var pObj) == true && pObj is string[] vs ? vs : Array.Empty<string>();
                var types = model?.AdditionalSettings.TryGetValue("projection/types", out var tObj) == true && tObj is Type[] ts ? ts : Array.Empty<Type>();
                for (int i = 0; i < names.Length && i < types.Length; i++)
                {
                    var t = Nullable.GetUnderlyingType(types[i]) ?? types[i];
                    if (t != typeof(decimal)) continue;
                    var alias = names[i];
                    sql = ReplaceInSelect(sql, alias, (expr) => $"CAST({expr} AS DECIMAL(18, 4)) AS {alias}");
                }
            }
        }
        catch { }
        return sql;
    }

    private static string RegexEscape(string s) => Regex.Escape(s ?? string.Empty);

    private static string ReplaceInSelect(string sql, string alias, Func<string, string> replacer)
    {
        var m = Regex.Match(sql, @"(?is)\bSELECT\s+(?<sel>.+?)\s+FROM\s", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success) return sql;
        var sel = m.Groups["sel"].Value;
        var updated = RewriteSelectColumns(sel, alias, replacer);
        if (ReferenceEquals(updated, sel)) return sql;
        var start = m.Groups["sel"].Index;
        var length = m.Groups["sel"].Length;
        return sql.Substring(0, start) + updated + sql.Substring(start + length);
    }

    private static string RewriteSelectColumns(string sel, string targetAlias, Func<string, string> replacer)
    {
        var parts = SplitTopLevel(sel);
        var changed = false;
        for (int i = 0; i < parts.Count; i++)
        {
            var col = parts[i].Trim();
            var match = Regex.Match(col, $@"(?i)^(?<expr>.+?)\s+AS\s+{RegexEscape(targetAlias)}(?![\w])");
            if (match.Success)
            {
                var expr = match.Groups["expr"].Value.Trim();
                if (!expr.StartsWith("CAST(", StringComparison.OrdinalIgnoreCase))
                {
                    parts[i] = replacer(expr);
                    changed = true;
                }
            }
        }
        return changed ? string.Join(", ", parts) : sel;
    }

    private static System.Collections.Generic.List<string> SplitTopLevel(string list)
    {
        var items = new System.Collections.Generic.List<string>();
        int depth = 0; int start = 0;
        for (int i = 0; i < list.Length; i++)
        {
            var ch = list[i];
            if (ch == '(') depth++;
            else if (ch == ')') depth = System.Math.Max(0, depth - 1);
            else if (ch == ',' && depth == 0)
            {
                items.Add(list.Substring(start, i - start));
                start = i + 1;
            }
        }
        if (start < list.Length) items.Add(list[start..]);
        return items;
    }
}