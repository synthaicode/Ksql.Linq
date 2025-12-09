using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Hub.Analysis;
using Ksql.Linq.Query.Builders.Visitors;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Ksql.Linq.Query.Builders.Statements;

/// <summary>
/// Builds CREATE STREAM/TABLE AS statements that include WINDOW TUMBLING clause
/// by adapting output from KsqlCreateStatementBuilder and injecting window spec.
/// </summary>
internal static class KsqlCreateWindowedStatementBuilder
{
    public static string Build(string name, KsqlQueryModel model, string timeframe, string? emitOverride = null, string? inputOverride = null, RenderOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name required", nameof(name));
        if (model is null) throw new ArgumentNullException(nameof(model));
        if (string.IsNullOrWhiteSpace(timeframe)) throw new ArgumentException("timeframe required", nameof(timeframe));
        // If hub rows is specified as input and metadata not yet attached, compute overrides/excludes here
        var isHubInput = !string.IsNullOrWhiteSpace(inputOverride) && inputOverride.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase);
        if (isHubInput && model.SelectProjection != null && model.SelectProjectionMetadata == null)
        {
            // Adapt C# projection for hub rows and build metadata-driven overrides
            model.SelectProjection = HubRowsProjectionAdapter.Adapt(model.SelectProjection);
            var meta = Ksql.Linq.Query.Analysis.ProjectionMetadataAnalyzer.Build(model, isHubInput: true);
            model.SelectProjectionMetadata = meta;
            HubSelectPolicy.BuildOverridesAndExcludes(meta, out var ov, out var ex);
            model.Extras["select/overrides"] = ov;
            model.Extras["select/exclude"] = ex;
        }
        // Ensure sink sizing (PARTITIONS/REPLICAS) is surfaced even if the caller forgot to attach Extras
        if (!model.Extras.ContainsKey("sink/partitions"))
            model.Extras["sink/partitions"] = 1;
        if (!model.Extras.ContainsKey("sink/replicas"))
            model.Extras["sink/replicas"] = 1;

        var baseSql = KsqlCreateStatementBuilder.Build(name, model, options: options);
        if (!string.IsNullOrWhiteSpace(emitOverride))
            baseSql = baseSql.Replace("EMIT CHANGES", emitOverride);
        if (!string.IsNullOrWhiteSpace(inputOverride))
        {
            baseSql = OverrideFrom(baseSql, inputOverride);
        }
        var window = FormatWindow(timeframe);
        // Optional GRACE insertion using simple heuristic: if model has AdditionalSettings[graceSeconds] on adapted entity, caller should pre-embed.
        var sql = InjectWindowAfterFrom(baseSql, window);
        // 注入オフ: WINDOWSTART 列は値側に追加しない（Window開始時刻は windowed key から復元する）
        return sql;
    }

    public static string BuildHopping(
        string name,
        KsqlQueryModel model,
        string? keySchemaFullName = null,
        string? valueSchemaFullName = null,
        Func<Type, string>? sourceResolver = null,
        string? emitOverride = null,
        string? inputOverride = null,
        RenderOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name required", nameof(name));
        if (model is null) throw new ArgumentNullException(nameof(model));
        if (model.Hopping is null) throw new ArgumentException("Hopping window not specified on model", nameof(model));

        var hopping = model.Hopping!;
        if (!model.HasGroupBy())
            throw new InvalidOperationException("Hopping window requires GroupBy().");
        // Ensure sink sizing defaults
        if (!model.Extras.ContainsKey("sink/partitions"))
            model.Extras["sink/partitions"] = 1;
        if (!model.Extras.ContainsKey("sink/replicas"))
            model.Extras["sink/replicas"] = 1;

        var resolver = sourceResolver ?? (t => t.Name.ToUpperInvariant());
        var baseSql = KsqlCreateStatementBuilder.Build(
            name,
            model,
            keySchemaFullName,
            valueSchemaFullName,
            resolver,
            options: options);
        if (!string.IsNullOrWhiteSpace(emitOverride))
            baseSql = baseSql.Replace("EMIT CHANGES", emitOverride);
        if (!string.IsNullOrWhiteSpace(inputOverride))
            baseSql = OverrideFrom(baseSql, inputOverride);

        var windowClause = FormatHoppingWindow(hopping);
        var sql = InjectWindowAfterFrom(baseSql, windowClause);
        var wsVisitor = new WindowStartDetectionVisitor();
        if (model.SelectProjection != null)
            wsVisitor.Visit(model.SelectProjection.Body);

        if (wsVisitor.Count == 0)
        {
            sql = RemoveWindowStartProjection(sql);
        }

        sql = EnsureWindowEndProjection(sql);
        // Always strip WINDOWSTART projection to keep value schema lean; window bounds are in the windowed key.
        sql = Regex.Replace(sql, @"WINDOWSTART\s+AS\s+[A-Za-z_][\w]*\s*,?", string.Empty, RegexOptions.IgnoreCase);
        return sql;
    }

    public static Dictionary<string, string> BuildAll(string namePrefix, KsqlQueryModel model, Func<string, string> nameFormatter)
    {
        if (model is null) throw new ArgumentNullException(nameof(model));
        if (nameFormatter is null) throw new ArgumentNullException(nameof(nameFormatter));
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var tf in model.Windows)
        {
            var name = nameFormatter(tf);
            result[tf] = Build(name, model, tf);
        }
        return result;
    }

    private static string FormatWindow(string timeframe)
    {
        // timeframe like: 1m, 5m, 1h, 1d, 7d, 1wk, 1mo
        if (timeframe.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(timeframe[..^2], out var w))
                return $"WINDOW TUMBLING (SIZE {w * 7} DAYS)";
        }
        if (timeframe.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(timeframe[..^2], out var mo))
                return $"WINDOW TUMBLING (SIZE {mo} MONTHS)"; // KSQL supports MONTHS in recent versions
        }
        var unit = timeframe[^1];
        if (!int.TryParse(timeframe[..^1], out var val)) val = 1;
        return unit switch
        {
            's' => $"WINDOW TUMBLING (SIZE {val} SECONDS)",
            'm' => $"WINDOW TUMBLING (SIZE {val} MINUTES)",
            'h' => $"WINDOW TUMBLING (SIZE {val} HOURS)",
            'd' => $"WINDOW TUMBLING (SIZE {val} DAYS)",
            _ => $"WINDOW TUMBLING (SIZE {val} MINUTES)"
        };
    }

    private static string FormatHoppingWindow(HoppingWindowSpec hopping)
    {
        string FormatTs(TimeSpan ts)
        {
            if (ts.TotalDays >= 1 && ts.TotalHours % 24 == 0)
                return $"SIZE {(int)ts.TotalDays} DAYS";
            if (ts.TotalHours >= 1 && ts.TotalMinutes % 60 == 0)
                return $"SIZE {(int)ts.TotalHours} HOURS";
            if (ts.TotalMinutes >= 1 && ts.TotalSeconds % 60 == 0)
                return $"SIZE {(int)ts.TotalMinutes} MINUTES";
            return $"SIZE {(int)ts.TotalSeconds} SECONDS";
        }
        string FormatAdvance(TimeSpan ts)
        {
            if (ts.TotalDays >= 1 && ts.TotalHours % 24 == 0)
                return $"ADVANCE BY {(int)ts.TotalDays} DAYS";
            if (ts.TotalHours >= 1 && ts.TotalMinutes % 60 == 0)
                return $"ADVANCE BY {(int)ts.TotalHours} HOURS";
            if (ts.TotalMinutes >= 1 && ts.TotalSeconds % 60 == 0)
                return $"ADVANCE BY {(int)ts.TotalMinutes} MINUTES";
            return $"ADVANCE BY {(int)ts.TotalSeconds} SECONDS";
        }

        var sb = new StringBuilder();
        sb.Append("WINDOW HOPPING ( ");
        sb.Append(FormatTs(hopping.Size));
        sb.Append(" , ");
        sb.Append(FormatAdvance(hopping.Advance));
        if (hopping.Grace.HasValue)
        {
            var g = hopping.Grace.Value;
            if (g.TotalDays >= 1 && g.TotalHours % 24 == 0)
                sb.Append($" , GRACE PERIOD {(int)g.TotalDays} DAYS");
            else if (g.TotalHours >= 1 && g.TotalMinutes % 60 == 0)
                sb.Append($" , GRACE PERIOD {(int)g.TotalHours} HOURS");
            else if (g.TotalMinutes >= 1 && g.TotalSeconds % 60 == 0)
                sb.Append($" , GRACE PERIOD {(int)g.TotalMinutes} MINUTES");
            else
                sb.Append($" , GRACE PERIOD {(int)g.TotalSeconds} SECONDS");
        }
        sb.Append(" )");
        return sb.ToString();
    }

    private static string OverrideFrom(string sql, string source)
    {
        var pattern = new Regex(@"\bFROM\s+([A-Za-z_][\w]*)\s+([A-Za-z_][\w]*)", RegexOptions.IgnoreCase);
        return pattern.Replace(sql, m => $"FROM {source} {m.Groups[2].Value}", 1);
    }

    private static string InjectWindowAfterFrom(string sql, string windowClause)
    {
        // Replace first occurrence of "FROM <ident> [alias]" with "FROM <ident> [alias] {window}"
        var pattern = new Regex(@"\bFROM\s+([A-Za-z_][\w]*)(\s+[A-Za-z_][\w]*)?(?=\s+(JOIN|WINDOW|GROUP|EMIT|WHERE|ON|WITHIN)\b|;)", RegexOptions.IgnoreCase);
        return pattern.Replace(sql, m =>
        {
            var alias = m.Groups[2].Value;
            return $"FROM {m.Groups[1].Value}{alias} {windowClause}";
        }, 1);
    }

    private static string EnsureWindowEndProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return sql;
        // Replace "WINDOWSTART\s+(?:AS\s+)?ENDTS" (case-insensitive) with WINDOWEND to surface window end timestamp
        return Regex.Replace(sql, @"WINDOWSTART\s+(?:AS\s+)?ENDTS", "WINDOWEND AS EndTs", RegexOptions.IgnoreCase);
    }

    private static string RemoveWindowStartProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return sql;

        return Regex.Replace(sql, @"WINDOWSTART\s+(?:AS\s+[A-Za-z_][\w]*)\s*,?", string.Empty, RegexOptions.IgnoreCase);
    }

    private static string EnsureWindowStartColumn(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return sql;

        var selectMatch = Regex.Match(sql, @"SELECT\s+", RegexOptions.IgnoreCase);
        if (!selectMatch.Success)
            return sql;

        var selectIndex = selectMatch.Index + selectMatch.Length;
        var afterSelect = sql.Substring(selectIndex);
        var fromMatch = Regex.Match(afterSelect, @"\bFROM\b", RegexOptions.IgnoreCase);
        if (!fromMatch.Success)
            return sql;

        var columnsSegment = afterSelect.Substring(0, fromMatch.Index);
        if (Regex.IsMatch(columnsSegment, @"\bAS\s+""?(WINDOWSTARTRAW|WINDOWSTART)""?\b", RegexOptions.IgnoreCase))
            return sql;

        var trimmed = columnsSegment.Trim();
        string prefixWhitespace;
        string suffixWhitespace;

        if (trimmed.Length == 0)
        {
            prefixWhitespace = columnsSegment;
            suffixWhitespace = string.Empty;
        }
        else
        {
            var firstIndex = columnsSegment.IndexOf(trimmed, StringComparison.Ordinal);
            prefixWhitespace = firstIndex >= 0 ? columnsSegment[..firstIndex] : string.Empty;
            var lastIndex = columnsSegment.LastIndexOf(trimmed, StringComparison.Ordinal);
            suffixWhitespace = lastIndex >= 0
                ? columnsSegment[(lastIndex + trimmed.Length)..]
                : string.Empty;
        }

        var builder = new StringBuilder();
        builder.Append(prefixWhitespace);
        builder.Append("WINDOWSTART AS WindowStartRaw");
        if (trimmed.Length > 0)
        {
            // Preserve multi-line formatting where possible
            builder.Append(trimmed.Contains('\n') ? ",\n" : ", ");
            builder.Append(trimmed);
            builder.Append(suffixWhitespace);
        }

        var newColumnsSegment = builder.ToString();
        var newSql = sql.Remove(selectIndex, fromMatch.Index).Insert(selectIndex, newColumnsSegment);
        return newSql;
    }

}
