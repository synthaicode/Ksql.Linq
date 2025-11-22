using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Hub.Analysis;
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
    public static string Build(
        string name,
        KsqlQueryModel model,
        string timeframe,
        string? emitOverride = null,
        string? inputOverride = null,
        RenderOptions? options = null,
        TimeSpan? hopInterval = null,
        string? keySchemaFullName = null,
        string? valueSchemaFullName = null)
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

        var baseSql = KsqlCreateStatementBuilder.Build(
            name,
            model,
            keySchemaFullName: keySchemaFullName,
            valueSchemaFullName: valueSchemaFullName,
            options: options);
        if (!string.IsNullOrWhiteSpace(emitOverride))
            baseSql = baseSql.Replace("EMIT CHANGES", emitOverride);
        if (!string.IsNullOrWhiteSpace(inputOverride))
        {
            baseSql = OverrideFrom(baseSql, inputOverride);
        }

        // Build window clause with optional GRACE PERIOD
        var graceSeconds = model.GraceSeconds.HasValue && model.GraceSeconds.Value > 0
            ? (int?)model.GraceSeconds.Value
            : null;

        var window = hopInterval.HasValue
            ? FormatHoppingWindow(timeframe, hopInterval.Value, graceSeconds)
            : FormatWindow(timeframe, graceSeconds);

        var sql = InjectWindowAfterFrom(baseSql, window);
        // 注入オフ: WINDOWSTART 列は値側に追加しない（Window開始時刻は windowed key から復元する）
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

    private static string FormatWindow(string timeframe, int? graceSeconds = null)
    {
        // timeframe like: 1m, 5m, 1h, 1d, 7d, 1wk, 1mo
        var gracePart = graceSeconds.HasValue ? $", GRACE PERIOD {graceSeconds.Value} SECONDS" : "";

        if (timeframe.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(timeframe[..^2], out var w))
                return $"WINDOW TUMBLING (SIZE {w * 7} DAYS{gracePart})";
        }
        if (timeframe.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(timeframe[..^2], out var mo))
                return $"WINDOW TUMBLING (SIZE {mo} MONTHS{gracePart})"; // KSQL supports MONTHS in recent versions
        }
        var unit = timeframe[^1];
        if (!int.TryParse(timeframe[..^1], out var val)) val = 1;
        return unit switch
        {
            's' => $"WINDOW TUMBLING (SIZE {val} SECONDS{gracePart})",
            'm' => $"WINDOW TUMBLING (SIZE {val} MINUTES{gracePart})",
            'h' => $"WINDOW TUMBLING (SIZE {val} HOURS{gracePart})",
            'd' => $"WINDOW TUMBLING (SIZE {val} DAYS{gracePart})",
            _ => $"WINDOW TUMBLING (SIZE {val} MINUTES{gracePart})"
        };
    }

    private static string FormatHoppingWindow(string timeframe, TimeSpan hopInterval, int? graceSeconds = null)
    {
        var (windowValue, windowUnit) = ParseTimeframe(timeframe);
        var (hopValue, hopUnit) = FormatTimeSpan(hopInterval);
        var gracePart = graceSeconds.HasValue ? $", GRACE PERIOD {graceSeconds.Value} SECONDS" : "";
        return $"WINDOW HOPPING (SIZE {windowValue} {windowUnit}, ADVANCE BY {hopValue} {hopUnit}{gracePart})";
    }

    private static (int Value, string Unit) ParseTimeframe(string timeframe)
    {
        // Handle special cases first
        if (timeframe.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(timeframe[..^2], out var w))
                return (w * 7, "DAYS");
        }
        if (timeframe.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(timeframe[..^2], out var mo))
                return (mo, "MONTHS");
        }

        var unit = timeframe[^1];
        if (!int.TryParse(timeframe[..^1], out var val)) val = 1;

        var unitName = unit switch
        {
            's' => "SECONDS",
            'm' => "MINUTES",
            'h' => "HOURS",
            'd' => "DAYS",
            _ => "MINUTES"
        };

        return (val, unitName);
    }

    private static (int Value, string Unit) FormatTimeSpan(TimeSpan ts)
    {
        if (ts.TotalSeconds < 60 && ts.TotalSeconds == (int)ts.TotalSeconds)
            return ((int)ts.TotalSeconds, "SECONDS");
        if (ts.TotalMinutes < 60 && ts.TotalMinutes == (int)ts.TotalMinutes)
            return ((int)ts.TotalMinutes, "MINUTES");
        if (ts.TotalHours < 24 && ts.TotalHours == (int)ts.TotalHours)
            return ((int)ts.TotalHours, "HOURS");
        return ((int)ts.TotalDays, "DAYS");
    }

    private static string OverrideFrom(string sql, string source)
    {
        var pattern = new Regex(@"\bFROM\s+([A-Za-z_][\w]*)\s+([A-Za-z_][\w]*)", RegexOptions.IgnoreCase);
        return pattern.Replace(sql, m => $"FROM {source} {m.Groups[2].Value}", 1);
    }

    private static string InjectWindowAfterFrom(string sql, string windowClause)
    {
        // Replace first occurrence of "FROM <ident> [alias]" with "FROM <ident> [alias] {window}"
        var pattern = new Regex(@"\bFROM\s+([A-Za-z_][\w]*)(\s+(?!GROUP\b|WINDOW\b|EMIT\b|WHERE\b|JOIN\b)[A-Za-z_][\w]*)?", RegexOptions.IgnoreCase);
        return pattern.Replace(sql, m =>
        {
            var alias = m.Groups[2].Value;
            return $"FROM {m.Groups[1].Value}{alias} {windowClause}";
        }, 1);
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
