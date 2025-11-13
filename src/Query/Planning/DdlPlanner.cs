using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Builders.Utilities;
using Ksql.Linq.Query.Dsl;
using System;
using System.Linq;

namespace Ksql.Linq.Query.Planning;

/// <summary>
/// Central orchestration for DDL construction policies (CTAS/CSAS).
/// - Injects GRACE for tumbling windows when requested.
/// - Surfaces sink sizing/retention from inputs into WITH clause via model.Extras.
/// - Enables hub-input overrides to lift non-aggregate columns using LATEST_BY_OFFSET.
/// Pure utility – safe for L2 unit testing without environment dependencies.
/// </summary>
internal static class DdlPlanner
{
    public static string BuildWindowedCtas(
        string name,
        KsqlQueryModel model,
        string timeframe,
        int? graceSeconds = null,
        string? inputOverride = null,
        int? partitions = null,
        int? replicas = null,
        long? retentionMs = null,
        string? emitOverride = "EMIT CHANGES")
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name required", nameof(name));
        if (model is null) throw new ArgumentNullException(nameof(model));
        if (string.IsNullOrWhiteSpace(timeframe)) throw new ArgumentException("timeframe required", nameof(timeframe));

        // Surface sink sizing/retention (overrides if provided)
        if (partitions.HasValue && partitions.Value > 0) model.Extras["sink/partitions"] = partitions.Value;
        if (replicas.HasValue && replicas.Value > 0) model.Extras["sink/replicas"] = replicas.Value;
        if (retentionMs.HasValue && retentionMs.Value > 0) model.Extras["sink/retentionMs"] = retentionMs.Value;

        // Ensure timeframe is captured on the model so downstream helpers (e.g., WITH retention gating) detect tumbling.
        if (!model.Windows.Contains(timeframe, StringComparer.OrdinalIgnoreCase))
        {
            model.Windows.Add(timeframe);
        }

        // Build base statement via windowed builder (handles hub overrides when inputOverride ends with _1s_rows)
        var sql = KsqlCreateWindowedStatementBuilder.Build(name, model, timeframe, emitOverride, inputOverride, options: null);

        // Inject GRACE if requested and not already present
        var g = graceSeconds ?? model.GraceSeconds;
        if (g.HasValue && g.Value > 0 && sql.IndexOf("GRACE", StringComparison.OrdinalIgnoreCase) < 0)
        {
            sql = System.Text.RegularExpressions.Regex.Replace(
                sql,
                @"(WINDOW\s+TUMBLING\s*\(\s*SIZE\s+[^\)]+)\)",
                $"$1, GRACE PERIOD {g.Value} SECONDS)",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }
        return sql;
    }

    public static string BuildRowsLastCtas(
        string targetName,
        string sourceRowsName,
        string[] keyColumns,
        string[] valueColumns,
        int? partitions = null,
        int? replicas = null,
        long? retentionMs = null)
    {
        if (string.IsNullOrWhiteSpace(targetName)) throw new ArgumentException("targetName required", nameof(targetName));
        if (string.IsNullOrWhiteSpace(sourceRowsName)) throw new ArgumentException("sourceRowsName required", nameof(sourceRowsName));
        keyColumns ??= Array.Empty<string>();
        valueColumns ??= Array.Empty<string>();
        if (keyColumns.Length == 0) throw new ArgumentException("keyColumns required", nameof(keyColumns));
        if (valueColumns.Length == 0) throw new ArgumentException("valueColumns required", nameof(valueColumns));

        var withParts = new System.Collections.Generic.List<string>
        {
            $"KAFKA_TOPIC='{targetName}'",
            "KEY_FORMAT='AVRO'",
            "VALUE_FORMAT='AVRO'"
        };
        if (partitions.HasValue && partitions.Value > 0) withParts.Add($"PARTITIONS={partitions.Value}");
        if (replicas.HasValue && replicas.Value > 0) withParts.Add($"REPLICAS={replicas.Value}");
        var retentionCandidate = retentionMs.HasValue && retentionMs.Value > 0 ? retentionMs : null;
        WithClauseUtils.AddRetentionIfSupported(
            withParts,
            retentionCandidate,
            allowRetentionMs: true,
            model: null,
            retentionPolicy: WithClauseRetentionPolicy.Auto,
            objectType: StreamTableType.Table,
            isWindowed: false);

        // SELECT: keys passthrough + LATEST_BY_OFFSET for values
        static string Up(string s) => string.IsNullOrWhiteSpace(s) ? s : s.Trim().ToUpperInvariant();
        var selectCols = new System.Collections.Generic.List<string>();
        foreach (var k in keyColumns) selectCols.Add(Up(k));
        foreach (var v in valueColumns)
        {
            var u = Up(v);
            selectCols.Add($"LATEST_BY_OFFSET({u}) AS {u}");
        }
        var selectClause = string.Join(", ", selectCols);
        var groupByClause = string.Join(", ", System.Linq.Enumerable.Select(keyColumns, c => Up(c)));

        var rowsIdent = Up(sourceRowsName);
        var targetIdent = Up(targetName);
        var sql = $"CREATE TABLE IF NOT EXISTS {targetIdent} WITH ({string.Join(", ", withParts)}) AS\nSELECT {selectClause}\nFROM {rowsIdent}\nGROUP BY {groupByClause}\nEMIT CHANGES;";
        return sql;
    }
}



