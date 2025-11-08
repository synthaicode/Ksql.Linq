using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Ksql.Linq.Infrastructure.Ksql;

/// <summary>
/// Utilities for normalizing and matching ksqlDB SHOW QUERIES outputs
/// across JSON and text table formats. Extracted for focused unit testing.
/// </summary>
internal static class KsqlWaitService
{
    public static IWaitDiagnosticsSink DiagnosticsSink { get; set; } = new FileWaitDiagnosticsSink();
    public static string NormalizeIdentifier(string? value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim().Trim('"').ToUpperInvariant();

    public static string NormalizeSql(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql)) return string.Empty;
        var sb = new StringBuilder(sql.Length);
        var prevWs = false;
        foreach (var ch in sql!)
        {
            if (char.IsWhiteSpace(ch))
            {
                if (!prevWs) { sb.Append(' '); prevWs = true; }
            }
            else { sb.Append(char.ToUpperInvariant(ch)); prevWs = false; }
        }
        return sb.ToString().Trim();
    }

    public static void AppendWaitDiagRaw(string label, string body)
        => DiagnosticsSink.Append(label, body);

    public static string ResolveWaitRawLogPath()
    {
        var configured = System.Environment.GetEnvironmentVariable("KSQL_MATCH_RAW_LOG_PATH");
        if (!string.IsNullOrWhiteSpace(configured)) return configured;
        try
        {
            var baseDir = System.AppContext.BaseDirectory ?? System.IO.Directory.GetCurrentDirectory();
            return System.IO.Path.Combine(baseDir, "reports", "physical", "wait_raw.log");
        }
        catch
        {
            return System.IO.Path.Combine(System.IO.Directory.GetCurrentDirectory(), "reports", "physical", "wait_raw.log");
        }
    }

    public static bool ShowQueriesContainsQuery(string showQueriesOutput, string queryId, string targetTopic)
    {
        if (string.IsNullOrWhiteSpace(showQueriesOutput) || string.IsNullOrWhiteSpace(queryId))
            return false;

        var normalizedQueryId = queryId.Trim();
        var normalizedTarget = NormalizeIdentifier(targetTopic);

        try
        {
            using var doc = JsonDocument.Parse(showQueriesOutput);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in doc.RootElement.EnumerateArray())
                {
                    if (!entry.TryGetProperty("queries", out var queries) || queries.ValueKind != JsonValueKind.Array)
                        continue;
                    foreach (var query in queries.EnumerateArray())
                    {
                        if (!query.TryGetProperty("id", out var idEl) || idEl.ValueKind != JsonValueKind.String)
                            continue;
                        var id = idEl.GetString();
                        if (string.IsNullOrWhiteSpace(id))
                            continue;
                        if (id.Equals(normalizedQueryId, StringComparison.OrdinalIgnoreCase))
                            return true;
                        if (query.TryGetProperty("sinks", out var sinksEl) && sinksEl.ValueKind == JsonValueKind.Array && !string.IsNullOrWhiteSpace(normalizedTarget))
                        {
                            foreach (var sink in sinksEl.EnumerateArray())
                            {
                                if (sink.ValueKind != JsonValueKind.String)
                                    continue;
                                var sinkName = NormalizeIdentifier(sink.GetString());
                                if (!string.IsNullOrWhiteSpace(sinkName) && sinkName.Equals(normalizedTarget, StringComparison.OrdinalIgnoreCase))
                                    return true;
                            }
                        }
                    }
                }
            }
        }
        catch { /* fall back to text table parsing */ }

        var lines = showQueriesOutput.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var rawLine in lines)
        {
            var line = rawLine.Trim();
            if (string.IsNullOrEmpty(line) || line.StartsWith("+") || line.StartsWith("-"))
                continue;
            if (!line.Contains('|'))
                continue;

            if (line.IndexOf(normalizedQueryId, StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
            if (!string.IsNullOrWhiteSpace(normalizedTarget) && line.IndexOf(normalizedTarget, StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
        }

        return false;
    }

    public static bool ShowListingContainsEntity(string showOutput, string normalizedName)
    {
        if (string.IsNullOrWhiteSpace(showOutput) || string.IsNullOrWhiteSpace(normalizedName))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(showOutput);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in doc.RootElement.EnumerateArray())
                {
                    if (entry.TryGetProperty("tables", out var tables) && tables.ValueKind == JsonValueKind.Array)
                    {
                        if (ListingArrayContains(tables, normalizedName))
                            return true;
                    }
                    if (entry.TryGetProperty("streams", out var streams) && streams.ValueKind == JsonValueKind.Array)
                    {
                        if (ListingArrayContains(streams, normalizedName))
                            return true;
                    }
                }
            }
        }
        catch (JsonException)
        {
            // fall back to raw search
        }

        return showOutput.IndexOf(normalizedName, StringComparison.OrdinalIgnoreCase) >= 0;

        static bool ListingArrayContains(JsonElement array, string normalizedName)
        {
            foreach (var item in array.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object)
                    continue;
                if (item.TryGetProperty("name", out var nameEl))
                {
                    var nameValue = KsqlWaitService.NormalizeIdentifier(nameEl.GetString());
                    if (!string.IsNullOrEmpty(nameValue) && nameValue.Equals(normalizedName, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                if (item.TryGetProperty("topic", out var topicEl))
                {
                    var topicValue = KsqlWaitService.NormalizeIdentifier(topicEl.GetString());
                    if (!string.IsNullOrEmpty(topicValue) && topicValue.Equals(normalizedName, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
            }
            return false;
        }
    }

    public static int? ReadGraceSecondsFromDescribe(string describeOutput)
    {
        try
        {
            using var doc = JsonDocument.Parse(describeOutput);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                return null;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (!el.TryGetProperty("writeQueries", out var arr) || arr.ValueKind != JsonValueKind.Array)
                    continue;
                foreach (var wq in arr.EnumerateArray())
                {
                    if (!wq.TryGetProperty("queryString", out var qsEl) || qsEl.ValueKind != JsonValueKind.String)
                        continue;
                    var qs = qsEl.GetString() ?? string.Empty;
                    var idx = qs.IndexOf("GRACE PERIOD", StringComparison.OrdinalIgnoreCase);
                    if (idx >= 0)
                    {
                        var tail = qs.Substring(idx);
                        var m = System.Text.RegularExpressions.Regex.Match(tail, @"GRACE\s+PERIOD\s+(\d+)\s+SECONDS", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                        if (m.Success && int.TryParse(m.Groups[1].Value, out var gs))
                            return gs;
                    }
                }
            }
        }
        catch { }
        return null;
    }
    public static bool DescribeExtendedContainsQuery(string describeOutput, string queryId)
    {
        if (string.IsNullOrWhiteSpace(describeOutput) || string.IsNullOrWhiteSpace(queryId))
            return false;
        try
        {
            using var doc = JsonDocument.Parse(describeOutput);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in doc.RootElement.EnumerateArray())
                {
                    if (!element.TryGetProperty("writeQueries", out var writeQueries) || writeQueries.ValueKind != JsonValueKind.Array)
                        continue;
                    foreach (var writeQuery in writeQueries.EnumerateArray())
                    {
                        if (writeQuery.TryGetProperty("queryId", out var qidEl) && qidEl.ValueKind == JsonValueKind.String)
                        {
                            var qid = qidEl.GetString();
                            if (!string.IsNullOrWhiteSpace(qid) && qid.Equals(queryId, StringComparison.OrdinalIgnoreCase))
                                return true;
                        }
                    }
                }
            }
        }
        catch { /* fall back to text search */ }
        return describeOutput.IndexOf(queryId, StringComparison.OrdinalIgnoreCase) >= 0;
    }

    public static bool TryGetQueryStateFromJson(string showQueriesOutput, string normalizedTarget, string? normalizedQueryId, out string? state)
    {
        state = null;
        if (string.IsNullOrWhiteSpace(showQueriesOutput)) return false;
        try
        {
            using var doc = JsonDocument.Parse(showQueriesOutput);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return false;
            foreach (var entry in doc.RootElement.EnumerateArray())
            {
                if (!entry.TryGetProperty("queries", out var queries) || queries.ValueKind != JsonValueKind.Array) continue;
                foreach (var query in queries.EnumerateArray())
                {
                    if (!MatchesTargetQuery(query, normalizedTarget, normalizedQueryId)) continue;
                    if (query.TryGetProperty("state", out var stateProp))
                    {
                        state = stateProp.GetString();
                        return true;
                    }
                    if (query.TryGetProperty("statusCount", out var statusCount) && statusCount.ValueKind == JsonValueKind.Object)
                    {
                        if (statusCount.TryGetProperty("RUNNING", out var running) && running.ValueKind == JsonValueKind.Number)
                        {
                            try { if (running.GetInt32() > 0) { state = "RUNNING"; return true; } } catch { }
                        }
                    }
                }
            }
        }
        catch (JsonException) { /* fall back to text */ }
        return false;
    }

    public static bool CheckQueryRunningInText(IEnumerable<string> lines, string targetUpper, string? normalizedQueryId)
    {
        foreach (var line in lines)
        {
            if (!line.Contains('|')) continue;
            var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
            var upper = line.ToUpperInvariant();
            if (!string.IsNullOrEmpty(normalizedQueryId) && upper.Contains(normalizedQueryId) && upper.Contains("RUNNING")) return true;
            if (upper.Contains(targetUpper) && upper.Contains("RUNNING")) return true;
            if (parts.Length > 3 && parts[1].Trim().Equals(targetUpper, StringComparison.OrdinalIgnoreCase))
            {
                var state = parts[3].Trim().ToUpperInvariant();
                if (state.StartsWith("RUNNING", StringComparison.OrdinalIgnoreCase)) return true;
            }
        }
        return false;
    }

    public static string? FindQueryIdInShowQueries(string showQueriesOutput, string targetTopic, string? statement)
    {
        if (string.IsNullOrWhiteSpace(showQueriesOutput)) return null;
        showQueriesOutput = showQueriesOutput.Replace("\\n", "\n");
        var normalizedTarget = NormalizeIdentifier(targetTopic);
        var normalizedStmt = NormalizeSql(statement);
        if (TryFindQueryIdInJson(showQueriesOutput, normalizedTarget, normalizedStmt, out var jsonId)) return jsonId;
        var lines = showQueriesOutput.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var raw in lines)
        {
            var line = raw.Trim(); if (string.IsNullOrEmpty(line) || line.StartsWith('+') || line.StartsWith('-')) continue;
            if (!line.Contains('|')) continue;
            var columns = ParseColumns(line); if (columns.Count == 0) continue;
            var qid = columns[0]; if (string.IsNullOrEmpty(qid) || qid.Equals("Query ID", StringComparison.OrdinalIgnoreCase)) continue;
            var topicMatches = false;
            if (!string.IsNullOrEmpty(normalizedTarget))
            {
                if (columns.Count > 1)
                {
                    var topics = columns[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(t => NormalizeIdentifier(t.Replace("\"", string.Empty))).ToList();
                    topicMatches = topics.Any(t => !string.IsNullOrEmpty(t) && t.Equals(normalizedTarget, StringComparison.OrdinalIgnoreCase));
                }
                if (!topicMatches && line.ToUpperInvariant().Contains(normalizedTarget)) topicMatches = true;
            }
            var stmtMatches = false;
            if (!string.IsNullOrEmpty(normalizedStmt))
            {
                var normalizedColStmt = columns.Count > 2 ? NormalizeSql(columns[2]) : string.Empty;
                stmtMatches = line.ToUpperInvariant().Contains(normalizedStmt) || (!string.IsNullOrEmpty(normalizedColStmt) && normalizedColStmt.Contains(normalizedStmt, StringComparison.Ordinal));
            }
            if (topicMatches || stmtMatches) return qid;
        }
        return null;
    }

    private static bool MatchesTargetQuery(JsonElement query, string normalizedTarget, string? normalizedQueryId)
    {
        if (string.IsNullOrEmpty(normalizedTarget) && string.IsNullOrEmpty(normalizedQueryId)) return false;
        if (!string.IsNullOrEmpty(normalizedQueryId))
        {
            if (query.TryGetProperty("id", out var idProp) && idProp.ValueKind == JsonValueKind.String)
            {
                var idValue = NormalizeIdentifier(idProp.GetString() ?? string.Empty);
                if (!string.IsNullOrEmpty(idValue) && idValue.Equals(normalizedQueryId, StringComparison.OrdinalIgnoreCase)) return true;
            }
        }
        if (string.IsNullOrEmpty(normalizedTarget)) return false;
        if (query.TryGetProperty("sinkKafkaTopics", out var sinkTopics) && sinkTopics.ValueKind == JsonValueKind.Array)
        {
            foreach (var sink in sinkTopics.EnumerateArray())
            {
                var sinkValue = NormalizeIdentifier(sink.GetString() ?? string.Empty);
                if (!string.IsNullOrEmpty(sinkValue) && sinkValue.Equals(normalizedTarget, StringComparison.OrdinalIgnoreCase)) return true;
            }
        }
        if (query.TryGetProperty("sinks", out var sinks) && sinks.ValueKind == JsonValueKind.Array)
        {
            foreach (var sink in sinks.EnumerateArray())
            {
                var sinkValue = NormalizeIdentifier(sink.GetString() ?? string.Empty);
                if (!string.IsNullOrEmpty(sinkValue) && sinkValue.Equals(normalizedTarget, StringComparison.OrdinalIgnoreCase)) return true;
            }
        }
        if (query.TryGetProperty("queryString", out var qstr))
        {
            var normalizedQueryString = NormalizeSql(qstr.GetString());
            if (!string.IsNullOrEmpty(normalizedQueryString) && normalizedQueryString.Contains(normalizedTarget, StringComparison.Ordinal)) return true;
        }
        return false;
    }

    private static bool TryFindQueryIdInJson(string showQueriesOutput, string normalizedTarget, string normalizedStatement, out string? queryId)
    {
        queryId = null;
        try
        {
            using var doc = JsonDocument.Parse(showQueriesOutput);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return false;
            foreach (var entry in doc.RootElement.EnumerateArray())
            {
                if (!entry.TryGetProperty("queries", out var queries) || queries.ValueKind != JsonValueKind.Array) continue;
                foreach (var query in queries.EnumerateArray())
                {
                    var id = query.TryGetProperty("id", out var idProp) ? idProp.GetString() : null;
                    if (string.IsNullOrWhiteSpace(id)) continue;
                    var topicMatches = false;
                    if (!string.IsNullOrEmpty(normalizedTarget))
                    {
                        if (query.TryGetProperty("sinkKafkaTopics", out var sinks) && sinks.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var sink in sinks.EnumerateArray())
                            {
                                var sinkValue = NormalizeIdentifier(sink.GetString() ?? string.Empty);
                                if (!string.IsNullOrEmpty(sinkValue) && sinkValue.Equals(normalizedTarget, StringComparison.OrdinalIgnoreCase)) { topicMatches = true; break; }
                            }
                        }
                        if (!topicMatches && query.TryGetProperty("sinks", out var sinksLegacy) && sinksLegacy.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var sink in sinksLegacy.EnumerateArray())
                            {
                                var sinkValue = NormalizeIdentifier(sink.GetString() ?? string.Empty);
                                if (!string.IsNullOrEmpty(sinkValue) && sinkValue.Equals(normalizedTarget, StringComparison.OrdinalIgnoreCase)) { topicMatches = true; break; }
                            }
                        }
                    }
                    var statementMatches = false;
                    if (!string.IsNullOrEmpty(normalizedStatement) && query.TryGetProperty("queryString", out var queryStringProp))
                    {
                        var normalizedQueryString = NormalizeSql(queryStringProp.GetString());
                        if (!string.IsNullOrEmpty(normalizedQueryString) && normalizedQueryString.Contains(normalizedStatement, StringComparison.Ordinal)) statementMatches = true;
                    }
                    if (topicMatches || statementMatches) { queryId = id; return true; }
                }
            }
        }
        catch (JsonException) { }
        return false;
    }

    private static List<string> ParseColumns(string line)
    {
        var list = new List<string>();
        if (string.IsNullOrEmpty(line)) return list;
        var builder = new StringBuilder();
        foreach (var ch in line)
        {
            if (ch == '|') { AddColumn(builder, list); }
            else { builder.Append(ch); }
        }
        AddColumn(builder, list);
        return list;

        static void AddColumn(StringBuilder sb, List<string> target)
        {
            if (sb.Length == 0) return;
            var value = sb.ToString().Trim();
            sb.Clear();
            if (value.Length > 0) target.Add(value);
        }
    }
}

public interface IWaitDiagnosticsSink
{
    void Append(string label, string body);
}

internal sealed class FileWaitDiagnosticsSink : IWaitDiagnosticsSink
{
    public void Append(string label, string body)
    {
        var path = KsqlWaitService.ResolveWaitRawLogPath();
        var dir = System.IO.Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) System.IO.Directory.CreateDirectory(dir);
        var sb = new StringBuilder();
        sb.AppendLine("-----");
        sb.AppendLine($"UTC {System.DateTime.UtcNow:O}");
        sb.AppendLine($"Label: {label}");
        sb.AppendLine("Body:");
        sb.AppendLine(body ?? string.Empty);
        System.IO.File.AppendAllText(path, sb.ToString());
    }
}