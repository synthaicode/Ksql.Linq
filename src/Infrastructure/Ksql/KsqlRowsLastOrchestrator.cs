using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Query.Builders.Utilities;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.Query.Planning;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Ksql;

/// <summary>
/// Orchestrates creation of the companion rows_last TABLE for a given 1s rows STREAM.
/// Existence check, optional DESCRIBE fallback to infer columns, DDL build and execution with retries.
/// </summary>
internal static class KsqlRowsLastOrchestrator
{
    public static async Task<KsqlDbResponse> EnsureAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        Func<Task<HashSet<string>>> getTableTopics,
        Func<Task<HashSet<string>>> getStreamTopics,
        EntityModel rowsModel,
        int ddlRetryCount,
        int ddlRetryInitialDelayMs,
        Func<Events.RuntimeEvent, Task>? publishEvent = null)
    {
        if (rowsModel == null) return new KsqlDbResponse(true, "rowsModel null");

        var rowsTopic = rowsModel.GetTopicName();
        var targetTopic = rowsTopic + "_last";

        var tables = await getTableTopics().ConfigureAwait(false);
        if (tables.Contains(targetTopic))
        {
            if (publishEvent != null)
                await SafePublish(publishEvent, new Events.RuntimeEvent { Name = "rows_last.ready", Phase = "exists", Entity = rowsModel?.EntityType?.Name, Topic = targetTopic, Success = true, Message = "rows_last already exists" });
            return new KsqlDbResponse(true, $"SKIPPED (exists): {targetTopic}");
        }

        // Ensure source stream is visible in metastore
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var streams = await getStreamTopics().ConfigureAwait(false);
                if (streams.Contains(rowsTopic) || streams.Contains(rowsTopic.ToUpperInvariant()) || streams.Contains(rowsTopic.ToLowerInvariant()))
                    break;
            }
            catch { }
            await Task.Delay(500).ConfigureAwait(false);
        }

        // Determine key/value columns
        var metadata = rowsModel.GetOrCreateMetadata();
        var keyCols = TryGetNames(rowsModel.KeyProperties);
        var allCols = TryGetNames(rowsModel.AllProperties);
        if (keyCols.Length == 0 || allCols.Length == 0)
        {
            var metaKeys = metadata.Keys.Names;
            if (metaKeys != null && metaKeys.Length > 0)
                keyCols = metaKeys.Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
            var metaValues = metadata.Projection.Names;
            if (metaValues != null && metaValues.Length > 0)
                allCols = keyCols.Concat(metaValues.Where(s => !string.IsNullOrWhiteSpace(s))).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        }
        if (keyCols.Length == 0 || allCols.Length == 0)
        {
            // Fallback: DESCRIBE rows stream for fields
            try
            {
                var desc = await execute($"DESCRIBE {rowsTopic.ToUpperInvariant()} EXTENDED;").ConfigureAwait(false);
                if (desc.IsSuccess && !string.IsNullOrWhiteSpace(desc.Message))
                {
                    (keyCols, allCols) = ParseFieldsFromDescribe(desc.Message, keyCols);
                }
            }
            catch { }
        }
        var nonKeyCols = allCols.Where(n => !keyCols.Any(k => string.Equals(k, n, StringComparison.OrdinalIgnoreCase))).ToArray();
        // Exclude BUCKETSTART from keys; ensure an appropriate bucket/timestamp column exists in the values
        keyCols = keyCols.Where(k => !k.Equals("BUCKETSTART", StringComparison.OrdinalIgnoreCase)).ToArray();
        var timestampColumn = metadata.TimestampColumn;
        if (!string.IsNullOrWhiteSpace(timestampColumn))
        {
            var tsUpper = timestampColumn.ToUpperInvariant();
            if (nonKeyCols.Any(v => v.Equals("BUCKETSTART", StringComparison.OrdinalIgnoreCase)))
            {
                nonKeyCols = nonKeyCols.Where(v => !v.Equals("BUCKETSTART", StringComparison.OrdinalIgnoreCase)).ToArray();
                if (!nonKeyCols.Any(v => v.Equals(tsUpper, StringComparison.OrdinalIgnoreCase)))
                    nonKeyCols = nonKeyCols.Concat(new[] { tsUpper }).ToArray();
            }
            else if (!nonKeyCols.Any(v => v.Equals(tsUpper, StringComparison.OrdinalIgnoreCase)))
            {
                nonKeyCols = nonKeyCols.Concat(new[] { tsUpper }).ToArray();
            }
        }
        else
        {
            if (!nonKeyCols.Any(v => v.Equals("BUCKETSTART", StringComparison.OrdinalIgnoreCase)))
                nonKeyCols = nonKeyCols.Concat(new[] { "BUCKETSTART" }).ToArray();
        }

        var retentionMs = metadata.RetentionMs ?? 7L * 24 * 60 * 60 * 1000;
        var ddl = DdlPlanner.BuildRowsLastCtas(
            targetName: targetTopic,
            sourceRowsName: rowsTopic,
            keyColumns: keyCols,
            valueColumns: nonKeyCols,
            partitions: 1,
            replicas: 1,
            retentionMs: retentionMs > 0 ? retentionMs : (long?)null);

        var attempts = Math.Max(3, ddlRetryCount);
        var delayMs = Math.Max(250, ddlRetryInitialDelayMs);
        KsqlDbResponse? response = null;
        for (int attempt = 0; attempt < attempts; attempt++)
        {
            response = await execute(ddl).ConfigureAwait(false);
            if (response.IsSuccess) break;
            var messageText = response?.Message ?? string.Empty;
            var nonFatal = !string.IsNullOrWhiteSpace(messageText)
                && ddl.TrimStart().StartsWith("CREATE ", StringComparison.OrdinalIgnoreCase)
                && messageText.IndexOf("already exists", StringComparison.OrdinalIgnoreCase) >= 0;
            if (nonFatal)
            {
                response = new KsqlDbResponse(true, messageText, response?.ErrorCode, response?.ErrorDetail);
                break;
            }
            if (attempt + 1 >= attempts)
                break;
            await Task.Delay(delayMs).ConfigureAwait(false);
            delayMs = Math.Min(delayMs * 2, 8000);
        }

        if (response == null) response = new KsqlDbResponse(false, "No response");
        if (!response.IsSuccess)
            return response;

        if (publishEvent != null)
            await SafePublish(publishEvent, new Events.RuntimeEvent { Name = "rows_last.ready", Phase = "ddl", Entity = rowsModel?.EntityType?.Name, Topic = targetTopic, Success = true, Message = response.Message });

        // Stabilize persistent query (RUNNING) and confirm entity visibility
        var qid = QueryIdUtils.ExtractQueryId(response);
        if (string.IsNullOrWhiteSpace(qid))
        {
            try { qid = await KsqlWaitClient.TryGetQueryIdFromShowQueriesAsync(execute, targetTopic, ddl, attempts: 5, delayMs: 1000).ConfigureAwait(false); } catch { }
        }

        await WaitForQueryRunningAsync(execute, targetTopic, qid, TimeSpan.FromSeconds(120)).ConfigureAwait(false);
        var exists = await KsqlWaitClient.ConfirmEntityExistsAsync(execute, targetTopic).ConfigureAwait(false);
        if (!exists)
            return new KsqlDbResponse(false, $"rows_last entity not visible: {targetTopic}");

        if (publishEvent != null)
            await SafePublish(publishEvent, new Events.RuntimeEvent { Name = "rows_last.ready", Phase = "stabilized", Entity = rowsModel?.EntityType?.Name, Topic = targetTopic, Success = true, Message = "RUNNING & visible" });

        return new KsqlDbResponse(true, response.Message);

        static string[] TryGetNames(System.Reflection.PropertyInfo[]? props)
            => props == null ? Array.Empty<string>() : props.Where(p => p != null && !string.IsNullOrWhiteSpace(p.Name)).Select(p => p.Name).ToArray();

        static (string[] Keys, string[] All) ParseFieldsFromDescribe(string describeJson, string[] fallbackKeys)
        {
            try
            {
                using var doc = JsonDocument.Parse(describeJson);
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    if (item.ValueKind != JsonValueKind.Object) continue;
                    if (!item.TryGetProperty("sourceDescription", out var source) || source.ValueKind != JsonValueKind.Object) continue;
                    if (!source.TryGetProperty("fields", out var fields) || fields.ValueKind != JsonValueKind.Array) continue;
                    var keys = new List<string>();
                    var values = new List<string>();
                    foreach (var f in fields.EnumerateArray())
                    {
                        if (f.ValueKind != JsonValueKind.Object) continue;
                        if (!f.TryGetProperty("name", out var nameEl) || nameEl.ValueKind != JsonValueKind.String) continue;
                        var name = nameEl.GetString() ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(name)) continue;
                        if (name.Equals("BROKER", StringComparison.OrdinalIgnoreCase) || name.Equals("SYMBOL", StringComparison.OrdinalIgnoreCase))
                            keys.Add(name);
                        else
                            values.Add(name);
                    }
                    if (keys.Count == 0 && fallbackKeys.Length > 0)
                        keys.AddRange(fallbackKeys.Select(s => s.ToUpperInvariant()));
                    return (keys.ToArray(), keys.Concat(values).Distinct(StringComparer.OrdinalIgnoreCase).ToArray());
                }
            }
            catch { }
            return (fallbackKeys, fallbackKeys);
        }
    }

    private static async Task SafePublish(Func<Events.RuntimeEvent, Task> publisher, Events.RuntimeEvent evt)
    {
        try { await publisher(evt).ConfigureAwait(false); } catch { }
    }

    private static async Task WaitForQueryRunningAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string targetEntityName,
        string? queryId,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var targetUpper = KsqlWaitService.NormalizeIdentifier(targetEntityName);
        var qidNorm = KsqlWaitService.NormalizeIdentifier(queryId);
        var consecutive = 0;
        const int required = 5;
        const int pollMs = 2000;
        while (DateTime.UtcNow < deadline)
        {
            var resp = await execute("SHOW QUERIES;").ConfigureAwait(false);
            if (resp.IsSuccess && !string.IsNullOrWhiteSpace(resp.Message))
            {
                var running = KsqlWaitService.TryGetQueryStateFromJson(resp.Message, targetUpper, qidNorm, out var state)
                    ? string.Equals(state, "RUNNING", StringComparison.OrdinalIgnoreCase)
                    : KsqlWaitService.CheckQueryRunningInText(resp.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries), targetUpper, qidNorm);
                if (running)
                {
                    consecutive++;
                    if (consecutive >= required) return;
                }
                else
                {
                    consecutive = 0;
                }
            }
            await Task.Delay(pollMs).ConfigureAwait(false);
        }
        throw new TimeoutException($"CTAS/CSAS query for {targetEntityName} did not reach RUNNING within {timeout.TotalSeconds}s");
    }
}

