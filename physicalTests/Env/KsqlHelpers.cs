using Ksql.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace PhysicalTestEnv;

#nullable enable

public static class KsqlHelpers
{
    public static async Task<KsqlDbResponse> ExecuteStatementWithRetryAsync(KsqlContext ctx, string statement, int retries = 3, int delayMs = 1000)
    {
        Exception? last = null;
        for (var i = 0; i < retries; i++)
        {
            try
            {
                var r = await ctx.ExecuteStatementAsync(statement);
                if (r.IsSuccess) return r;
                last = new InvalidOperationException(r.Message);
            }
            catch (Exception ex)
            {
                last = ex;
            }
            await Task.Delay(delayMs);
        }
        throw last ?? new InvalidOperationException("ExecuteStatementWithRetryAsync failed without exception");
    }

    /// <summary>
    /// Wait until ksqlDB is stable: /healthcheck ready and SHOW QUERIES responds cleanly
    /// for a given number of consecutive times, then optionally wait a settle period.
    /// </summary>
    public static async Task WaitForKsqlStableAsync(string ksqlBaseUrl, int consecutiveOk = 5, TimeSpan? timeout = null, int settleMs = 0)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };

        // Phase 1: /healthcheck consecutive OK
        var consec = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var resp = await http.GetAsync("/healthcheck");
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300)
                {
                    consec++;
                    if (consec >= consecutiveOk) break;
                }
                else consec = 0;
            }
            catch { consec = 0; }
            await Task.Delay(2000);
        }
        if (consec < consecutiveOk)
            throw new TimeoutException("ksqlDB /healthcheck did not stabilize in time");

        // Phase 2: SHOW QUERIES EXTENDED returns valid JSON array without statement_error, consecutive OK
        consec = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var payload = new { ksql = "SHOW QUERIES EXTENDED;", streamsProperties = new { } };
                using var content = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
                using var resp = await http.PostAsync("/ksql", content);
                var body = await resp.Content.ReadAsStringAsync();
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300 && IsValidKsqlArray(body))
                {
                    consec++;
                    if (consec >= consecutiveOk) break;
                }
                else consec = 0;
            }
            catch { consec = 0; }
            await Task.Delay(2000);
        }
        if (consec < consecutiveOk)
            throw new TimeoutException("ksqlDB SHOW QUERIES did not stabilize in time");

        if (settleMs > 0)
            await Task.Delay(settleMs);
    }

    /// <summary>
    /// Wait until all existing persistent queries report RUNNING in SHOW QUERIES EXTENDED.
    /// If there are zero queries, this returns immediately as successful.
    /// </summary>
    public static async Task WaitForQueriesRunningAsync(string ksqlBaseUrl, int consecutiveOk = 3, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(120));
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        var consec = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var payload = new { ksql = "SHOW QUERIES EXTENDED;", streamsProperties = new { } };
                using var content = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
                using var resp = await http.PostAsync("/ksql", content);
                var body = await resp.Content.ReadAsStringAsync();
                if ((int)resp.StatusCode >= 200 && (int)resp.StatusCode < 300 && IsValidKsqlArray(body))
                {
                    if (AllQueriesRunningOrNone(body))
                    {
                        consec++;
                        if (consec >= consecutiveOk) return;
                    }
                    else consec = 0;
                }
                else consec = 0;
            }
            catch { consec = 0; }
            await Task.Delay(2000);
        }
        throw new TimeoutException("ksqlDB queries did not report RUNNING in time");
    }

    private static bool IsValidKsqlArray(string body)
    {
        if (string.IsNullOrWhiteSpace(body)) return false;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Array)
                return false;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (el.TryGetProperty("@type", out var t) && t.ValueKind == System.Text.Json.JsonValueKind.String)
                {
                    var ty = t.GetString();
                    if (!string.IsNullOrWhiteSpace(ty) && ty.Contains("statement_error", StringComparison.OrdinalIgnoreCase))
                        return false;
                }
            }
            return true;
        }
        catch { return false; }
    }

    private static bool AllQueriesRunningOrNone(string body)
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Array) return false;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (!el.TryGetProperty("queries", out var queries) || queries.ValueKind != System.Text.Json.JsonValueKind.Array)
                    continue;
                var count = 0;
                var running = 0;
                foreach (var q in queries.EnumerateArray())
                {
                    count++;
                    // state or status may be present depending on ksqlDB version
                    string? state = null;
                    if (q.TryGetProperty("state", out var stateEl) && stateEl.ValueKind == System.Text.Json.JsonValueKind.String)
                        state = stateEl.GetString();
                    else if (q.TryGetProperty("status", out var statusEl) && statusEl.ValueKind == System.Text.Json.JsonValueKind.String)
                        state = statusEl.GetString();
                    if (!string.IsNullOrWhiteSpace(state) && state.Equals("RUNNING", StringComparison.OrdinalIgnoreCase))
                        running++;
                }
                if (count == 0) return true; // no queries
                if (count > 0 && running == count) return true; // all running
                return false; // some not running
            }
            // No queries section found: treat as no queries
            return true;
        }
        catch { return false; }
    }

    /// <summary>
    /// Backward-compatible: minimal readiness (uses /healthcheck) then optional grace period.
    /// </summary>
    public static async Task WaitForKsqlReadyAsync(string ksqlBaseUrl, TimeSpan? timeout = null, int graceMs = 0)
    {
        await WaitForKsqlStableAsync(ksqlBaseUrl, consecutiveOk: 3, timeout: timeout ?? TimeSpan.FromSeconds(90), settleMs: graceMs);
    }

    /// <summary>
    /// Create KsqlContext (or derived) with retries. The factory is invoked per-attempt.
    /// </summary>
    public static async Task<T> CreateContextWithRetryAsync<T>(Func<T> factory, int retries = 3, int delayMs = 1000) where T : KsqlContext
    {
        Exception? last = null;
        for (var i = 0; i < retries; i++)
        {
            try
            {
                return factory();
            }
            catch (Exception ex)
            {
                last = ex;
            }
            await Task.Delay(delayMs);
        }
        throw last ?? new InvalidOperationException("CreateContextWithRetryAsync failed without exception");
    }

    public static async Task TerminateAllAsync(string ksqlBaseUrl)
    {
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        var payload = new { ksql = "TERMINATE ALL;", streamsProperties = new { } };
        var json = System.Text.Json.JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
        try { using var _ = await http.PostAsync("/ksql", content); } catch { }
    }

    public static async Task DropArtifactsAsync(string ksqlBaseUrl, IEnumerable<string> objectsInDependencyOrder)
    {
        using var http = new HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        foreach (var obj in objectsInDependencyOrder)
        {
            var stmt = $"DROP TABLE IF EXISTS {obj} DELETE TOPIC;";
            var streamStmt = $"DROP STREAM IF EXISTS {obj} DELETE TOPIC;";
            var payload = new { ksql = stmt, streamsProperties = new { } };
            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            try { using var resp = await http.PostAsync("/ksql", content); }
            catch { }
            // Try as stream too
            payload = new { ksql = streamStmt, streamsProperties = new { } };
            json = System.Text.Json.JsonSerializer.Serialize(payload);
            using var content2 = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            try { using var resp2 = await http.PostAsync("/ksql", content2); }
            catch { }
        }
    }

    public static Task TerminateAndDropBarArtifactsAsync(string ksqlBaseUrl)
    {
        // Drop dependents first (tables), then base stream
        var order = new[]
        {
            // Drop LIVE tables first, then base streams; FINAL variants are no longer created.
            "BAR_TBIMP_V2_1s_rows",
            "BAR_TBIMP_V2",
            "TICKS_TBIMP_V2",
            "BAR_TBIMP_5M_LIVE",
            "BAR_TBIMP_1M_LIVE",
            "BAR_TBIMP_1s_rows",
            "BAR_TBIMP",
            "TICKS_TBIMP",
            "bar_tbimp_v2_5m_live",
            "bar_tbimp_v2_1m_live",
            "bar_tbimp_v2_1s_rows",
            "bar_tbimp_v2",
            "ticks_tbimp_v2",
            "bar_tbimp_5m_live",
            "bar_tbimp_1m_live",
            "bar_tbimp_1s_rows",
            "bar_tbimp",
            "ticks_tbimp",
            "bar_1m_live",
            "bar_5m_live",
            "bar_1s_rows"
        };
        return Task.Run(async () =>
        {
            await TerminateAllAsync(ksqlBaseUrl);
            await DropArtifactsAsync(ksqlBaseUrl, order);
        });
    }
}



