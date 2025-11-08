using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Infrastructure.KsqlDb;

internal class KsqlDbClient : IKsqlDbClient, IDisposable
{
    private readonly HttpClient _client;
    private readonly ILogger<KsqlDbClient> _logger;

    public KsqlDbClient(Uri baseAddress, ILogger<KsqlDbClient>? logger = null)
    {
        _client = new HttpClient { BaseAddress = baseAddress };
        try
        {
            _client.DefaultRequestHeaders.Accept.Clear();
            _client.DefaultRequestHeaders.Accept.ParseAdd("application/vnd.ksql.v1+json");
        }
        catch { }
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<KsqlDbClient>.Instance;
    }

    public async Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
    {
        _logger.LogDebug("Executing KSQL statement: {Statement}", statement);
        var payload = new { ksql = statement, streamsProperties = new { } };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await _client.PostAsync("/ksql", content);
        var body = await response.Content.ReadAsStringAsync();
        _logger.LogDebug("KSQL response ({StatusCode}): {Body}", (int)response.StatusCode, body);
        var success = response.IsSuccessStatusCode && !body.Contains("\"error_code\"");
        // Harden success detection: fail on statement_error or non-SUCCESS commandStatus
        try
        {
            if (!string.IsNullOrWhiteSpace(body))
            {
                if (body.IndexOf("statement_error", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    success = false;
                }
                else
                {
                    using var doc = JsonDocument.Parse(body);
                    if (doc.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var el in doc.RootElement.EnumerateArray())
                        {
                            if (IsNonSuccessCommand(el)) { success = false; break; }
                        }
                    }
                    else if (doc.RootElement.ValueKind == JsonValueKind.Object)
                    {
                        if (IsNonSuccessCommand(doc.RootElement)) success = false;
                    }
                }
            }
        }
        catch { }
        return new KsqlDbResponse(success, body);

        static bool IsNonSuccessCommand(JsonElement el)
        {
            try
            {
                if (el.ValueKind != JsonValueKind.Object) return false;
                if (el.TryGetProperty("@type", out var t) && t.ValueKind == JsonValueKind.String)
                {
                    var v = t.GetString();
                    if (!string.IsNullOrEmpty(v) && v.Equals("statement_error", StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                if (el.TryGetProperty("commandStatus", out var status) && status.ValueKind == JsonValueKind.Object)
                {
                    if (status.TryGetProperty("status", out var s) && s.ValueKind == JsonValueKind.String)
                    {
                        var v = s.GetString();
                        if (!string.IsNullOrEmpty(v) && !v.Equals("SUCCESS", StringComparison.OrdinalIgnoreCase))
                            return true;
                    }
                }
            }
            catch { }
            return false;
        }
    }

    public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql)
    {
        return ExecuteStatementAsync($"EXPLAIN {ksql}");
    }

    public async Task<HashSet<string>> GetTableTopicsAsync()
    {
        var sql = "SHOW TABLES;";
        var response = await ExecuteStatementAsync(sql);
        var tableTopics = new HashSet<string>();
        if (!response.IsSuccess)
            return tableTopics;

        try
        {
            using var doc = JsonDocument.Parse(response.Message);
            foreach (var item in doc.RootElement.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                if (item.TryGetProperty("tables", out var arr))
                {
                    foreach (var element in arr.EnumerateArray())
                    {
                        // Use the "topic" field here
                        if (element.TryGetProperty("topic", out var topicEl) && topicEl.ValueKind == JsonValueKind.String)
                        {
                            var topic = topicEl.GetString();
                            if (!string.IsNullOrEmpty(topic))
                                tableTopics.Add(topic.ToLowerInvariant()); // lowercasing is safer
                        }
                        // Additionally add the "name" (table name) as a cross-check
                        if (element.TryGetProperty("name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String)
                        {
                            var tableName = nameEl.GetString();
                            if (!string.IsNullOrEmpty(tableName))
                                tableTopics.Add(tableName.ToLowerInvariant());
                        }
                    }
                }
            }
        }
        catch
        {
            // ignore parse errors
        }

        return tableTopics;
    }

    public async Task<HashSet<string>> GetStreamTopicsAsync()
    {
        var sql = "SHOW STREAMS;";
        var response = await ExecuteStatementAsync(sql);
        var topics = new HashSet<string>();
        if (!response.IsSuccess)
            return topics;

        try
        {
            using var doc = JsonDocument.Parse(response.Message);
            foreach (var item in doc.RootElement.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                if (item.TryGetProperty("streams", out var arr))
                {
                    foreach (var element in arr.EnumerateArray())
                    {
                        if (element.TryGetProperty("topic", out var topicEl) && topicEl.ValueKind == JsonValueKind.String)
                        {
                            var topic = topicEl.GetString();
                            if (!string.IsNullOrEmpty(topic))
                                topics.Add(topic.ToLowerInvariant());
                        }
                        if (element.TryGetProperty("name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String)
                        {
                            var streamName = nameEl.GetString();
                            if (!string.IsNullOrEmpty(streamName))
                                topics.Add(streamName.ToLowerInvariant());
                        }
                    }
                }
            }
        }
        catch { }

        return topics;
    }

    public async Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new
        {
            sql,
            properties = new System.Collections.Generic.Dictionary<string, object>
            {
                ["ksql.streams.auto.offset.reset"] = "earliest"
            }
        };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var request = new HttpRequestMessage(HttpMethod.Post, "/query-stream") { Content = content };
        // Stream the response without buffering the whole content
        using var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        int count = 0;
        while (!reader.EndOfStream && !cts.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) break;
            // Heuristic: ksqlDB returns JSON lines with a "row" field for data
            if (line.IndexOf("\"row\"", StringComparison.OrdinalIgnoreCase) >= 0)
                count++;
        }
        return count;
    }

    public async Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new { ksql = sql, streamsProperties = new { } };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(15));
        HttpResponseMessage? response = null;
        string body = string.Empty;
        try
        {
            response = await _client.PostAsync("/query", content, cts.Token);
            body = await response.Content.ReadAsStringAsync(cts.Token);
            if (!response.IsSuccessStatusCode || body.IndexOf("\"error_code\"", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Fall back to push query (EMIT CHANGES LIMIT 1)
                var push = sql.IndexOf("EMIT CHANGES", StringComparison.OrdinalIgnoreCase) >= 0
                    ? sql
                    : (sql.TrimEnd().EndsWith(";", StringComparison.Ordinal) ? sql.TrimEnd()[..^1] : sql) + " EMIT CHANGES LIMIT 1;";
                return await ExecuteQueryStreamCountAsync(push, TimeSpan.FromSeconds(10));
            }
        }
        catch
        {
            // Fall back to push query on transport/protocol errors
            var push = sql.IndexOf("EMIT CHANGES", StringComparison.OrdinalIgnoreCase) >= 0
                ? sql
                : (sql.TrimEnd().EndsWith(";", StringComparison.Ordinal) ? sql.TrimEnd()[..^1] : sql) + " EMIT CHANGES LIMIT 1;";
            return await ExecuteQueryStreamCountAsync(push, TimeSpan.FromSeconds(10));
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            int cnt = 0;
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out _))
                        cnt++;
                }
            }
            return cnt;
        }
        catch
        {
            // Fallback: count occurrences of "row" if JSON parsing fails
            var idx = 0; int count = 0;
            while ((idx = body.IndexOf("\"row\"", idx, StringComparison.OrdinalIgnoreCase)) >= 0)
            { count++; idx += 5; }
            return count;
        }
    }

    public async Task<System.Collections.Generic.List<object?[]>> ExecutePullQueryRowsAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new { ksql = sql, streamsProperties = new { } };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var response = await _client.PostAsync("/query", content, cts.Token);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync(cts.Token);
        var rows = new System.Collections.Generic.List<object?[]>();
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out var rowEl))
                    {
                        if (rowEl.TryGetProperty("columns", out var cols) && cols.ValueKind == JsonValueKind.Array)
                        {
                            var list = new object?[cols.GetArrayLength()];
                            int i = 0;
                            foreach (var c in cols.EnumerateArray())
                            {
                                list[i++] = c.ValueKind switch
                                {
                                    JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                                    JsonValueKind.String => c.GetString(),
                                    JsonValueKind.True => true,
                                    JsonValueKind.False => false,
                                    JsonValueKind.Null => null,
                                    _ => c.ToString()
                                };
                            }
                            rows.Add(list);
                        }
                    }
                }
            }
        }
        catch
        {
            // best-effort parsing; return what we have
        }
        return rows;
    }

    public async Task<System.Collections.Generic.List<object?[]>> ExecuteQueryStreamRowsAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new { sql = sql, streamsProperties = new { } };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var request = new HttpRequestMessage(HttpMethod.Post, "/query-stream") { Content = content };
        using var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        response.EnsureSuccessStatusCode();
        var rows = new System.Collections.Generic.List<object?[]>();
        using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        while (!reader.EndOfStream && !cts.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line))
                continue;
            try
            {
                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;
                if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("row", out var rowEl))
                {
                    if (rowEl.TryGetProperty("columns", out var cols) && cols.ValueKind == JsonValueKind.Array)
                    {
                        var list = new object?[cols.GetArrayLength()];
                        int i = 0;
                        foreach (var c in cols.EnumerateArray())
                        {
                            list[i++] = c.ValueKind switch
                            {
                                JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                                JsonValueKind.String => c.GetString(),
                                JsonValueKind.True => true,
                                JsonValueKind.False => false,
                                JsonValueKind.Null => null,
                                _ => c.ToString()
                            };
                        }
                        rows.Add(list);
                        // Most of our use cases provide LIMIT; stop once we collected at least one row
                        if (rows.Count > 0)
                            break;
                    }
                }
            }
            catch { }
        }
        return rows;
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}