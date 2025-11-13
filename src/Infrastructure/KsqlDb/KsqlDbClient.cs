using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.KsqlDb;

internal class KsqlDbClient : IKsqlDbClient, IDisposable
{
    private readonly HttpClient _client;
    private readonly ILogger<KsqlDbClient> _logger;
    private readonly KsqlHttp _http;

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
        _http = new KsqlHttp(_client);
    }

    public async Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
    {
        _logger.LogDebug("Executing KSQL statement: {Statement}", statement);
        var payload = new { ksql = statement, streamsProperties = new { } };
        using var response = await _http.PostJsonAsync("/ksql", payload);
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
        if (!response.IsSuccess) return new HashSet<string>();
        return KsqlJsonUtils.ExtractLowercasedFields(response.Message, "tables", "topic", "name");
    }

    public async Task<HashSet<string>> GetStreamTopicsAsync()
    {
        var sql = "SHOW STREAMS;";
        var response = await ExecuteStatementAsync(sql);
        if (!response.IsSuccess) return new HashSet<string>();
        return KsqlJsonUtils.ExtractLowercasedFields(response.Message, "streams", "topic", "name");
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
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(60));
        // Stream the response without buffering the whole content
        using var response = await _http.SendJsonStreamAsync("/query-stream", payload, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        int count = 0;
        while (!reader.EndOfStream && !cts.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) break;
            if (KsqlJsonUtils.TryParseRowLine(line, out _)) count++;
        }
        return count;
    }

    public async Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new { ksql = sql, streamsProperties = new { } };
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(15));
        HttpResponseMessage? response = null;
        string body = string.Empty;
        try
        {
            response = await _http.PostJsonAsync("/query", payload, cts.Token);
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

        return KsqlJsonUtils.CountRowsInArrayBody(body);
    }

    public async Task<System.Collections.Generic.List<object?[]>> ExecutePullQueryRowsAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new { ksql = sql, streamsProperties = new { } };
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var response = await _http.PostJsonAsync("/query", payload, cts.Token);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync(cts.Token);
        return KsqlJsonUtils.ParseRowsFromBody(body);
    }

    public async Task<System.Collections.Generic.List<object?[]>> ExecuteQueryStreamRowsAsync(string sql, TimeSpan? timeout = null)
    {
        var payload = new { sql = sql, streamsProperties = new { } };
        using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var response = await _http.SendJsonStreamAsync("/query-stream", payload, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        response.EnsureSuccessStatusCode();
        var rows = new System.Collections.Generic.List<object?[]>();
        using var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        while (!reader.EndOfStream && !cts.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line))
                continue;
            if (KsqlJsonUtils.TryParseRowLine(line, out var cols) && cols is not null)
            {
                rows.Add(cols);
                if (rows.Count > 0) break; // most use-cases have LIMIT
            }
        }
        return rows;
    }

    public void Dispose()
    {
        _client.Dispose();
    }
}
