using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Infrastructure.KsqlDb;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Infrastructure.Ksql;

public sealed class KsqlExecutor : IKsqlExecutor
{
    private readonly IKsqlDbClient _client;
    private readonly ILogger? _logger;

    public KsqlExecutor(IKsqlDbClient client, ILogger? logger = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _logger = logger;
    }

    public async Task<global::Ksql.Linq.KsqlDbResponse> ExecuteStatementAsync(string statement, CancellationToken ct = default)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "EXECUTE", Preview(statement));
        var resp = await _client.ExecuteStatementAsync(statement);
        // Log raw (pre-processed) response body for diagnostics
        _logger?.LogInformation("ksql result: {Kind} SUCCESS={Success} BODY={Body}", "EXECUTE", resp.IsSuccess, resp.Message);
        return resp;
    }

    public async Task<global::Ksql.Linq.KsqlDbResponse> ExecuteExplainAsync(string ksql, CancellationToken ct = default)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "EXPLAIN", Preview(ksql));
        var resp = await _client.ExecuteExplainAsync(ksql);
        _logger?.LogInformation("ksql result: {Kind} SUCCESS={Success} BODY={Body}", "EXPLAIN", resp.IsSuccess, resp.Message);
        return resp;
    }

    public Task<int> QueryStreamCountAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "QUERY_STREAM_COUNT", Preview(sql));
        return _client.ExecuteQueryStreamCountAsync(sql, timeout);
    }

    public Task<int> QueryCountAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        _logger?.LogInformation("ksql execute: {Kind} SQL={Preview}", "QUERY_PULL_COUNT", Preview(sql));
        return _client.ExecutePullQueryCountAsync(sql, timeout);
    }

    public Task<List<object?[]>> QueryRowsAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        if (_client is KsqlDbClient concrete)
        {
            return concrete.ExecutePullQueryRowsAsync(sql, timeout);
        }
        throw new NotSupportedException("Underlying IKsqlDbClient does not support row materialization.");
    }

    private static string Preview(string? sql, int max = 120)
    {
        if (string.IsNullOrWhiteSpace(sql)) return string.Empty;
        sql = sql.Replace("\n", " ").Replace("\r", " ").Trim();
        return sql.Length <= max ? sql : sql.Substring(0, max) + "...";
    }
}