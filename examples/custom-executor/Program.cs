using System;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Infrastructure.KsqlDb;

// Demonstrates implementing IKsqlExecutor and wrapping behavior (logging/retry)
// Note: Wiring a custom executor into KsqlContext is internal today; this sample
// calls the executor directly with a stub IKsqlDbClient to illustrate usage.

public sealed class LoggingRetryExecutor : IKsqlExecutor
{
    private readonly IKsqlExecutor _inner;
    public LoggingRetryExecutor(IKsqlDbClient client)
    {
        _inner = new KsqlExecutor(client);
    }

    public async Task<KsqlDbResponse> ExecuteStatementAsync(string statement, CancellationToken ct = default)
    {
        Console.WriteLine($"[exec] {Preview(statement)}");
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var res = await _inner.ExecuteStatementAsync(statement, ct).ConfigureAwait(false);
                Console.WriteLine($"[exec] status={res.IsSuccess}");
                return res;
            }
            catch (Exception ex) when (attempt < 3)
            {
                Console.WriteLine($"[exec] retry {attempt}: {ex.Message}");
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt), ct);
            }
        }
    }

    public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql, CancellationToken ct = default)
        => ExecuteStatementAsync($"EXPLAIN {ksql}", ct);

    public Task<int> QueryStreamCountAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default)
        => _inner.QueryStreamCountAsync(sql, timeout, ct);

    public Task<int> QueryCountAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default)
        => _inner.QueryCountAsync(sql, timeout, ct);

    public Task<System.Collections.Generic.List<object?[]>> QueryRowsAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default)
        => _inner.QueryRowsAsync(sql, timeout, ct);

    private static string Preview(string sql)
        => sql.Length <= 120 ? sql : sql.Substring(0, 120) + "...";
}

public sealed class StubKsqlDbClient : IKsqlDbClient, IDisposable
{
    public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
        => Task.FromResult(new KsqlDbResponse(true, "[{\"@type\":\"statement\",\"message\":\"ok\"}]"));
    public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql)
        => ExecuteStatementAsync($"EXPLAIN {ksql}");
    public Task<System.Collections.Generic.HashSet<string>> GetTableTopicsAsync()
        => Task.FromResult(new System.Collections.Generic.HashSet<string>());
    public Task<System.Collections.Generic.HashSet<string>> GetStreamTopicsAsync()
        => Task.FromResult(new System.Collections.Generic.HashSet<string>());
    public Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null)
        => Task.FromResult(0);
    public Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null)
        => Task.FromResult(0);
    public Task<System.Collections.Generic.List<object?[]>> ExecutePullQueryRowsAsync(string sql, TimeSpan? timeout = null)
        => Task.FromResult(new System.Collections.Generic.List<object?[]>());
    public Task<System.Collections.Generic.List<object?[]>> ExecuteQueryStreamRowsAsync(string sql, TimeSpan? timeout = null)
        => Task.FromResult(new System.Collections.Generic.List<object?[]>());
    public void Dispose() { }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        using var client = new StubKsqlDbClient();
        IKsqlExecutor exec = new LoggingRetryExecutor(client);
        var res = await exec.ExecuteStatementAsync("SHOW STREAMS;");
        Console.WriteLine($"Response.Success={res.IsSuccess}\nBody={res.Message}");
    }
}
