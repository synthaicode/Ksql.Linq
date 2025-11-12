using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq;

public abstract partial class KsqlContext
{
    public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
    {
        _ksqlExecutor ??= new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(
            _ksqlDbClient,
            _loggerFactory?.CreateLogger<Ksql.Linq.Infrastructure.Ksql.KsqlExecutor>());
        return _ksqlExecutor!.ExecuteStatementAsync(statement);
    }

    public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql)
    {
        _ksqlExecutor ??= new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(
            _ksqlDbClient,
            _loggerFactory?.CreateLogger<Ksql.Linq.Infrastructure.Ksql.KsqlExecutor>());
        var rewritten = TryQualifySimpleJoin(ksql);
        return _ksqlExecutor!.ExecuteExplainAsync(rewritten);
    }

    public Task<int> QueryStreamCountAsync(string sql, TimeSpan? timeout = null)
    {
        _ksqlExecutor ??= new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(
            _ksqlDbClient,
            _loggerFactory?.CreateLogger<Ksql.Linq.Infrastructure.Ksql.KsqlExecutor>());
        return _ksqlExecutor!.QueryStreamCountAsync(sql, timeout);
    }

    public Task<int> QueryCountAsync(string sql, TimeSpan? timeout = null)
    {
        _ksqlExecutor ??= new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(
            _ksqlDbClient,
            _loggerFactory?.CreateLogger<Ksql.Linq.Infrastructure.Ksql.KsqlExecutor>());
        return _ksqlExecutor!.QueryCountAsync(sql, timeout);
    }

    public Task<List<object?[]>> QueryRowsAsync(string sql, TimeSpan? timeout = null)
    {
        _ksqlExecutor ??= new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(
            _ksqlDbClient,
            _loggerFactory?.CreateLogger<Ksql.Linq.Infrastructure.Ksql.KsqlExecutor>());
        return _ksqlExecutor!.QueryRowsAsync(sql, timeout);
    }

    // Convenience: Direct Pull Query execution helpers
    public Task<int> PullCountAsync(string tableOrSql, string? where = null, TimeSpan? timeout = null)
    {
        var sql = BuildPullSql(tableOrSql, where, selectCount: true, limit: null);
        return QueryCountAsync(sql, timeout);
    }

    public Task<List<object?[]>> PullRowsAsync(string tableOrSql, string? where = null, int? limit = null, TimeSpan? timeout = null)
    {
        var sql = BuildPullSql(tableOrSql, where, selectCount: false, limit: limit);
        return QueryRowsAsync(sql, timeout);
    }

    private static string BuildPullSql(string tableOrSql, string? where, bool selectCount, int? limit)
    {
        var upper = tableOrSql?.TrimStart();
        if (!string.IsNullOrWhiteSpace(upper) && upper!.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
            return tableOrSql; // already a full SELECT
        var sb = new System.Text.StringBuilder();
        sb.Append(selectCount ? "SELECT 1" : "SELECT *");
        sb.Append(" FROM ");
        sb.Append(tableOrSql);
        if (!string.IsNullOrWhiteSpace(where))
        {
            sb.Append(" WHERE ");
            sb.Append(where);
        }
        if (!selectCount && limit.HasValue && limit.Value > 0)
        {
            sb.Append(" LIMIT ");
            sb.Append(limit.Value);
        }
        sb.Append(';');
        return sb.ToString();
    }
}
