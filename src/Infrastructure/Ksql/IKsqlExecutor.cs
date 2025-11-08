using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Ksql;

/// <summary>
/// Thin abstraction over ksqlDB execution endpoints.
/// Intended to decouple orchestration from transport/JSON shape.
/// </summary>
public interface IKsqlExecutor
{
    Task<global::Ksql.Linq.KsqlDbResponse> ExecuteStatementAsync(string statement, CancellationToken ct = default);
    Task<global::Ksql.Linq.KsqlDbResponse> ExecuteExplainAsync(string ksql, CancellationToken ct = default);
    Task<int> QueryStreamCountAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default);
    Task<int> QueryCountAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default);
    Task<List<object?[]>> QueryRowsAsync(string sql, TimeSpan? timeout = null, CancellationToken ct = default);
}