namespace Ksql.Linq.Infrastructure.KsqlDb;

using System; // for TimeSpan
using System.Collections.Generic;
using System.Threading.Tasks;

public interface IKsqlDbClient
{
    Task<global::Ksql.Linq.KsqlDbResponse> ExecuteStatementAsync(string statement);
    Task<global::Ksql.Linq.KsqlDbResponse> ExecuteExplainAsync(string ksql);
    /// <summary>
    /// Executes SHOW TABLES and returns the Kafka topic names of all tables.
    /// </summary>
    Task<HashSet<string>> GetTableTopicsAsync();

    /// <summary>
    /// Executes SHOW STREAMS and returns the Kafka topic names of all streams.
    /// Includes both the topic name and the stream name for cross-checking,
    /// normalized to lowercase for matching.
    /// </summary>
    Task<HashSet<string>> GetStreamTopicsAsync();

    /// <summary>
    /// Execute a query via /query-stream and count the number of rows returned.
    /// Intended for assertions with SELECT ... EMIT CHANGES LIMIT N.
    /// </summary>
    Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null);

    /// <summary>
    /// Execute a pull query via /query and return row count.
    /// Suitable for SELECT ... FROM TABLE [WHERE ...] LIMIT N (no EMIT CHANGES).
    /// </summary>
    Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null);
}

