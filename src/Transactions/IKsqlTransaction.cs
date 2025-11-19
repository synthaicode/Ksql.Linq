using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Transactions;

/// <summary>
/// Interface for Kafka transactions in Ksql.Linq.
/// </summary>
public interface IKsqlTransaction : IAsyncDisposable
{
    /// <summary>
    /// Gets the transactional ID for this transaction.
    /// </summary>
    string TransactionalId { get; }

    /// <summary>
    /// Gets whether the transaction has been committed.
    /// </summary>
    bool IsCommitted { get; }

    /// <summary>
    /// Gets whether the transaction has been aborted.
    /// </summary>
    bool IsAborted { get; }

    /// <summary>
    /// Produces a message within the transaction.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="topicName">Target topic name</param>
    /// <param name="entity">Entity to produce</param>
    /// <param name="headers">Optional message headers</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task ProduceAsync<T>(string topicName, T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Tracks a consumed offset for exactly-once semantics.
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="offset">Offset value</param>
    void TrackConsumedOffset(string topic, int partition, long offset);

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts the transaction.
    /// </summary>
    void Abort();
}
