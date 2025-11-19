using Ksql.Linq.Core.Abstractions;
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
    /// Adds an entity to an entity set within the transaction.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="entitySet">Target entity set</param>
    /// <param name="entity">Entity to add</param>
    /// <param name="headers">Optional message headers</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task AddAsync<T>(IEntitySet<T> entitySet, T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) where T : class;

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
