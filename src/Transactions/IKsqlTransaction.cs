using System;
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
    /// Commits the transaction.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts the transaction.
    /// </summary>
    void Abort();
}
