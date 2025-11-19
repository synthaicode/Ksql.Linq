using Ksql.Linq.Transactions;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq;

/// <summary>
/// Transaction support for KsqlContext.
/// </summary>
public abstract partial class KsqlContext
{
    /// <summary>
    /// Begins a new Kafka transaction.
    /// </summary>
    /// <param name="options">Transaction options including TransactionalId</param>
    /// <returns>A new transaction instance</returns>
    public Task<IKsqlTransaction> BeginTransactionAsync(TransactionOptions options)
    {
        if (options == null)
            throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(options.TransactionalId))
            throw new ArgumentException("TransactionalId is required", nameof(options));

        var transaction = new KsqlTransaction(
            _dslOptions,
            options,
            _mappingRegistry,
            GetSchemaRegistryClient(),
            null, // Consumer group metadata will be set when tracking offsets
            _loggerFactory?.CreateLogger<KsqlTransaction>());

        return Task.FromResult<IKsqlTransaction>(transaction);
    }

    /// <summary>
    /// Begins a new Kafka transaction with the specified transactional ID.
    /// </summary>
    /// <param name="transactionalId">Unique identifier for the transaction</param>
    /// <returns>A new transaction instance</returns>
    public Task<IKsqlTransaction> BeginTransactionAsync(string transactionalId)
    {
        return BeginTransactionAsync(new TransactionOptions { TransactionalId = transactionalId });
    }

    /// <summary>
    /// Executes actions within a transaction, automatically committing on success or aborting on failure.
    /// </summary>
    /// <param name="action">The action to execute within the transaction</param>
    /// <param name="options">Transaction options</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ExecuteInTransactionAsync(
        Func<IKsqlTransaction, Task> action,
        TransactionOptions options,
        CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        await using var transaction = await BeginTransactionAsync(options);

        try
        {
            await action(transaction);
            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            transaction.Abort();
            throw;
        }
    }

    /// <summary>
    /// Executes actions within a transaction using the specified transactional ID.
    /// </summary>
    /// <param name="action">The action to execute within the transaction</param>
    /// <param name="transactionalId">Unique identifier for the transaction</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public Task ExecuteInTransactionAsync(
        Func<IKsqlTransaction, Task> action,
        string transactionalId,
        CancellationToken cancellationToken = default)
    {
        return ExecuteInTransactionAsync(
            action,
            new TransactionOptions { TransactionalId = transactionalId },
            cancellationToken);
    }

    /// <summary>
    /// Executes a simple action within a transaction (for producing to multiple topics atomically).
    /// </summary>
    /// <param name="action">The action to execute</param>
    /// <param name="transactionalId">Unique identifier for the transaction</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ExecuteInTransactionAsync(
        Func<Task> action,
        string transactionalId,
        CancellationToken cancellationToken = default)
    {
        await ExecuteInTransactionAsync(
            async _ => await action(),
            transactionalId,
            cancellationToken);
    }
}
