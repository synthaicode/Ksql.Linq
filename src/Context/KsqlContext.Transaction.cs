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

    /// <summary>
    /// Creates a transactional consumer for Exactly-Once Semantics processing.
    /// </summary>
    /// <typeparam name="TSource">Source entity type</typeparam>
    /// <typeparam name="TTarget">Target entity type</typeparam>
    /// <param name="sourceTopicName">Source topic name</param>
    /// <param name="targetTopicName">Target topic name</param>
    /// <param name="options">Transaction options</param>
    /// <returns>A transactional consumer instance</returns>
    public TransactionalConsumer<TSource, TTarget> CreateTransactionalConsumer<TSource, TTarget>(
        string sourceTopicName,
        string targetTopicName,
        TransactionOptions options)
        where TSource : class
        where TTarget : class
    {
        return new TransactionalConsumer<TSource, TTarget>(
            _dslOptions,
            options,
            _mappingRegistry,
            GetSchemaRegistryClient(),
            sourceTopicName,
            targetTopicName,
            _loggerFactory?.CreateLogger<TransactionalConsumer<TSource, TTarget>>());
    }

    /// <summary>
    /// Processes messages with Exactly-Once Semantics (consume-transform-produce pattern).
    /// </summary>
    /// <typeparam name="TSource">Source entity type</typeparam>
    /// <typeparam name="TTarget">Target entity type</typeparam>
    /// <param name="sourceSet">Source entity set</param>
    /// <param name="targetSet">Target entity set</param>
    /// <param name="transform">Transform function</param>
    /// <param name="options">Transaction options</param>
    /// <param name="batchSize">Number of messages per transaction batch</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ConsumeTransformProduceAsync<TSource, TTarget>(
        IEntitySet<TSource> sourceSet,
        IEntitySet<TTarget> targetSet,
        Func<TSource, TTarget> transform,
        TransactionOptions options,
        int batchSize = 100,
        CancellationToken cancellationToken = default)
        where TSource : class
        where TTarget : class
    {
        var sourceTopicName = sourceSet.GetTopicName();
        var targetTopicName = targetSet.GetTopicName();

        await using var consumer = CreateTransactionalConsumer<TSource, TTarget>(
            sourceTopicName,
            targetTopicName,
            options);

        await consumer.ProcessAsync(transform, batchSize, cancellationToken);
    }

    /// <summary>
    /// Processes messages with Exactly-Once Semantics using async transform.
    /// </summary>
    public async Task ConsumeTransformProduceAsync<TSource, TTarget>(
        IEntitySet<TSource> sourceSet,
        IEntitySet<TTarget> targetSet,
        Func<TSource, Task<TTarget>> transformAsync,
        TransactionOptions options,
        int batchSize = 100,
        CancellationToken cancellationToken = default)
        where TSource : class
        where TTarget : class
    {
        var sourceTopicName = sourceSet.GetTopicName();
        var targetTopicName = targetSet.GetTopicName();

        await using var consumer = CreateTransactionalConsumer<TSource, TTarget>(
            sourceTopicName,
            targetTopicName,
            options);

        await consumer.ProcessAsync(transformAsync, batchSize, cancellationToken);
    }
}
