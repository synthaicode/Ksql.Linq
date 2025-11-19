using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Messaging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Transactions;

/// <summary>
/// Extension methods for transactional operations on entity sets.
/// </summary>
public static class TransactionExtensions
{
    /// <summary>
    /// Wraps an entity set to enable transactional processing with exactly-once semantics.
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <param name="entitySet">The entity set to wrap</param>
    /// <param name="options">Transaction options</param>
    /// <returns>A transactional entity set wrapper</returns>
    public static TransactionalEntitySet<T> WithTransaction<T>(
        this IEntitySet<T> entitySet,
        TransactionOptions options) where T : class
    {
        return new TransactionalEntitySet<T>(entitySet, options);
    }

    /// <summary>
    /// Wraps an entity set to enable transactional processing with a simple transactional ID.
    /// </summary>
    public static TransactionalEntitySet<T> WithTransaction<T>(
        this IEntitySet<T> entitySet,
        string transactionalId) where T : class
    {
        return new TransactionalEntitySet<T>(entitySet, new TransactionOptions { TransactionalId = transactionalId });
    }
}

/// <summary>
/// Wrapper for entity sets that enables transactional processing.
/// </summary>
/// <typeparam name="T">Entity type</typeparam>
public class TransactionalEntitySet<T> where T : class
{
    private readonly IEntitySet<T> _innerSet;
    private readonly TransactionOptions _options;

    public TransactionalEntitySet(IEntitySet<T> entitySet, TransactionOptions options)
    {
        _innerSet = entitySet ?? throw new ArgumentNullException(nameof(entitySet));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Gets the underlying entity set.
    /// </summary>
    public IEntitySet<T> InnerSet => _innerSet;

    /// <summary>
    /// Gets the transaction options.
    /// </summary>
    public TransactionOptions Options => _options;

    /// <summary>
    /// Gets the topic name.
    /// </summary>
    public string GetTopicName() => _innerSet.GetTopicName();

    /// <summary>
    /// Processes each message with exactly-once semantics.
    /// </summary>
    /// <typeparam name="TResult">Result entity type</typeparam>
    /// <param name="targetSet">Target entity set for produced messages</param>
    /// <param name="transform">Transform function</param>
    /// <param name="batchSize">Number of messages per transaction batch</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ForEachAsync<TResult>(
        IEntitySet<TResult> targetSet,
        Func<T, TResult> transform,
        int batchSize = 100,
        CancellationToken cancellationToken = default) where TResult : class
    {
        var context = _innerSet.GetContext();
        if (context is KsqlContext ksqlContext)
        {
            await ksqlContext.ConsumeTransformProduceAsync(
                _innerSet,
                targetSet,
                transform,
                _options,
                batchSize,
                cancellationToken);
        }
        else
        {
            throw new InvalidOperationException("Transactional processing requires KsqlContext");
        }
    }

    /// <summary>
    /// Processes each message with exactly-once semantics using async transform.
    /// </summary>
    public async Task ForEachAsync<TResult>(
        IEntitySet<TResult> targetSet,
        Func<T, Task<TResult>> transformAsync,
        int batchSize = 100,
        CancellationToken cancellationToken = default) where TResult : class
    {
        var context = _innerSet.GetContext();
        if (context is KsqlContext ksqlContext)
        {
            await ksqlContext.ConsumeTransformProduceAsync(
                _innerSet,
                targetSet,
                transformAsync,
                _options,
                batchSize,
                cancellationToken);
        }
        else
        {
            throw new InvalidOperationException("Transactional processing requires KsqlContext");
        }
    }

    /// <summary>
    /// Processes each message with exactly-once semantics, producing to multiple targets.
    /// </summary>
    /// <param name="action">Action that receives the entity and a transaction context</param>
    /// <param name="batchSize">Number of messages per transaction batch</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ForEachAsync(
        Func<T, TransactionContext, Task> action,
        int batchSize = 100,
        CancellationToken cancellationToken = default)
    {
        var context = _innerSet.GetContext();
        if (context is not KsqlContext ksqlContext)
        {
            throw new InvalidOperationException("Transactional processing requires KsqlContext");
        }

        // This is a more complex pattern that allows producing to multiple targets
        // within a single transaction
        await ksqlContext.ExecuteInTransactionAsync(
            async transaction =>
            {
                await foreach (var entity in _innerSet.WithCancellation(cancellationToken))
                {
                    var txContext = new TransactionContext(transaction, _innerSet.GetTopicName());
                    await action(entity, txContext);
                }
            },
            _options,
            cancellationToken);
    }
}

/// <summary>
/// Context for transactional operations within ForEachAsync.
/// </summary>
public class TransactionContext
{
    private readonly IKsqlTransaction _transaction;
    private readonly string _sourceTopicName;

    internal TransactionContext(IKsqlTransaction transaction, string sourceTopicName)
    {
        _transaction = transaction;
        _sourceTopicName = sourceTopicName;
    }

    /// <summary>
    /// Produces an entity to a topic within the transaction.
    /// </summary>
    /// <typeparam name="TTarget">Target entity type</typeparam>
    /// <param name="topicName">Target topic name</param>
    /// <param name="entity">Entity to produce</param>
    /// <param name="headers">Optional message headers</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public Task ProduceAsync<TTarget>(
        string topicName,
        TTarget entity,
        Dictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default) where TTarget : class
    {
        return _transaction.ProduceAsync(topicName, entity, headers, cancellationToken);
    }

    /// <summary>
    /// Produces an entity to an entity set within the transaction.
    /// </summary>
    public Task ProduceAsync<TTarget>(
        IEntitySet<TTarget> targetSet,
        TTarget entity,
        Dictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default) where TTarget : class
    {
        return _transaction.ProduceAsync(targetSet.GetTopicName(), entity, headers, cancellationToken);
    }

    /// <summary>
    /// Tracks the consumed offset for exactly-once semantics.
    /// </summary>
    /// <param name="partition">Partition number</param>
    /// <param name="offset">Offset value</param>
    public void TrackOffset(int partition, long offset)
    {
        _transaction.TrackConsumedOffset(_sourceTopicName, partition, offset);
    }
}
