using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Messaging;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Query;

/// <summary>
/// EntitySet wrapper that supports JOIN operations.
/// </summary>
public class JoinableEntitySet<T> : IEntitySet<T>, IJoinableEntitySet<T> where T : class
{
    private readonly IEntitySet<T> _baseEntitySet;

    public JoinableEntitySet(IEntitySet<T> baseEntitySet)
    {
        _baseEntitySet = baseEntitySet ?? throw new ArgumentNullException(nameof(baseEntitySet));
    }

    public Task AddAsync(T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.AddAsync(entity, headers, cancellationToken);
    }

    public Task RemoveAsync(T entity, CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.RemoveAsync(entity, cancellationToken);
    }

    public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        return _baseEntitySet.ToListAsync(cancellationToken);
    }

    public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout, bool autoCommit, CancellationToken cancellationToken)
    {
        return _baseEntitySet.ForEachAsync((e, h, _) => action(e), timeout, autoCommit, cancellationToken);
    }

    // Convenience overloads
    public Task ForEachAsync(Func<T, Task> action)
        => _baseEntitySet.ForEachAsync(action);

    public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout)
        => _baseEntitySet.ForEachAsync(action, timeout);

    public Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken)
        => _baseEntitySet.ForEachAsync(action, cancellationToken);

    // Removed obsolete overloads with (T, Dictionary<string,string>) delegate

    public Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout, bool autoCommit, CancellationToken cancellationToken)
    {
        return _baseEntitySet.ForEachAsync(action, timeout, autoCommit, cancellationToken);
    }

    public Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action)
        => _baseEntitySet.ForEachAsync(action);

    public Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action, bool autoCommit)
        => _baseEntitySet.ForEachAsync(action, autoCommit);

    public Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout)
        => _baseEntitySet.ForEachAsync(action, timeout);

    public string GetTopicName() => _baseEntitySet.GetTopicName();

    public EntityModel GetEntityModel() => _baseEntitySet.GetEntityModel();

    public IKsqlContext GetContext() => _baseEntitySet.GetContext();

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await foreach (var item in _baseEntitySet.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    public IJoinResult<T, TInner> Join<TInner, TKey>(
        IEntitySet<TInner> inner,
        Expression<Func<T, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector) where TInner : class
    {
        if (inner == null)
            throw new ArgumentNullException(nameof(inner));
        if (outerKeySelector == null)
            throw new ArgumentNullException(nameof(outerKeySelector));
        if (innerKeySelector == null)
            throw new ArgumentNullException(nameof(innerKeySelector));

        return new JoinResult<T, TInner>(this, inner, outerKeySelector, innerKeySelector);
    }

    public override string ToString()
    {
        return $"JoinableEntitySet<{typeof(T).Name}> wrapping {_baseEntitySet}";
    }
}
