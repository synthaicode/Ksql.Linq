using Ksql.Linq.Messaging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Core.Abstractions;

/// <summary>
/// Unified interface for query and update operations
/// while preserving LINQ compatibility.
/// </summary>
public interface IEntitySet<T> : IAsyncEnumerable<T> where T : class
{
    // Producer operations
    Task AddAsync(T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default);
    Task RemoveAsync(T entity, CancellationToken cancellationToken = default);

    // Consumer operations
    Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);

    // Streaming operations
    // Primary (fully-specified) overloads
    Task ForEachAsync(Func<T, Task> action, TimeSpan timeout, bool autoCommit, CancellationToken cancellationToken);

    Task ForEachAsync(Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout, bool autoCommit, CancellationToken cancellationToken);

    // No optional-parameter overloads in the interface; see EntitySet extensions for convenience wrappers.



    // Metadata
    string GetTopicName();
    EntityModel GetEntityModel();
    IKsqlContext GetContext();
}
