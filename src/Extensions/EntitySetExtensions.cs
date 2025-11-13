using Ksql.Linq.Core.Abstractions;
using System;
using Ksql.Linq.Messaging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq;

/// <summary>
/// Bridges IEntitySet helpers onto EventSet-backed implementations.
/// </summary>
public static class EntitySetExtensions
{
    /// <summary>
    /// Applies error handling when the IEntitySet derives from EventSet.
    /// </summary>
    public static EventSet<T> OnError<T>(this IEntitySet<T> entitySet, ErrorAction errorAction) where T : class
    {
        if (entitySet is EventSet<T> eventSet)
        {
            return eventSet.OnError(errorAction);
        }

        throw new InvalidOperationException("OnError is only supported on EventSet-based entity sets.");
    }

    // Convenience wrappers for ForEachAsync (no optional parameters in interface)
    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Task> action) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, true, CancellationToken.None);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Task> action, TimeSpan timeout) where T : class
        => set.ForEachAsync(action, timeout, true, CancellationToken.None);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Task> action, CancellationToken cancellationToken) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, true, cancellationToken);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Task> action, TimeSpan timeout, CancellationToken cancellationToken) where T : class
        => set.ForEachAsync(action, timeout, true, cancellationToken);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Task> action, bool autoCommit, CancellationToken cancellationToken) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, autoCommit, cancellationToken);

     public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Dictionary<string, string>, MessageMeta, Task> action) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, true, CancellationToken.None);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Dictionary<string, string>, MessageMeta, Task> action, bool autoCommit) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, autoCommit, CancellationToken.None);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout) where T : class
        => set.ForEachAsync(action, timeout, true, CancellationToken.None);

    // Overloads with CancellationToken for headers + meta delegate
    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Dictionary<string, string>, MessageMeta, Task> action, CancellationToken cancellationToken) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, true, cancellationToken);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Dictionary<string, string>, MessageMeta, Task> action, TimeSpan timeout, CancellationToken cancellationToken) where T : class
        => set.ForEachAsync(action, timeout, true, cancellationToken);

    public static Task ForEachAsync<T>(this IEntitySet<T> set, Func<T, Dictionary<string, string>, MessageMeta, Task> action, bool autoCommit, CancellationToken cancellationToken) where T : class
        => set.ForEachAsync(action, TimeSpan.Zero, autoCommit, cancellationToken);
}
