using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq;

/// <summary>
/// EventSet-centric helpers that stay in the flat Ksql.Linq namespace.
/// </summary>
public static class EventSetExtensions
{
    /// <summary>
    /// Apply error handling policy to the underlying EventSet.
    /// </summary>
    public static EventSet<T> OnError<T>(this EventSet<T> eventSet, ErrorAction errorAction) where T : class
    {
        if (typeof(T) == typeof(Messaging.DlqEnvelope) && errorAction == ErrorAction.DLQ)
            throw new InvalidOperationException("OnError(DLQ) cannot be used on a DLQ stream (to prevent infinite loops)");

        var policy = new ErrorHandlingPolicy
        {
            Action = errorAction
        };

        return eventSet.WithErrorPolicy(policy);
    }

    /// <summary>
    /// Returns the newest <paramref name="count"/> items ordered by BarTime and removes older items when supported.
    /// </summary>
    public static async Task<List<T>> Limit<T>(this IEntitySet<T> entitySet, int count, CancellationToken cancellationToken = default)
        where T : class
    {
        if (entitySet == null) throw new ArgumentNullException(nameof(entitySet));
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));

        var items = await entitySet.ToListAsync(cancellationToken).ConfigureAwait(false);
        var model = entitySet.GetEntityModel();
        if (model.GetExplicitStreamTableType() != StreamTableType.Table)
            throw new NotSupportedException("Limit is only supported for Table entities.");
        if (model.BarTimeSelector == null)
            throw new InvalidOperationException($"Entity {typeof(T).Name} is missing bar time selector configuration.");

        var selector = (Func<T, DateTime>)model.BarTimeSelector.Compile();
        var ordered = items.OrderByDescending(selector).ToList();
        var toKeep = ordered.Take(count).ToList();
        var toRemove = ordered.Skip(count).ToList();

        if (entitySet is IRemovableEntitySet<T> removable)
        {
            foreach (var item in toRemove)
            {
                await removable.RemoveAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }

        return toKeep;
    }
}