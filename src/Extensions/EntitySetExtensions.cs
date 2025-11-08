using Ksql.Linq.Core.Abstractions;
using System;

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
}
