using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;

namespace Ksql.Linq.Runtime;

/// <summary>
/// Convenience extensions for pulling hopping window tables.
/// </summary>
public static class HoppingExtensions
{
    /// <summary>
    /// Pull rows from a hopping table by key and optional time range.
    /// </summary>
    public static Task<IReadOnlyList<T>> ReadHoppingAsync<T>(
        this KsqlContext ctx,
        object key,
        DateTime? from = null,
        DateTime? to = null,
        int? limit = null,
        TimeSpan? timeout = null,
        string? tableName = null,
        CancellationToken ct = default)
        where T : class, IWindowedRecord
    {
        if (ctx == null) throw new ArgumentNullException(nameof(ctx));
        var hw = HoppingWindow.Get<T>(ctx, tableName);
        return hw.ToListAsync(key, from, to, limit, timeout, ct);
    }
}
