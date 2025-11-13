using System;

namespace Ksql.Linq;

/// <summary>
/// Facade for TimeBucket APIs to avoid exposing the Runtime namespace
/// in call sites. Forwards to Ksql.Linq.Runtime.TimeBucket.
/// </summary>
public static class TimeBucket
{
    // Ensure `<base>_1s_rows_last` exists for migration flows.
    public static System.Threading.Tasks.Task EnsureRowsLastAsync<T>(KsqlContext context) where T : class
        => context.EnsureRowsLastTableAsync<T>();

    public static System.Threading.Tasks.Task<bool> WaitForBucketAsync<T>(
        KsqlContext context,
        Runtime.Period period,
        System.Collections.Generic.IReadOnlyList<string> pkFilter,
        System.DateTime bucketStartUtc,
        System.TimeSpan tolerance,
        System.TimeSpan timeout,
        System.Threading.CancellationToken ct = default) where T : class
    {
        var tb = Runtime.TimeBucket.Get<T>(context, period);
        return tb.WaitForBucketAsync(pkFilter, bucketStartUtc, tolerance, timeout, ct);
    }

    // Simple wrapper: wait for the target bucket then return a fresh snapshot.
    // Tolerance is derived from period (opinionated default) to avoid extra decisions for users.
    public static async System.Threading.Tasks.Task<System.Collections.Generic.List<T>> WaitForBucketAndListAsync<T>(
        KsqlContext context,
        Runtime.Period period,
        System.Collections.Generic.IReadOnlyList<string> pkFilter,
        System.DateTime bucketStartUtc,
        System.TimeSpan timeout,
        System.Threading.CancellationToken ct = default) where T : class
    {
        var tb = Runtime.TimeBucket.Get<T>(context, period);
        System.TimeSpan tolerance = period.Unit == Runtime.PeriodUnit.Minutes
            ? (period.Value <= 1 ? System.TimeSpan.FromSeconds(1) : System.TimeSpan.FromSeconds(2))
            : System.TimeSpan.FromSeconds(1);
        var ok = await tb.WaitForBucketAsync(pkFilter, bucketStartUtc, tolerance, timeout, ct).ConfigureAwait(false);
        if (!ok)
        {
            // Return current snapshot (may be empty) to keep the wrapper simple.
            return await tb.ToListAsync(pkFilter, ct).ConfigureAwait(false);
        }
        return await tb.ToListAsync(pkFilter, ct).ConfigureAwait(false);
    }

    // Waiting overloads on ToListAsync were removed; callers should use ReadAsync/WaitForBucketAsync.

    public static System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ReadAsync<T>(
        KsqlContext context,
        Runtime.Period period,
        System.Collections.Generic.IReadOnlyList<string> pkFilter,
        System.DateTime bucketStartUtc,
        System.TimeSpan? tolerance = null,
        System.Threading.CancellationToken ct = default) where T : class
    {
        var tb = Runtime.TimeBucket.Get<T>(context, period);
        return tb.ReadAsync(pkFilter, bucketStartUtc, tolerance, ct);
    }

    // Simplest read: immediate snapshot (no bucket wait), unified under ReadAsync name.
    public static System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ReadAsync<T>(
        KsqlContext context,
        Runtime.Period period,
        System.Collections.Generic.IReadOnlyList<string> pkFilter,
        System.Threading.CancellationToken ct) where T : class
    {
        return Runtime.TimeBucket.Get<T>(context, period).ToListAsync(pkFilter, ct);
    }

    // Convenience: write a single row via TimeBucket writer
    public static System.Threading.Tasks.Task WriteAsync<T>(
        KsqlContext context,
        Runtime.Period period,
        T row,
        System.Threading.CancellationToken ct = default) where T : class
    {
        return Runtime.TimeBucket.Set<T>(context, period).WriteAsync(row, ct);
    }
}
