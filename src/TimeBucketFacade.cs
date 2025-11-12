using System;
using Ksql.Linq.Runtime;

namespace Ksql.Linq;

/// <summary>
/// Facade for TimeBucket APIs to avoid exposing the Runtime namespace
/// in call sites. Forwards to Ksql.Linq.Runtime.TimeBucket.
/// </summary>
public static class TimeBucket
{
    // New overload: allow using KsqlContext directly
    [Obsolete("Use TimeBucket.ReadAsync<T>() or Runtime.TimeBucket.Get<T>() as needed.")]
    public static Runtime.TimeBucket<T> Get<T>(KsqlContext context, Runtime.Period period) where T : class
    {
        if (period.Unit == Ksql.Linq.Runtime.PeriodUnit.Seconds)
            throw new ArgumentOutOfRangeException(nameof(period), "Period must be minutes or greater.");
        return Runtime.TimeBucket.Get<T>(context, period);
    }

    // New overload: writer using KsqlContext directly
    [Obsolete("Use TimeBucket.WriteAsync<T>() or Runtime.TimeBucket.Set<T>() as needed.")]
    public static Runtime.TimeBucketWriter<T> Set<T>(KsqlContext context, Runtime.Period period) where T : class
    {
        if (period.Unit == Ksql.Linq.Runtime.PeriodUnit.Seconds)
            throw new ArgumentOutOfRangeException(nameof(period), "Period must be minutes or greater.");
        return Runtime.TimeBucket.Set<T>(context, period);
    }

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

    // New overload: use existing CancellationToken for waiting; no separate timeout parameter.
    // Removed: waiting overloads on ToListAsync to keep timeframe concerns within TimeBucket methods.
    [System.Obsolete("Use TimeBucket.Get(...).ReadAtAsync(...) or WaitForBucketAsync(...) + ToListAsync(...)")]
    public static System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ToListAsync<T>(KsqlContext c, Runtime.Period p, System.Collections.Generic.IReadOnlyList<string>? f, System.DateTime? w, System.TimeSpan? tol, System.Threading.CancellationToken ct) where T:class
        => Runtime.TimeBucket.Get<T>(c, p).ToListAsync(f, ct);
    [System.Obsolete("Use TimeBucket.Get(...).ReadAtAsync(...) or WaitForBucketAsync(...) + ToListAsync(...)")]
    public static System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ToListAsync<T>(KsqlContext c, Runtime.Period p, System.Collections.Generic.IReadOnlyList<string>? f, System.DateTime? w, System.Threading.CancellationToken ct) where T:class
        => Runtime.TimeBucket.Get<T>(c, p).ToListAsync(f, ct);

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
