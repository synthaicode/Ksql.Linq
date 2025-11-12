using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Core.Async;

internal static class SafeDelay
{
    public static Task For(TimeSpan delay, CancellationToken ct = default)
    {
        try { return Task.Delay(delay, ct); } catch { return Task.CompletedTask; }
    }

    public static Task Milliseconds(int milliseconds, CancellationToken ct = default)
        => For(TimeSpan.FromMilliseconds(milliseconds), ct);
}
