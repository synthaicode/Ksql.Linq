using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Core.Async;

public static class SafePoll
{
    public static async Task<bool> UntilAsync(
        Func<CancellationToken, Task<bool>> condition,
        TimeSpan interval,
        TimeSpan timeout,
        int requiredConsecutive = 1,
        TimeSpan stabilizationWindow = default,
        CancellationToken ct = default)
    {
        if (condition == null) throw new ArgumentNullException(nameof(condition));
        if (interval <= TimeSpan.Zero) interval = TimeSpan.FromMilliseconds(200);
        if (timeout < TimeSpan.Zero) timeout = TimeSpan.Zero;
        if (requiredConsecutive <= 0) requiredConsecutive = 1;

        var deadline = DateTime.UtcNow + timeout;
        var consecutive = 0;
        while (timeout == TimeSpan.Zero || DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            bool ok;
            try { ok = await condition(ct).ConfigureAwait(false); }
            catch { ok = false; }
            if (ok)
            {
                consecutive++;
                if (consecutive >= requiredConsecutive)
                {
                    if (stabilizationWindow > TimeSpan.Zero)
                    {
                        await SafeDelay.For(stabilizationWindow, ct).ConfigureAwait(false);
                        try { return await condition(ct).ConfigureAwait(false); } catch { return false; }
                    }
                    return true;
                }
            }
            else
            {
                consecutive = 0;
            }
            await SafeDelay.For(interval, ct).ConfigureAwait(false);
        }
        return false;
    }
}

