using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Events;

public static class RuntimeEvents
{
    public static async Task TryPublishAsync(RuntimeEvent evt, CancellationToken ct = default)
    {
        try { await RuntimeEventBus.PublishAsync(evt, ct).ConfigureAwait(false); } catch { }
    }

    public static void TryPublishFireAndForget(RuntimeEvent evt, CancellationToken ct = default)
    {
        _ = TryPublishAsync(evt, ct);
    }
}

