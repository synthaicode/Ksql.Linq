using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Events;

public interface IRuntimeEventSink
{
    Task PublishAsync(RuntimeEvent evt, CancellationToken ct = default);
}

public static class RuntimeEventBus
{
    public static IRuntimeEventSink? Sink { get; private set; }

    public static void SetSink(IRuntimeEventSink? sink) => Sink = sink;

    public static Task PublishAsync(RuntimeEvent evt, CancellationToken ct = default)
        => Sink is null ? Task.CompletedTask : Sink.PublishAsync(evt, ct);
}
