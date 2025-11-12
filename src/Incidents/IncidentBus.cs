using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Incidents;

internal interface IIncidentSink
{
    Task PublishAsync(Incident incident, CancellationToken ct = default);
}

internal static class IncidentBus
{
    public static IIncidentSink? Sink { get; private set; }

    public static void SetSink(IIncidentSink? sink) => Sink = sink;

    public static Task PublishAsync(Incident incident, CancellationToken ct = default)
        => Sink is null ? Task.CompletedTask : Sink.PublishAsync(incident, ct);
}
