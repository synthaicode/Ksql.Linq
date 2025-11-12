using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Incidents;

internal sealed class LoggerIncidentSink : IIncidentSink
{
    public Task PublishAsync(Incident incident, CancellationToken ct = default)
    {
        try
        {
            Console.WriteLine($"[incident] {incident.TimestampUtc:O} {incident.Name} entity={incident.Entity} period={incident.Period} keys=[{string.Join(',', incident.Keys ?? Array.Empty<string>())}] bucket={incident.BucketStartUtc:O} observed={incident.ObservedCount} notes={incident.Notes}");
        }
        catch { }
        return Task.CompletedTask;
    }
}
