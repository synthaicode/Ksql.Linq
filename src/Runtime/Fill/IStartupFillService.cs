using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Fill;

/// <summary>
/// Startup continuity hook. Must not emit synthetic rows to Kafka/ksqlDB.
/// Keep decisions aligned with project policy (continuation at read/delivery layers).
/// </summary>
public interface IStartupFillService
{
    Task RunAsync(KsqlContext context, CancellationToken ct);
}
