using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Dlq;

/// <summary>
/// High-level DLQ operations (produce/consume/metrics) for orchestration.
/// </summary>
public interface IDlqService
{
    Task InitializeAsync(CancellationToken ct = default);
}
