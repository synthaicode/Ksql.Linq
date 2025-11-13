using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Fill;

/// <summary>
/// Default startup fill implementation: does nothing except optional readiness checks.
/// Strictly avoids emitting any synthetic records in accordance with design policy.
/// </summary>
public sealed class NoopStartupFillService : IStartupFillService
{
    public Task RunAsync(KsqlContext context, CancellationToken ct)
    {
        // Intentionally no-op; optional: ensure rows_last tables exist for hub streams if needed in future.
        context.Logger?.LogInformation("StartupFillService: no-op (no synthetic rows emitted)");
        return Task.CompletedTask;
    }
}
