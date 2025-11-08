using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Scheduling;

/// <summary>
/// Provides market schedule information and manages refresh cadence.
/// </summary>
public interface IMarketScheduleService : IAsyncDisposable
{
    Task StartAsync(CancellationToken ct);
    Task StopAsync();
    DateTime GetNowUtc();
}
