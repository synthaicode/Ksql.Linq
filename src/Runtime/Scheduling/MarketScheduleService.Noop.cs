using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Scheduling;

public sealed class MarketScheduleServiceNoop : IMarketScheduleService
{
    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
    public Task StopAsync() => Task.CompletedTask;
    public DateTime GetNowUtc() => DateTime.UtcNow;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
