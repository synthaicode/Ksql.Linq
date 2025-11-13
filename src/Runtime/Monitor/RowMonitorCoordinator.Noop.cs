using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Monitor;

public sealed class RowMonitorCoordinatorNoop : IRowMonitorCoordinator
{
    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
    public Task StopAsync() => Task.CompletedTask;
    public Task StartForResults(System.Collections.Generic.IReadOnlyList<object> results, CancellationToken ct) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
