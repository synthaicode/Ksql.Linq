using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Monitor;

/// <summary>
/// Coordinates the lifecycle of row monitors for derived entities.
/// </summary>
public interface IRowMonitorCoordinator : IAsyncDisposable
{
    Task StartAsync(CancellationToken ct);
    Task StopAsync();
    Task StartForResults(System.Collections.Generic.IReadOnlyList<object> results, CancellationToken ct);
}