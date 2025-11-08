using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Dlq;

public sealed class DlqServiceNoop : IDlqService
{
    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;
}
