using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Cache;

public sealed class TableCacheManagerNoop : ITableCacheManager
{
    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;
}
