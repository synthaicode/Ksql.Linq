using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Cache;

/// <summary>
/// Manages registration and lifecycle of table caches.
/// </summary>
public interface ITableCacheManager
{
    Task InitializeAsync(CancellationToken ct = default);
}

