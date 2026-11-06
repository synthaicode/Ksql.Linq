namespace Ksql.Linq.Cache.Core;

internal interface ITableCache<T> : System.IDisposable where T : class
{
    System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ToListAsync(
        System.Collections.Generic.List<string>? filter = null,
        System.TimeSpan? timeout = null);

    async System.Threading.Tasks.Task<System.Collections.Generic.List<T>> ToListAsync(
        System.Collections.Generic.List<string>? filter,
        System.TimeSpan? timeout,
        System.Threading.CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Legacy implementations cannot abort their underlying operation.
        var result = await ToListAsync(filter, timeout).WaitAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }
}
