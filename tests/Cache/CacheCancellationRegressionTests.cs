using Ksql.Linq.Cache.Core;
using Ksql.Linq.Cache.Extensions;
using Ksql.Linq.Core.Abstractions;
using Moq;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Cache;

[Trait("Level", "L3")]
public class CacheCancellationRegressionTests
{
    private static TableCache<Row> Cache(Func<TimeSpan?, Task> wait, Func<IEnumerable<KeyValuePair<object, object>>> enumerate) =>
        new(wait, new Lazy<Func<IEnumerable<KeyValuePair<object, object>>>>(() => enumerate), k => (string)k, (k, v, t) => new Row());

    private static ReadCachedEntitySet<Row> ReadSet(ITableCache<Row> cache)
    {
        var context = new Mock<IKsqlContext>().Object;
        var registry = new TableCacheRegistry();
        registry.Register(typeof(Row), cache);
        context.AttachTableCacheRegistry(registry);
        var model = new EntityModel { EntityType = typeof(Row) };
        return new ReadCachedEntitySet<Row>(context, model, baseSet: new BaseSet(context, model));
    }

    [Fact]
    public async Task RunningWait_CancelsCallerAndDoesNotEnumerateAfterRelease()
    {
        var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var enumerations = 0;
        var cache = Cache(_ => { entered.TrySetResult(true); return release.Task; }, () => { enumerations++; return Array.Empty<KeyValuePair<object, object>>(); });
        using var cancellation = new CancellationTokenSource();
        var task = ReadSet(cache).ToListAsync(cancellation.Token);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task.WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.False(release.Task.IsCompleted);
        }
        finally
        {
            release.TrySetResult(true);
            try { await task.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        }
        Assert.Equal(0, enumerations);
    }

    [Fact]
    public async Task MigrationRetryDelay_CancelsBeforeAnotherEnumeration()
    {
        using var cancellation = new CancellationTokenSource();
        var attempts = 0;
        var cache = Cache(_ => Task.CompletedTask, () =>
        {
            if (++attempts == 1) { cancellation.Cancel(); throw new InvalidOperationException("may have migrated to another instance"); }
            return Array.Empty<KeyValuePair<object, object>>();
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ReadSet(cache).ToListAsync(cancellation.Token).WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task CancellationWithMigrationText_IsNeverRetried()
    {
        var attempts = 0;
        var original = new OperationCanceledException("PENDING_SHUTDOWN");
        var cache = Cache(_ => Task.CompletedTask, () => { if (++attempts == 1) throw original; return Array.Empty<KeyValuePair<object, object>>(); });
        Assert.Same(original, await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ReadSet(cache).ToListAsync().WaitAsync(TimeSpan.FromSeconds(2))));
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task LegacyReflectionAndInterface_StillWork()
    {
        var cache = Cache(_ => Task.CompletedTask, () => new[] { new KeyValuePair<object, object>("a\0b\0123\0", 1) });
        Assert.NotNull(cache.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) }));
        Assert.Single(await ((ITableCache<Row>)cache).ToListAsync(new List<string> { "a", "b" }, null));
        Assert.Single(await ReadSet(cache).ToListAsync());
        Assert.Empty(await ReadSet(new LegacyCache()).ToListAsync());
    }

    [Fact]
    public async Task FilterComponentContainingNul_IsRejected()
    {
        var cache = Cache(_ => Task.CompletedTask, () => Array.Empty<KeyValuePair<object, object>>());
        await Assert.ThrowsAsync<ArgumentException>(() => cache.ToListAsync(new List<string> { "a\0b" }));
    }

    private sealed class LegacyCache : ITableCache<Row>
    {
        public Task<List<Row>> ToListAsync(List<string>? filter = null, TimeSpan? timeout = null) => Task.FromResult(new List<Row>());
        public void Dispose() { }
    }
    private sealed class BaseSet : EventSet<Row>
    {
        public BaseSet(IKsqlContext context, EntityModel model) : base(context, model) { }
        public override Task<List<Row>> ToListAsync(CancellationToken token = default) => Task.FromResult(new List<Row>());
        public override IAsyncEnumerator<Row> GetAsyncEnumerator(CancellationToken token = default) => throw new NotSupportedException();
        protected override Task SendEntityAsync(Row entity, Dictionary<string, string>? headers, CancellationToken token) => Task.CompletedTask;
    }
    public sealed class Row { }
}
