using Ksql.Linq.Configuration;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Application;

public class KsqlContextDeferredStartupTests
{
    private class Sample
    {
        public int Id { get; set; }
    }

    private class DeferredContext : KsqlContext
    {
        public DeferredContext() : base(new KsqlDslOptions { DeferStartup = true }) { }

        protected override bool SkipSchemaRegistration => true;
    }

    private class EagerContext : KsqlContext
    {
        public EagerContext() : base(new KsqlDslOptions()) { }

        protected override bool SkipSchemaRegistration => true;
    }

    [Fact]
    public void Set_BeforeStartAsync_Throws()
    {
        using var ctx = new DeferredContext();
        var ex = Assert.Throws<InvalidOperationException>(() => ctx.Set<Sample>());
        Assert.Contains("StartAsync", ex.Message);
    }

    [Fact]
    public void GetEventSet_BeforeStartAsync_Throws()
    {
        using var ctx = new DeferredContext();
        Assert.Throws<InvalidOperationException>(() => ctx.GetEventSet(typeof(Sample)));
    }

    [Fact]
    public async Task Set_AfterStartAsync_Succeeds()
    {
        using var ctx = new DeferredContext();
        await ctx.StartAsync();
        var set = ctx.Set<Sample>();
        Assert.NotNull(set);
    }

    [Fact]
    public async Task StartAsync_IsIdempotent()
    {
        using var ctx = new DeferredContext();
        await ctx.StartAsync();
        await ctx.StartAsync();
        Assert.NotNull(ctx.Set<Sample>());
    }

    [Fact]
    public async Task StartAsync_OnEagerContext_IsNoOp()
    {
        using var ctx = new EagerContext();
        // Startup already ran in the constructor; Set works immediately and
        // StartAsync stays callable without side effects.
        Assert.NotNull(ctx.Set<Sample>());
        await ctx.StartAsync();
        Assert.NotNull(ctx.Set<Sample>());
    }

    [Fact]
    public async Task StartAsync_Cancelled_LeavesContextUnstarted()
    {
        using var ctx = new DeferredContext();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ctx.StartAsync(cts.Token));
        Assert.Throws<InvalidOperationException>(() => ctx.Set<Sample>());
    }
}
