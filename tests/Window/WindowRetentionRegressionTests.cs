using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Window;
using Xunit;

namespace Ksql.Linq.Tests.Window;

[Trait("Level", "L3")]
public class WindowRetentionRegressionTests
{
    private static readonly DateTime Epoch = new(2026, 11, 6, 0, 0, 0, DateTimeKind.Utc);
    private sealed record Message(DateTime Timestamp, int Id);
    private sealed class Clock { public DateTime Now = Epoch; }

    private static WindowAggregator<Message, string, int> Create(Clock clock,
        Func<int, CancellationToken, ValueTask> emit,
        Func<IWindowGrouping<string, Message>, int>? select = null,
        Func<int, Exception, CancellationToken, ValueTask>? failure = null,
        WindowAggregatorMetrics? metrics = null) => new(
            TimeSpan.FromMinutes(1), TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5),
            _ => "key", m => m.Timestamp, select ?? (g => g.Single().Id), emit,
            emitFailureHandler: failure, metrics: metrics, utcNowProvider: () => clock.Now,
            deduplicationKeySelector: m => m.Id);

    private static void Seed(WindowAggregator<Message, string, int> aggregator, Clock clock)
    {
        aggregator.ProcessMessage(new Message(Epoch, 1));
        aggregator.ProcessMessage(new Message(Epoch.AddMinutes(1), 2));
        clock.Now = Epoch.AddMinutes(3);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Exhausted_callback_retains_unattempted_bucket(bool flush)
    {
        var clock = new Clock();
        var attempts = new List<int>();
        var fail = true;
        var aggregator = Create(clock, (id, _) =>
        {
            attempts.Add(id);
            return fail ? ValueTask.FromException(new InvalidOperationException("emit")) : ValueTask.CompletedTask;
        });
        Seed(aggregator, clock);
        await Assert.ThrowsAsync<WindowAggregationException>(async () =>
        {
            if (flush) await aggregator.FlushAsync(CancellationToken.None);
            else await aggregator.SweepOnceAsync(CancellationToken.None);
        });
        Assert.Equal(new[] { 1, 1, 1 }, attempts);
        fail = false;
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(new[] { 1, 1, 1, 2 }, attempts);
    }

    [Fact]
    public async Task Selector_failure_retains_unattempted_bucket()
    {
        var clock = new Clock();
        var selected = new List<int>();
        var emitted = new List<int>();
        var fail = true;
        var aggregator = Create(clock, (id, _) => { emitted.Add(id); return ValueTask.CompletedTask; }, g =>
        {
            var id = g.Single().Id;
            selected.Add(id);
            if (fail) throw new InvalidOperationException("selector");
            return id;
        });
        Seed(aggregator, clock);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await aggregator.SweepOnceAsync(CancellationToken.None));
        Assert.Equal(new[] { 1 }, selected);
        Assert.Empty(emitted);
        fail = false;
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(new[] { 1, 2 }, selected);
        Assert.Equal(new[] { 2 }, emitted);
    }

    [Fact]
    public async Task Cancellation_after_first_success_retains_next_bucket()
    {
        var clock = new Clock();
        using var cts = new CancellationTokenSource();
        var emitted = new List<int>();
        var aggregator = Create(clock, (id, _) =>
        {
            emitted.Add(id);
            if (id == 1) cts.Cancel();
            return ValueTask.CompletedTask;
        });
        Seed(aggregator, clock);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await aggregator.SweepOnceAsync(cts.Token));
        Assert.Equal(new[] { 1 }, emitted);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(new[] { 1, 2 }, emitted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Success_and_handled_failure_continue_to_next_bucket(bool handledFailure)
    {
        var clock = new Clock();
        var attempts = new List<int>();
        var handled = new List<int>();
        var aggregator = Create(clock, (id, _) =>
        {
            attempts.Add(id);
            return handledFailure && id == 1 ? ValueTask.FromException(new InvalidOperationException("emit")) : ValueTask.CompletedTask;
        }, failure: (id, _, _) => { handled.Add(id); return ValueTask.CompletedTask; });
        Seed(aggregator, clock);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(handledFailure ? new[] { 1, 1, 1, 2 } : new[] { 1, 2 }, attempts);
        Assert.Equal(handledFailure ? new[] { 1 } : Array.Empty<int>(), handled);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(handledFailure ? 4 : 2, attempts.Count);
    }

    [Fact]
    public async Task Flush_retains_open_bucket_added_during_emission()
    {
        var clock = new Clock();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var emitted = new List<int>();
        var aggregator = Create(clock, async (id, _) =>
        {
            emitted.Add(id);
            if (id == 1) { entered.SetResult(); await release.Task; }
        });
        aggregator.ProcessMessage(new Message(Epoch, 1));
        clock.Now = Epoch.AddMinutes(2);
        var flush = aggregator.FlushAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        aggregator.ProcessMessage(new Message(clock.Now, 2));
        release.SetResult();
        Assert.Equal(1, await flush.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, aggregator.ManagerCount);
        clock.Now = Epoch.AddMinutes(3);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(new[] { 1, 2 }, emitted);
    }

    [Fact]
    public async Task Duplicate_late_and_idle_cleanup_controls()
    {
        var clock = new Clock();
        var emitted = new List<int>();
        var metrics = new WindowAggregatorMetrics();
        var aggregator = Create(clock, (id, _) => { emitted.Add(id); return ValueTask.CompletedTask; }, metrics: metrics);
        var message = new Message(Epoch, 1);
        aggregator.ProcessMessage(message);
        aggregator.ProcessMessage(message);
        clock.Now = Epoch.AddMinutes(1);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        aggregator.ProcessMessage(message);
        Assert.Equal(1, metrics.DuplicateDrops);
        Assert.Equal(1, metrics.LateDrops);
        clock.Now = Epoch.AddMinutes(6);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(0, aggregator.ManagerCount);
        Assert.Equal(new[] { 1 }, emitted);
    }

    [Fact]
    public async Task Idle_cleanup_retains_future_pending_bucket()
    {
        var clock = new Clock();
        var emitted = new List<int>();
        var aggregator = Create(clock, (id, _) => { emitted.Add(id); return ValueTask.CompletedTask; });
        aggregator.ProcessMessage(new Message(Epoch.AddHours(1), 1));
        clock.Now = Epoch.AddMinutes(6);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(1, aggregator.ManagerCount);
        Assert.Empty(emitted);
        clock.Now = Epoch.AddHours(1).AddMinutes(1);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(new[] { 1 }, emitted);
    }

    [Fact]
    public async Task Concurrent_sweeps_take_each_bucket_once()
    {
        var clock = new Clock();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var emitted = new List<int>();
        var aggregator = Create(clock, async (id, _) =>
        {
            emitted.Add(id);
            if (id == 1) { entered.SetResult(); await release.Task; }
        });
        Seed(aggregator, clock);
        var first = aggregator.SweepOnceAsync(CancellationToken.None).AsTask();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await aggregator.SweepOnceAsync(CancellationToken.None);
        }
        finally { release.TrySetResult(); }
        await first.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { 1, 2 }, emitted);
    }

    private sealed class SchedulingKey
    {
        public Action? OnHash;
        public override int GetHashCode() { OnHash?.Invoke(); return 1; }
        public override bool Equals(object? obj) => ReferenceEquals(this, obj);
    }

    [Fact]
    public async Task Append_racing_idle_removal_remains_reachable()
    {
        var clock = new Clock();
        var key = new SchedulingKey();
        using var removing = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var appendStarted = new ManualResetEventSlim();
        var sweepThread = 0;
        var emitted = new List<int>();
        var aggregator = new WindowAggregator<Message, SchedulingKey, int>(
            TimeSpan.FromMinutes(1), TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5),
            _ => key, m => m.Timestamp, g => g.Single().Id,
            (id, _) => { emitted.Add(id); return ValueTask.CompletedTask; }, utcNowProvider: () => clock.Now);
        aggregator.ProcessMessage(new Message(Epoch, 1));
        clock.Now = Epoch.AddHours(1);
        // Dictionary removal hashes the key after the real manager idle check.
        key.OnHash = () =>
        {
            if (Environment.CurrentManagedThreadId != Volatile.Read(ref sweepThread)) return;
            removing.Set();
            if (!release.Wait(TimeSpan.FromSeconds(5))) throw new TimeoutException("idle removal release");
        };
        var sweep = Task.Run(async () =>
        {
            Volatile.Write(ref sweepThread, Environment.CurrentManagedThreadId);
            await aggregator.SweepOnceAsync(CancellationToken.None);
        });
        Task? append = null;
        try
        {
            Assert.True(removing.Wait(TimeSpan.FromSeconds(5)), "Sweep did not reach dictionary removal");
            append = Task.Run(() =>
            {
                appendStarted.Set();
                aggregator.ProcessMessage(new Message(clock.Now, 2));
            });
            Assert.True(appendStarted.Wait(TimeSpan.FromSeconds(5)));
            // Coordinated ownership may defer append until removal finishes.
            await Task.WhenAny(append, Task.Delay(250));
        }
        finally { release.Set(); }
        await sweep.WaitAsync(TimeSpan.FromSeconds(5));
        if (append is not null) await append.WaitAsync(TimeSpan.FromSeconds(5));
        key.OnHash = null;
        clock.Now = Epoch.AddHours(1).AddMinutes(1);
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(new[] { 1, 2 }, emitted);
    }
}
