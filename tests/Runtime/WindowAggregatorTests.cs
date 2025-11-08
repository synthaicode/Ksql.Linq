using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Window;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ksql.Linq.Tests.Runtime;

public class WindowAggregatorTests
{
    private sealed record TestMessage(string Broker, string Symbol, DateTime Timestamp, decimal Price);

    private sealed class TestClock
    {
        private DateTime _utcNow;

        public TestClock(DateTime initialUtc)
        {
            _utcNow = initialUtc;
        }

        public DateTime UtcNow => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);

        public void Set(DateTime instant) => _utcNow = instant;
    }

    private sealed class CaptureSink
    {
        private readonly List<WindowResult> _results = new();

        public IReadOnlyList<WindowResult> Results => _results;

        public ValueTask EmitAsync(WindowResult result, CancellationToken token)
        {
            _results.Add(result);
            return ValueTask.CompletedTask;
        }
    }

    private sealed record WindowResult(string Key, DateTime BucketStartUtc, decimal Open, decimal High, decimal Low, decimal Close);

    private static WindowAggregator<TestMessage, string, WindowResult> CreateAggregator(
        TestClock clock,
        CaptureSink sink,
        WindowAggregatorMetrics? metrics = null,
        Func<WindowResult, Exception, CancellationToken, ValueTask>? failureHandler = null,
        Func<WindowResult, CancellationToken, ValueTask>? emitOverride = null,
        Func<TestMessage, object?>? deduplicationKeySelector = null)
    {
        metrics ??= new WindowAggregatorMetrics();

        ValueTask Emit(WindowResult result, CancellationToken token)
            => emitOverride is null ? sink.EmitAsync(result, token) : emitOverride(result, token);

        return new WindowAggregator<TestMessage, string, WindowResult>(
            windowSize: TimeSpan.FromMinutes(1),
            gracePeriod: TimeSpan.FromSeconds(5),
            sweepInterval: TimeSpan.FromSeconds(1),
            idleThreshold: TimeSpan.FromMinutes(5),
            keySelector: msg => $"{msg.Broker}|{msg.Symbol}",
            timestampSelector: msg => msg.Timestamp,
            resultSelector: grouping => new WindowResult(
                grouping.Key,
                grouping.WindowStart,
                grouping.EarliestByOffset(m => m.Price),
                grouping.Max(m => m.Price),
                grouping.Min(m => m.Price),
                grouping.LatestByOffset(m => m.Price)),
            emitCallback: Emit,
            emitFailureHandler: failureHandler,
            messageValidator: null,
            metrics: metrics,
            logger: NullLogger.Instance,
            utcNowProvider: () => clock.UtcNow,
            deduplicationKeySelector: deduplicationKeySelector);
    }

    [Fact]
    public async Task Emits_only_after_grace_boundary()
    {
        var clock = new TestClock(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc));
        var sink = new CaptureSink();
        var aggregator = CreateAggregator(clock, sink);

        var timestamp = clock.UtcNow.AddSeconds(5);
        clock.Set(timestamp);
        aggregator.ProcessMessage(new TestMessage("B", "S", timestamp, 101m));

        clock.Set(timestamp.AddSeconds(55));
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Empty(sink.Results);

        clock.Advance(TimeSpan.FromSeconds(10));
        await aggregator.SweepOnceAsync(CancellationToken.None);
        Assert.Single(sink.Results);
        Assert.Equal(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc), sink.Results[0].BucketStartUtc);
    }

    [Fact]
    public async Task Emit_failure_without_handler_propagates_after_retry_exhaustion()
    {
        var clock = new TestClock(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc));
        var sink = new CaptureSink();
        var metrics = new WindowAggregatorMetrics();
        var attempts = 0;

        ValueTask AlwaysFail(WindowResult _, CancellationToken __)
        {
            attempts++;
            return ValueTask.FromException(new InvalidOperationException("fail"));
        }

        var aggregator = CreateAggregator(clock, sink, metrics, emitOverride: AlwaysFail);

        var timestamp = clock.UtcNow.AddSeconds(5);
        clock.Set(timestamp);
        aggregator.ProcessMessage(new TestMessage("B", "S", timestamp, 101m));
        clock.Set(timestamp.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));

        var exception = await Assert.ThrowsAsync<WindowAggregationException>(async () =>
            await aggregator.SweepOnceAsync(CancellationToken.None));

        Assert.Equal(3, attempts);
        Assert.Equal(3, metrics.EmitFailures);
        Assert.Equal("B|S", exception.Key);
        Assert.Equal(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc), exception.BucketStartUtc);
    }

    [Fact]
    public async Task Flush_clears_managers_and_accepts_follow_up_messages()
    {
        var clock = new TestClock(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc));
        var sink = new CaptureSink();
        var aggregator = CreateAggregator(clock, sink);

        var firstTimestamp = clock.UtcNow.AddSeconds(5);
        clock.Set(firstTimestamp);
        aggregator.ProcessMessage(new TestMessage("B", "S", firstTimestamp, 100m));
        clock.Set(firstTimestamp.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.FlushAsync(CancellationToken.None);

        Assert.Equal(0, aggregator.ManagerCount);
        Assert.Single(sink.Results);

        var followUpTimestamp = clock.UtcNow.AddMinutes(1).AddSeconds(5);
        clock.Set(followUpTimestamp);
        aggregator.ProcessMessage(new TestMessage("B", "S", followUpTimestamp, 140m));
        clock.Set(followUpTimestamp.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.FlushAsync(CancellationToken.None);

        Assert.Equal(2, sink.Results.Count);
        Assert.Equal(0, aggregator.ManagerCount);
    }

    [Fact]
    public async Task Metrics_persist_across_flush_for_duplicate_and_late_drops()
    {
        var clock = new TestClock(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc));
        var sink = new CaptureSink();
        var metrics = new WindowAggregatorMetrics();
        var aggregator = CreateAggregator(clock, sink, metrics, deduplicationKeySelector: m => $"{m.Timestamp:o}-{m.Price}");

        var firstTick = new TestMessage("B", "S", clock.UtcNow.AddSeconds(5), 101m);
        clock.Set(firstTick.Timestamp);
        aggregator.ProcessMessage(firstTick);
        aggregator.ProcessMessage(new TestMessage(firstTick.Broker, firstTick.Symbol, firstTick.Timestamp, firstTick.Price));

        Assert.Equal(1, metrics.DuplicateDrops);

        clock.Set(firstTick.Timestamp.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.SweepOnceAsync(CancellationToken.None);

        aggregator.ProcessMessage(new TestMessage(firstTick.Broker, firstTick.Symbol, firstTick.Timestamp, firstTick.Price));
        Assert.Equal(1, metrics.LateDrops);

        clock.Set(clock.UtcNow.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.FlushAsync(CancellationToken.None);

        Assert.Equal(1, metrics.DuplicateDrops);
        Assert.Equal(1, metrics.LateDrops);
    }

    [Fact]
    public async Task Sub_millisecond_events_floor_to_expected_window()
    {
        var baseStart = new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc);
        var clock = new TestClock(baseStart);
        var sink = new CaptureSink();
        var aggregator = CreateAggregator(clock, sink);

        var timestamp = baseStart.AddTicks(5_000_001);
        clock.Set(timestamp);
        aggregator.ProcessMessage(new TestMessage("B", "S", timestamp, 100m));

        clock.Set(timestamp.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.SweepOnceAsync(CancellationToken.None);

        Assert.Single(sink.Results);
        Assert.Equal(baseStart, sink.Results[0].BucketStartUtc);
    }

    [Fact]
    public async Task Boundary_events_split_into_adjacent_windows()
    {
        var baseStart = new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc);
        var clock = new TestClock(baseStart);
        var sink = new CaptureSink();
        var aggregator = CreateAggregator(clock, sink);

        var lastTickInFirstWindow = baseStart.AddMinutes(1).AddTicks(-1);
        clock.Set(lastTickInFirstWindow);
        aggregator.ProcessMessage(new TestMessage("B", "S", lastTickInFirstWindow, 100m));

        var firstTickNextWindow = baseStart.AddMinutes(1);
        clock.Set(firstTickNextWindow);
        aggregator.ProcessMessage(new TestMessage("B", "S", firstTickNextWindow, 110m));

        clock.Set(firstTickNextWindow.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.SweepOnceAsync(CancellationToken.None);

        Assert.Equal(2, sink.Results.Count);
        Assert.Equal(baseStart, sink.Results[0].BucketStartUtc);
        Assert.Equal(baseStart.AddMinutes(1), sink.Results[1].BucketStartUtc);
    }

    [Fact]
    public async Task Mixed_timestamp_kinds_align_to_same_window()
    {
        var clock = new TestClock(new DateTime(2025, 10, 10, 0, 0, 0, DateTimeKind.Utc));
        var sink = new CaptureSink();
        var aggregator = CreateAggregator(clock, sink);

        var windowStart = clock.UtcNow.AddSeconds(5);
        clock.Set(windowStart);
        aggregator.ProcessMessage(new TestMessage("B", "S", DateTime.SpecifyKind(windowStart, DateTimeKind.Local), 100m));
        var second = DateTime.SpecifyKind(windowStart.AddSeconds(10), DateTimeKind.Unspecified);
        clock.Set(second);
        aggregator.ProcessMessage(new TestMessage("B", "S", second, 120m));

        clock.Set(windowStart.AddMinutes(1));
        clock.Advance(TimeSpan.FromSeconds(5));
        await aggregator.SweepOnceAsync(CancellationToken.None);

        Assert.Single(sink.Results);
        Assert.Equal(100m, sink.Results[0].Open);
        Assert.Equal(120m, sink.Results[0].Close);
    }
}