using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Window;

public sealed class WindowAggregator<TSource, TKey, TResult> : IAsyncDisposable
    where TKey : notnull
{
    private const int MaxEmitAttempts = 3;

    private readonly TimeSpan _windowSize;
    private readonly TimeSpan _gracePeriod;
    private readonly TimeSpan _sweepInterval;
    private readonly TimeSpan _idleThreshold;
    private readonly Func<TSource, TKey> _keySelector;
    private readonly Func<TSource, DateTime> _timestampSelector;
    private readonly Func<IWindowGrouping<TKey, TSource>, TResult> _resultSelector;
    private readonly Func<TResult, CancellationToken, ValueTask> _emitCallback;
    private readonly Func<TResult, Exception, CancellationToken, ValueTask>? _emitFailureHandler;
    private readonly Action<TSource>? _messageValidator;
    private readonly Func<DateTime> _utcNowProvider;
    private readonly WindowAggregatorMetrics? _metrics;
    private readonly ILogger? _logger;
    private readonly Func<TSource, object?>? _deduplicationKeySelector;

    private readonly ConcurrentDictionary<TKey, WindowManager<TSource, TKey>> _managers = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly object _startSync = new();

    private Task? _loopTask;
    private bool _started;

    public WindowAggregator(
        TimeSpan windowSize,
        TimeSpan gracePeriod,
        TimeSpan sweepInterval,
        TimeSpan idleThreshold,
        Func<TSource, TKey> keySelector,
        Func<TSource, DateTime> timestampSelector,
        Func<IWindowGrouping<TKey, TSource>, TResult> resultSelector,
        Func<TResult, CancellationToken, ValueTask> emitCallback,
        Func<TResult, Exception, CancellationToken, ValueTask>? emitFailureHandler = null,
        Action<TSource>? messageValidator = null,
        WindowAggregatorMetrics? metrics = null,
        ILogger? logger = null,
        Func<DateTime>? utcNowProvider = null,
        Func<TSource, object?>? deduplicationKeySelector = null)
    {
        if (windowSize <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(windowSize));
        if (gracePeriod < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(gracePeriod));
        if (sweepInterval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(sweepInterval));
        if (idleThreshold < sweepInterval) throw new ArgumentOutOfRangeException(nameof(idleThreshold));

        _windowSize = windowSize;
        _gracePeriod = gracePeriod;
        _sweepInterval = sweepInterval;
        _idleThreshold = idleThreshold;
        _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
        _timestampSelector = timestampSelector ?? throw new ArgumentNullException(nameof(timestampSelector));
        _resultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
        _emitCallback = emitCallback ?? throw new ArgumentNullException(nameof(emitCallback));
        _emitFailureHandler = emitFailureHandler;
        _messageValidator = messageValidator;
        _metrics = metrics;
        _logger = logger;
        _utcNowProvider = utcNowProvider ?? (() => DateTime.UtcNow);
        _deduplicationKeySelector = deduplicationKeySelector;
    }

    public void Start()
    {
        lock (_startSync)
        {
            if (_started)
            {
                return;
            }

            _started = true;
            _loopTask = Task.Run(() => SweepLoopAsync(_cts.Token), _cts.Token);
        }
    }

    // Stop sweep loop without flushing any pending windows, and clear internal state.
    // Used during partition revocation to avoid驥崎､・∝・ while ensuring quick convergence.
    public void StopWithoutFlushAndClear()
    {
        try { _cts.Cancel(); } catch { }
        try { _loopTask?.Wait(); } catch { }
        _managers.Clear();
    }

    public void ProcessMessage(TSource message)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));

        _messageValidator?.Invoke(message);

        var key = _keySelector(message);
        var timestampUtc = NormalizeTimestamp(() => _timestampSelector(message));
        var nowUtc = _utcNowProvider();
        var windowStart = WindowingMath.FloorToWindow(timestampUtc, _windowSize);

        var manager = _managers.GetOrAdd(key, static (k, state) =>
            new WindowManager<TSource, TKey>(k, state.WindowSize, state.GracePeriod, state.InitialNowUtc, state.DeduplicationKeySelector),
            (WindowSize: _windowSize, GracePeriod: _gracePeriod, InitialNowUtc: nowUtc, DeduplicationKeySelector: _deduplicationKeySelector));

        var status = manager.AddMessage(windowStart, message, nowUtc);
        if (status == WindowAppendStatus.LateDrop)
        {
            _metrics?.RecordLateDrop();
            _logger?.LogWarning("Late arrival discarded for key {Key} at bucket {BucketStartUtc}", key, windowStart);
        }
        else if (status == WindowAppendStatus.Duplicate)
        {
            _metrics?.RecordDuplicateDrop();
            _logger?.LogDebug("Duplicate tick ignored for key {Key} at bucket {BucketStartUtc}", key, windowStart);
        }
    }

    public async ValueTask<int> FlushAsync(CancellationToken cancellationToken)
    {
        var emitted = 0;
        var nowUtc = _utcNowProvider();

        foreach (var kvp in _managers.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();
            emitted += await EmitClosedWindowsAsync(kvp.Value, nowUtc, cancellationToken).ConfigureAwait(false);

            if (_managers.TryRemove(kvp.Key, out _))
            {
                _logger?.LogDebug("Released window manager for key {Key} during flush", kvp.Key);
            }
        }

        return emitted;
    }
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        try
        {
            await FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "WindowAggregator flush failed during disposal.");
        }

        _cts.Dispose();
    }

    private async Task SweepLoopAsync(CancellationToken token)
    {
        var timer = new PeriodicTimer(_sweepInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                try
                {
                    await EmitSweepAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "WindowAggregator sweep iteration failed.");
                }
            }
        }
        finally
        {
            timer.Dispose();
        }
    }

    private async ValueTask EmitSweepAsync(CancellationToken token)
    {
        var nowUtc = _utcNowProvider();

        foreach (var kvp in _managers.ToArray())
        {
            token.ThrowIfCancellationRequested();
            var manager = kvp.Value;
            await EmitClosedWindowsAsync(manager, nowUtc, token).ConfigureAwait(false);

            if (manager.IsIdle(_idleThreshold, nowUtc) && _managers.TryRemove(kvp.Key, out _))
            {
                _logger?.LogDebug("Evicted idle window manager for key {Key}", kvp.Key);
            }
        }
    }

    private async ValueTask<int> EmitClosedWindowsAsync(WindowManager<TSource, TKey> manager, DateTime nowUtc, CancellationToken token)
    {
        var closed = manager.CollectClosedWindows(nowUtc);
        if (closed.Count == 0)
        {
            return 0;
        }

        var emitted = 0;
        foreach (var grouping in closed)
        {
            token.ThrowIfCancellationRequested();
            var result = _resultSelector(grouping);
            try
            {
                await EmitWithRetryAsync(result, grouping.Key, grouping.WindowStart, token).ConfigureAwait(false);
                emitted++;
            }
            catch (WindowAggregationException ex)
            {
                _logger?.LogError(ex, "Failed to emit window for key {Key} at {BucketStartUtc}", grouping.Key, grouping.WindowStart);
                throw;
            }        }

        return emitted;
    }

    private async ValueTask EmitWithRetryAsync(TResult result, object key, DateTime bucketStartUtc, CancellationToken token)
    {
        Exception? lastError = null;

        for (var attempt = 1; attempt <= MaxEmitAttempts; attempt++)
        {
            try
            {
                await _emitCallback(result, token).ConfigureAwait(false);
                _metrics?.RecordEmitSuccess();
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastError = ex;
                _metrics?.RecordEmitFailure();
                _logger?.LogWarning(ex, "Emit attempt {Attempt} failed for key {Key} at {BucketStartUtc}", attempt, key, bucketStartUtc);

                if (attempt < MaxEmitAttempts)
                {
                    var delay = TimeSpan.FromMilliseconds(Math.Pow(2, attempt) * 100);
                    await Task.Delay(delay, token).ConfigureAwait(false);
                    continue;
                }

                break;
            }
        }

        if (lastError is not null && _emitFailureHandler is not null)
        {
            await _emitFailureHandler(result, lastError, token).ConfigureAwait(false);
            return;
        }

        throw new WindowAggregationException(
            "Failed to emit window result after retries.",
            key,
            bucketStartUtc,
            lastError);
    }

    internal ValueTask SweepOnceAsync(CancellationToken token) => EmitSweepAsync(token);

    internal int ManagerCount => _managers.Count;


    private DateTime NormalizeTimestamp(Func<DateTime> timestampFactory)
    {
        try
        {
            var timestamp = timestampFactory();
            if (timestamp.Kind == DateTimeKind.Utc)
            {
                return timestamp;
            }

            if (timestamp.Kind == DateTimeKind.Unspecified)
            {
                timestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Local);
            }

            return timestamp.ToUniversalTime();
        }
        catch (Exception ex)
        {
            throw new WindowAggregationException("Failed to normalize timestamp.", innerException: ex);
        }
    }
}











