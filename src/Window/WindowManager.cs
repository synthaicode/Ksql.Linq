using System;
using System.Collections.Generic;
using System.Linq;

namespace Ksql.Linq.Window;

internal sealed class WindowManager<TSource, TKey>
{
    private readonly TKey _key;
    private readonly TimeSpan _windowSize;
    private readonly TimeSpan _gracePeriod;
    private readonly Func<TSource, object?>? _deduplicationKeySelector;
    private readonly object _sync = new();
    private readonly Dictionary<DateTime, WindowBucket> _openWindows = new();
    private readonly HashSet<DateTime> _sealedWindows = new();
    private readonly Queue<DateTime> _sealedOrder = new();
    private readonly int _sealedRetention;

    private DateTime _lastActivityUtc;

    public WindowManager(
        TKey key,
        TimeSpan windowSize,
        TimeSpan gracePeriod,
        DateTime initialUtc,
        Func<TSource, object?>? deduplicationKeySelector)
    {
        _key = key;
        _windowSize = windowSize;
        _gracePeriod = gracePeriod;
        _deduplicationKeySelector = deduplicationKeySelector;
        _lastActivityUtc = initialUtc;
        _sealedRetention = 512;
    }

    public WindowAppendStatus AddMessage(DateTime windowStartUtc, TSource message, DateTime nowUtc)
    {
        lock (_sync)
        {
            if (_sealedWindows.Contains(windowStartUtc))
            {
                return WindowAppendStatus.LateDrop;
            }

            if (!_openWindows.TryGetValue(windowStartUtc, out var bucket))
            {
                bucket = new WindowBucket();
                _openWindows[windowStartUtc] = bucket;
            }

            if (_deduplicationKeySelector is not null)
            {
                var key = _deduplicationKeySelector(message);
                if (!bucket.TryAddKey(key))
                {
                    return WindowAppendStatus.Duplicate;
                }
            }

            bucket.Messages.Add(message);
            _lastActivityUtc = nowUtc;
            return WindowAppendStatus.Appended;
        }
    }

    public IReadOnlyList<WindowGrouping<TKey, TSource>> CollectClosedWindows(DateTime nowUtc)
    {
        List<(DateTime WindowStart, WindowBucket Bucket)>? closed = null;

        lock (_sync)
        {
            foreach (var kvp in _openWindows.ToArray())
            {
                var windowEnd = kvp.Key + _windowSize;
                if (nowUtc >= windowEnd + _gracePeriod && kvp.Value.Messages.Count > 0)
                {
                    closed ??= new List<(DateTime, WindowBucket)>();
                    closed.Add((kvp.Key, kvp.Value));
                    _openWindows.Remove(kvp.Key);
                    SealWindow(kvp.Key);
                }
            }
        }

        if (closed is null)
        {
            return Array.Empty<WindowGrouping<TKey, TSource>>();
        }

        return closed.Select(tuple =>
                new WindowGrouping<TKey, TSource>(
                    _key,
                    tuple.WindowStart,
                    tuple.WindowStart + _windowSize,
                    tuple.Bucket.Messages))
            .ToArray();
    }

    public bool IsIdle(TimeSpan threshold, DateTime nowUtc) =>
        nowUtc - _lastActivityUtc >= threshold;

    private void SealWindow(DateTime windowStartUtc)
    {
        _sealedWindows.Add(windowStartUtc);
        _sealedOrder.Enqueue(windowStartUtc);

        while (_sealedOrder.Count > _sealedRetention)
        {
            var oldest = _sealedOrder.Dequeue();
            _sealedWindows.Remove(oldest);
        }
    }

    private sealed class WindowBucket
    {
        public List<TSource> Messages { get; } = new();
        private HashSet<object?>? _keys;

        public bool TryAddKey(object? key)
        {
            _keys ??= new HashSet<object?>();
            return _keys.Add(key);
        }
    }
}
