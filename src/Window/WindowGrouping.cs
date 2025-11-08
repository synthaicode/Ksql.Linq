using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Ksql.Linq.Window;

internal sealed class WindowGrouping<TKey, TElement> : IWindowGrouping<TKey, TElement>, System.Linq.IGrouping<TKey, TElement>
{
    private readonly IReadOnlyList<TElement> _messages;

    public WindowGrouping(TKey key, DateTime start, DateTime end, IReadOnlyList<TElement> messages)
    {
        Key = key;
        WindowStart = start;
        WindowEnd = end;
        _messages = messages ?? throw new ArgumentNullException(nameof(messages));
    }

    public TKey Key { get; }
    public DateTime WindowStart { get; }
    public DateTime WindowEnd { get; }

    public TResult Max<TResult>(Func<TElement, TResult> selector) where TResult : notnull, IComparable<TResult>
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));
        if (_messages == null || _messages.Count == 0) throw new InvalidOperationException("Max requires at least one element.");
        return _messages.Max(selector)!;
    }

    public TResult Min<TResult>(Func<TElement, TResult> selector) where TResult : notnull, IComparable<TResult>
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));
        if (_messages == null || _messages.Count == 0) throw new InvalidOperationException("Min requires at least one element.");
        return _messages.Min(selector)!;
    }

    public TResult Sum<TResult>(Func<TElement, TResult> selector) where TResult : struct
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));

        dynamic sum = default(TResult);
        foreach (var item in _messages)
        {
            sum += (dynamic)selector(item)!;
        }

        return sum;
    }

    public double Average(Func<TElement, double> selector)
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));
        return _messages.Average(selector);
    }

    public int Count() => _messages.Count;

    public TResult EarliestByOffset<TResult>(Func<TElement, TResult> selector)
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));
        return selector(_messages[0]);
    }

    public TResult LatestByOffset<TResult>(Func<TElement, TResult> selector)
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));
        return selector(_messages[_messages.Count - 1]);
    }

    public IEnumerator<TElement> GetEnumerator() => _messages.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
