using System;
using System.Collections.Generic;

namespace Ksql.Linq.Window;

public interface IWindowGrouping<TKey, TElement> : IEnumerable<TElement>
{
    TKey Key { get; }
    DateTime WindowStart { get; }
    DateTime WindowEnd { get; }

    TResult Max<TResult>(Func<TElement, TResult> selector) where TResult : IComparable<TResult>;
    TResult Min<TResult>(Func<TElement, TResult> selector) where TResult : IComparable<TResult>;
    TResult Sum<TResult>(Func<TElement, TResult> selector) where TResult : struct;
    double Average(Func<TElement, double> selector);
    int Count();
    TResult EarliestByOffset<TResult>(Func<TElement, TResult> selector);
    TResult LatestByOffset<TResult>(Func<TElement, TResult> selector);
}