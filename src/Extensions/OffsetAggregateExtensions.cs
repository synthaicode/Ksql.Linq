using Ksql.Linq.Window;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace Ksql.Linq
{
    /// <summary>
    /// Extension methods for KSQL offset aggregate functions.
    /// These are not used at runtime and exist only for LINQ expression analysis.
    /// </summary>
    public static class OffsetAggregateExtensions
    {
        public static TResult LatestByOffset<TSource, TKey, TResult>(this IGrouping<TKey, TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            if (source is IWindowGrouping<TKey, TSource> windowGrouping)
            {
                if (selector == null) throw new ArgumentNullException(nameof(selector));
                var func = selector.Compile();
                return windowGrouping.LatestByOffset(func);
            }

            throw new NotSupportedException("LatestByOffset is for expression translation only.");
        }

        public static TResult EarliestByOffset<TSource, TKey, TResult>(this IGrouping<TKey, TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            if (source is IWindowGrouping<TKey, TSource> windowGrouping)
            {
                if (selector == null) throw new ArgumentNullException(nameof(selector));
                var func = selector.Compile();
                return windowGrouping.EarliestByOffset(func);
            }

            throw new NotSupportedException("EarliestByOffset is for expression translation only.");
        }
    }
}
