using Ksql.Linq.Window;
using System;
using System.Linq;

namespace Ksql.Linq
{
    public static class WindowExtensions
    {
        public static DateTime WindowStart<TSource, TKey>(this IGrouping<TKey, TSource> source)
        {
            if (source is IWindowGrouping<TKey, TSource> windowGrouping)
            {
                return windowGrouping.WindowStart;
            }

            throw new NotSupportedException("WindowStart is for expression translation only.");
        }
    }
}
