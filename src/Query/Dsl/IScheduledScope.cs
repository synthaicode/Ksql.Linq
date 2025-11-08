using System;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Dsl;

public interface IScheduledScope<T>
{
    KsqlQueryable<T> Tumbling(
        Expression<Func<T, DateTime>> time,
        Windows windows,
        int baseUnitSeconds = 10,
        TimeSpan? grace = null,
        bool continuation = false);
}