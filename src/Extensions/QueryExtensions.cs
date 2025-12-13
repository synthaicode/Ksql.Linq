using Ksql.Linq.Query.Dsl;
using System;

namespace Ksql.Linq;

/// <summary>
/// Fluent helpers for building queries from EventSet instances.
/// </summary>
public static class QueryExtensions
{
    public static EventSet<T> ToQuery<T>(this EventSet<T> set, Func<KsqlQueryRoot, IKsqlQueryable> build)
        where T : class
    {
        if (set == null) throw new ArgumentNullException(nameof(set));
        if (build == null) throw new ArgumentNullException(nameof(build));

        var root = new KsqlQueryRoot();
        var query = build(root) ?? throw new InvalidOperationException("Query builder returned null");
        var model = query.Build();

        if (model.SelectProjection != null && model.SelectProjection.ReturnType != typeof(T))
        {
            throw new InvalidOperationException("Select projection type must match EventSet type.");
        }

        ToQueryValidator.ValidateHoppingPipeline(typeof(T), model);
        ToQueryValidator.ValidateSelectMatchesPoco(typeof(T), model);

        var entityModel = set.GetEntityModel();
        entityModel.QueryModel = model;
        return set;
    }
}
