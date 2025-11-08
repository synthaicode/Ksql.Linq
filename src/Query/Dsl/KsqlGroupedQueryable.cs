using System;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq.Query.Builders.Visitors;

namespace Ksql.Linq.Query.Dsl;

public class KsqlGroupedQueryable<T, TKey> : IKsqlQueryable
{
    private const string HavingNotSupportedMessage = "[KsqlLinq] HAVING is not supported." +
        "\nUse an emit-time predicate (pre-Kafka) or a downstream filter instead." +
        "\nSee: docs/amagiprotocol/window_aggregation_post_refactor_spec.md#limitations-havi";

    private readonly KsqlQueryModel _model;
    private readonly bool _hasTumbling;
    private QueryBuildStage _stage = QueryBuildStage.GroupBy;

    internal KsqlGroupedQueryable(KsqlQueryModel model)
    {
        _model = model;
        _hasTumbling = model.HasTumbling() || (model.Extras.TryGetValue("HasTumblingWindow", out var flag) && flag is true);
    }

    public KsqlGroupedQueryable<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));

        var hasTumbling = _hasTumbling || (_model.Extras.TryGetValue("HasTumblingWindow", out var flag) && flag is true);
        if (hasTumbling)
            throw new NotSupportedException(HavingNotSupportedMessage);

        if (_stage != QueryBuildStage.GroupBy)
            throw new InvalidOperationException("Having() must be called immediately after GroupBy().");

        _model.HavingCondition = predicate;
        _stage = QueryBuildStage.Having;
        return this;
    }

    public KsqlGroupedQueryable<T, TKey> Select<TResult>(Expression<Func<IGrouping<TKey, T>, TResult>> projection)
    {
        if (_stage is not (QueryBuildStage.GroupBy or QueryBuildStage.Having))
            throw new InvalidOperationException("Select() must be called after GroupBy() and optional Having().");

        _model.SelectProjection = projection;
        _stage = QueryBuildStage.Select;
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(projection.Body);
        var wsVisitor = new WindowStartDetectionVisitor();
        wsVisitor.Visit(projection.Body);
        _model.BucketColumnName = wsVisitor.ColumnName;
        return this;
    }

    // WhenEmpty has been removed. Use Tumbling(..., continuation: true) on the parent query to enable session-bound continuity.

    public KsqlQueryModel Build() => _model;
}

