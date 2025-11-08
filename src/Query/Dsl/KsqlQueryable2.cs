using System;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Visitors;

namespace Ksql.Linq.Query.Dsl;

public class KsqlQueryable2<T1, T2> : IKsqlQueryable
{
    private readonly KsqlQueryModel _model;
    private QueryBuildStage _stage = QueryBuildStage.Join;

    public KsqlQueryable2()
    {
        _model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(T1), typeof(T2) },
            PrimarySourceRequiresAlias = true
        };
    }

    internal KsqlQueryable2(KsqlQueryModel model)
    {
        _model = model;
    }

    public KsqlQueryable2<T1, T2> Where(Expression<Func<T1, T2, bool>> predicate)
    {
        if (_stage is QueryBuildStage.Select or QueryBuildStage.GroupBy or QueryBuildStage.Having)
            throw new InvalidOperationException("Where() must be called before GroupBy/Having/Select().");

        _model.WhereCondition = predicate;
        _stage = QueryBuildStage.Where;
        return this;
    }

    public KsqlQueryable2<T1, T2> Select<TResult>(Expression<Func<T1, T2, TResult>> projection)
    {
        if (_stage == QueryBuildStage.Select)
            throw new InvalidOperationException("Select() has already been specified.");

        if (_stage is QueryBuildStage.Join or QueryBuildStage.From or QueryBuildStage.Where or QueryBuildStage.GroupBy or QueryBuildStage.Having)
        {
            _stage = QueryBuildStage.Select;
        }
        else
        {
            throw new InvalidOperationException("Select() cannot be called in the current state.");
        }
        _model.SelectProjection = projection;

        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(projection.Body);

        return this;
    }

    public KsqlQueryable2<T1, T2> Tumbling(
        Expression<Func<T1, T2, DateTime>> time,
        Windows windows,
        int baseUnitSeconds = 10,
        TimeSpan? grace = null,
        bool continuation = false)
    {
        _model.Extras["HasTumblingWindow"] = true;
        _model.Continuation = continuation;
        _model.Extras["continuation"] = continuation;
        if (time.Body is MemberExpression me)
            _model.TimeKey = me.Member.Name;
        else if (time.Body is UnaryExpression ue && ue.Operand is MemberExpression me2)
            _model.TimeKey = me2.Member.Name;
        if (windows.Minutes != null) foreach (var m in windows.Minutes) _model.Windows.Add($"{m}m");
        if (windows.Hours != null) foreach (var h in windows.Hours) _model.Windows.Add($"{h}h");
        if (windows.Days != null) foreach (var d in windows.Days) _model.Windows.Add($"{d}d");
        if (windows.Months != null) foreach (var mo in windows.Months) _model.Windows.Add($"{mo}mo");
        _model.BaseUnitSeconds = baseUnitSeconds;
        if (grace.HasValue)
            _model.GraceSeconds = (int)Math.Ceiling(grace.Value.TotalSeconds);
        _model.NormalizeWindowsInPlace();
        return this;
    }

    public KsqlQueryable2<T1, T2> Within(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval), "interval must be > 0");
        var seconds = (int)Math.Ceiling(interval.TotalSeconds);
        _model.WithinSeconds = seconds;
        return this;
    }

    public KsqlQueryable2<T1, T2> RequireExplicitWithin()
    {
        _model.ForbidDefaultWithin = true;
        return this;
    }

    public KsqlQueryable2<T1, T2> Tumbling(Expression<Func<T1, T2, object>> timeProperty, TimeSpan size)
    {
        throw new NotSupportedException("Legacy Tumbling overload is not supported in this phase.");
    }

    public KsqlQueryModel Build()
    {
        _model.PrimarySourceRequiresAlias = PrimarySourceAliasDecider.Determine(_model);
        return _model;
    }

}