using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Visitors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Dsl;

public class KsqlQueryModel
{
    public KsqlQueryModel()
    {
    }
    public Type[] SourceTypes { get; init; } = Array.Empty<Type>();
    public LambdaExpression? JoinCondition { get; set; }

    /// <summary>
    /// Condition expression for the WHERE clause.  
    /// </summary>
    public LambdaExpression? WhereCondition { get; set; }
    public LambdaExpression? SelectProjection { get; set; }
    public LambdaExpression? GroupByExpression { get; set; }
    public LambdaExpression? HavingCondition { get; set; }
    public Type? BasedOnType { get; set; }
    public LambdaExpression? BasedOnDayKey { get; set; }
    public List<string> BasedOnJoinKeys { get; } = new();
    public string? BasedOnOpen { get; set; }
    public string? BasedOnClose { get; set; }
    public bool BasedOnOpenInclusive { get; set; } = true;
    public bool BasedOnCloseInclusive { get; set; } = false;
    /// <summary>
    /// Get Window definitions (e.g., "1m","5m","1h","1d","1wk","1mo").
    /// </summary>
    public List<string> Windows
    {
        get { return _windows; }
    }
    private readonly List<string> _windows = new();
    public DayOfWeek WeekAnchor { get; init; } = DayOfWeek.Monday;

    public string? TimeKey { get; set; }
    public string? BucketColumnName { get; set; }
    public int? WithinSeconds { get; internal set; }
    public bool ForbidDefaultWithin { get; set; }
    public int? BaseUnitSeconds { get; set; }
    public int? GraceSeconds { get; set; }
    public bool Continuation { get; set; } = false;
    public bool PrimarySourceRequiresAlias { get; set; }
    public List<string> OperationSequence { get; } = new();
    public System.Collections.Generic.Dictionary<string, object?> Extras { get; } = new();
    public HoppingWindowSpec? Hopping { get; set; }
    internal ProjectionMetadata? SelectProjectionMetadata { get; set; }

    public KsqlQueryModel Clone()
    {
        var clone = new KsqlQueryModel
        {
            SourceTypes = (Type[])SourceTypes.Clone(),
            JoinCondition = JoinCondition,
            WhereCondition = WhereCondition,
            SelectProjection = SelectProjection,
            GroupByExpression = GroupByExpression,
            HavingCondition = HavingCondition,
            BasedOnType = BasedOnType,
            BasedOnDayKey = BasedOnDayKey,
            BasedOnOpen = BasedOnOpen,
            BasedOnClose = BasedOnClose,
            BasedOnOpenInclusive = BasedOnOpenInclusive,
            BasedOnCloseInclusive = BasedOnCloseInclusive,
            WeekAnchor = WeekAnchor,
            TimeKey = TimeKey,
            BucketColumnName = BucketColumnName,
            WithinSeconds = WithinSeconds,
            ForbidDefaultWithin = ForbidDefaultWithin,
            BaseUnitSeconds = BaseUnitSeconds,
            GraceSeconds = GraceSeconds,
            Continuation = Continuation,
            PrimarySourceRequiresAlias = PrimarySourceRequiresAlias
        };
        clone.Windows.AddRange(Windows);
        clone.BasedOnJoinKeys.AddRange(BasedOnJoinKeys);
        clone.OperationSequence.AddRange(OperationSequence);
        foreach (var kv in Extras)
            clone.Extras[kv.Key] = kv.Value;
        clone.SelectProjectionMetadata = SelectProjectionMetadata;
        if (Hopping != null)
        {
            clone.Hopping = new HoppingWindowSpec
            {
                Size = Hopping.Size,
                Advance = Hopping.Advance,
                Grace = Hopping.Grace
            };
        }
        return clone;
    }

    /// <summary>
    /// Returns a simple string representation useful for debugging.
    /// </summary>
    public string Dump()
    {
        var sources = string.Join(",", SourceTypes.Select(t => t.Name));
        return $"Sources:[{sources}] Join:{JoinCondition} Where:{WhereCondition} Select:{SelectProjection} Aggregate:{IsAggregateQuery()}";
    }

    public bool HasGroupBy() => GroupByExpression != null;

    public bool HasTumbling() => Windows.Count > 0;
    public bool HasHopping() => Hopping != null;

    public bool HasAggregates()
    {
        if (SelectProjection == null) return false;
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(SelectProjection.Body);
        return visitor.HasAggregates;
    }

    public bool IsAggregateQuery() => HasGroupBy() || HasTumbling() || HasHopping() || HasAggregates();

    public StreamTableType DetermineType() => IsAggregateQuery() ? StreamTableType.Table : StreamTableType.Stream;

    // Normalize and deduplicate window tokens consistently (e.g., "1m","5m","1h","1wk","1mo").
    public static List<string> NormalizeWindows(IEnumerable<string> windows)
        => windows.Distinct().OrderBy(TimeframeUtils.ToMinutes).ToList();

    public void NormalizeWindowsInPlace()
    {
        var ordered = NormalizeWindows(Windows);
        Windows.Clear();
        Windows.AddRange(ordered);
    }
}

public sealed class HoppingWindowSpec
{
    public TimeSpan Size { get; set; }
    public TimeSpan Advance { get; set; }
    public TimeSpan? Grace { get; set; }
}
