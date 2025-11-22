using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Runtime;
using Ksql.Linq.Query.Metadata;
using System;

namespace Ksql.Linq.Core.Modeling;

/// <summary>
/// Extensions for defining query-based entities using the new ToQuery DSL.
/// </summary>
public static class EntityBuilderToQueryExtensions
{
    public static EntityModelBuilder<T> ToQuery<T>(this EntityModelBuilder<T> builder, Func<KsqlQueryRoot, IKsqlQueryable> build)
        where T : class
    {
        if (builder == null) throw new ArgumentNullException(nameof(builder));
        if (build == null) throw new ArgumentNullException(nameof(build));

        var root = new KsqlQueryRoot();
        var query = build(root) ?? throw new InvalidOperationException("Query builder returned null");
        var model = query.Build();

        if (model.SourceTypes.Length == 0 || model.SourceTypes.Length > 2)
            throw new NotSupportedException("Only 1 or 2 source types are supported in this phase.");

        ToQueryValidator.ValidateSelectMatchesPoco(typeof(T), model);

        builder.GetModel().QueryModel = model;

        // Auto-register hopping window read mapping if this is a hopping window query
        if (model.Extras.TryGetValue("WindowType", out var windowType) &&
            windowType is string windowTypeStr &&
            windowTypeStr == "HOPPING" &&
            model.HopInterval.HasValue &&
            model.Windows.Count > 0)
        {
            var sourceType = model.SourceTypes[0];
            var timeframe = model.Windows[0]; // e.g., "5m"
            var period = ParsePeriod(timeframe);
            var hopInterval = model.HopInterval.Value;

            TimeBucketTypes.RegisterHoppingRead(sourceType, period, hopInterval, typeof(T));

            // Ensure metadata/timeframe/role are populated so cache & SerDe can resolve window size
            var meta = builder.GetModel().GetOrCreateMetadata();
            meta = meta with { TimeframeRaw = meta.TimeframeRaw ?? timeframe, Role = meta.Role ?? "Live" };
            builder.GetModel().SetMetadata(meta);
            builder.GetModel().AdditionalSettings["timeframe"] = meta.TimeframeRaw!;
            builder.GetModel().AdditionalSettings["role"] = meta.Role!;
        }

        return builder;
    }

    private static Period ParsePeriod(string timeframe)
    {
        // Parse timeframe like "5m", "1h", "1d" into Period
        if (string.IsNullOrWhiteSpace(timeframe))
            throw new ArgumentException("Invalid timeframe", nameof(timeframe));

        var unit = timeframe[^1];
        if (!int.TryParse(timeframe[..^1], out var value))
            value = 1;

        return unit switch
        {
            's' => Period.Seconds(value),
            'm' => Period.Minutes(value),
            'h' => Period.Hours(value),
            'd' => Period.Days(value),
            _ => Period.Minutes(value)
        };
    }
}
