using Ksql.Linq.Query.Analysis;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Ksql.Linq.Query.Pipeline;

/// <summary>
/// Converts analyzed LINQ method calls into a TumblingQao model for downstream pipelines.
/// Responsibility: map ExpressionAnalysisResult to TumblingQao without side effects.
/// </summary>
internal static class TumblingExpressionAnalyzer
{
    public static TumblingQao Build(ExpressionAnalysisResult result)
    {
        if (result == null) throw new ArgumentNullException(nameof(result));
        // Windows are strings like "1s","5m","1h","1d","1wk","1mo"
        static Timeframe Parse(string w)
        {
            if (w.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
                return new Timeframe(int.Parse(w.Substring(0, w.Length - 2)), "mo");
            if (w.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
                return new Timeframe(int.Parse(w.Substring(0, w.Length - 2)), "wk");
            var unit = w[w.Length - 1].ToString();
            var value = int.Parse(w.Substring(0, w.Length - 1));
            return new Timeframe(value, unit);
        }

        var frames = result.Windows.Select(Parse).ToList();
        // Keys are group-by keys if present; otherwise empty (resolved later by orchestrator/model)
        var keys = result.GroupByKeys.Count > 0 ? result.GroupByKeys.ToArray() : Array.Empty<string>();
        var projection = new List<string>(); // Filled later upstream from model shape
        var basedOn = new BasedOnSpec(
            result.BasedOnJoinKeys.ToArray(),
            result.BasedOnOpen ?? string.Empty,
            result.BasedOnClose ?? string.Empty,
            result.BasedOnDayKey ?? string.Empty,
            result.BasedOnOpenInclusive,
            result.BasedOnCloseInclusive);

        var qao = new TumblingQao
        {
            TimeKey = result.TimeKey ?? string.Empty,
            Windows = frames,
            Keys = keys,
            Projection = projection,
            PocoShape = Array.Empty<ColumnShape>(),
            BasedOn = basedOn,
            WeekAnchor = result.WeekAnchor,
            BaseUnitSeconds = result.BaseUnitSeconds,
            GraceSeconds = result.GraceSeconds
        };

        // Grace step chain; already populated in result by WindowValidator
        foreach (var kv in result.GracePerTimeframe)
            qao.GracePerTimeframe[kv.Key] = kv.Value;

        return qao;
    }
}

