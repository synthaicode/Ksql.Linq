using System;
using System.Linq;
using Ksql.Linq.Query.Builders.Common;

namespace Ksql.Linq.Query.Pipeline;

internal static class WindowValidator
{
    public static void Validate(ExpressionAnalysisResult result)
    {
        if (result == null) throw new ArgumentNullException(nameof(result));
        if (result.Windows.Count == 0)
            return;
        if (!result.BaseUnitSeconds.HasValue)
            throw new InvalidOperationException("Base unit is required for tumbling windows.");

        var baseUnit = result.BaseUnitSeconds.Value;

        if (60 % baseUnit != 0)
            throw new InvalidOperationException("Base unit must divide 60 seconds.");

        var ordered = result.Windows.OrderBy(TimeframeUtils.ToSeconds).ToList();
        foreach (var w in ordered)
        {
            var seconds = TimeframeUtils.ToSeconds(w);

            if (seconds % baseUnit != 0)
                throw new InvalidOperationException($"Window {w} must be a multiple of base {baseUnit}s.");

            if (seconds >= 60 && seconds % 60 != 0)
                throw new InvalidOperationException("Windows ≥ 1 minute must be whole-minute multiples.");
        }

        var grace = result.GraceSeconds ?? 0;
        foreach (var w in ordered)
        {
            grace++;
            if (result.GracePerTimeframe.TryGetValue(w, out var g))
            {
                if (g != grace)
                    throw new InvalidOperationException($"Window {w} grace must be parent grace + 1s.");
            }
            else
            {
                result.GracePerTimeframe[w] = grace;
            }
        }
    }

    // Conversion logic moved to TimeframeUtils
}
