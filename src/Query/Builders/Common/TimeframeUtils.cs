using System;

namespace Ksql.Linq.Query.Builders.Common;

internal static class TimeframeUtils
{
    // Normalize unit name + value to compact token like "1m", "2h", "7d", "1mo"
    public static string Normalize(int value, string unitName)
    {
        if (value <= 0) value = 1;
        var unit = unitName?.Trim().ToLowerInvariant();
        return unit switch
        {
            "minutes" => value + "m",
            "hours" => value + "h",
            "days" => value + "d",
            "months" => value + "mo",
            _ => value.ToString()
        };
    }

    public static int ToSeconds(string timeframe)
    {
        if (string.IsNullOrWhiteSpace(timeframe)) return int.MaxValue;
        var tf = timeframe.Trim();
        if (tf.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
            return ParseIntSafe(tf.AsSpan(0, tf.Length - 2)) * 30 * 24 * 3600;
        if (tf.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
            return ParseIntSafe(tf.AsSpan(0, tf.Length - 2)) * 7 * 24 * 3600;
        var unit = char.ToLowerInvariant(tf[tf.Length - 1]);
        var value = ParseIntSafe(tf.AsSpan(0, tf.Length - 1));
        return unit switch
        {
            's' => value,
            'm' => value * 60,
            'h' => value * 3600,
            'd' => value * 86400,
            _ => value
        };
    }

    public static int ToMinutes(string timeframe)
    {
        var seconds = ToSeconds(timeframe);
        if (seconds == int.MaxValue) return int.MaxValue;
        // For month/week approximation above, convert seconds to minutes
        return seconds / 60;
    }

    public static bool TryToMilliseconds(string timeframe, out long milliseconds)
    {
        milliseconds = 0;
        var seconds = ToSeconds(timeframe);
        if (seconds == int.MaxValue || seconds <= 0)
            return false;
        milliseconds = (long)seconds * 1000L;
        return true;
    }

    public static int Compare(string a, string b)
    {
        // Sort by seconds ascending
        return ToSeconds(a).CompareTo(ToSeconds(b));
    }

    public static Ksql.Linq.Runtime.Period ToPeriod(string timeframe)
    {
        if (string.IsNullOrWhiteSpace(timeframe)) return Ksql.Linq.Runtime.Period.Minutes(1);
        var tf = timeframe.Trim();
        if (tf.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
        {
            var vm = ParseIntSafe(tf.AsSpan(0, tf.Length - 2));
            if (vm <= 0) vm = 1;
            return Ksql.Linq.Runtime.Period.Months(vm);
        }
        if (tf.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
        {
            // Treat as a week period.
            return Ksql.Linq.Runtime.Period.Week();
        }
        var unit = char.ToLowerInvariant(tf[tf.Length - 1]);
        var v = ParseIntSafe(tf.AsSpan(0, tf.Length - 1));
        if (v <= 0) v = 1;
        return unit switch
        {
            's' => Ksql.Linq.Runtime.Period.Seconds(v),
            'm' => Ksql.Linq.Runtime.Period.Minutes(v),
            'h' => Ksql.Linq.Runtime.Period.Hours(v),
            'd' => Ksql.Linq.Runtime.Period.Days(v),
            _ => Ksql.Linq.Runtime.Period.Minutes(1)
        };
    }

    public static string ToKsqlWindowClause(string timeframe)
    {
        if (string.IsNullOrWhiteSpace(timeframe))
            return "WINDOW TUMBLING (SIZE 1 MINUTES)";
        var tf = timeframe.Trim();
        if (tf.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
        {
            var w = ParseIntSafe(tf.AsSpan(0, tf.Length - 2));
            if (w <= 0) w = 1;
            return $"WINDOW TUMBLING (SIZE {w * 7} DAYS)";
        }
        if (tf.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
        {
            var mo = ParseIntSafe(tf.AsSpan(0, tf.Length - 2));
            if (mo <= 0) mo = 1;
            return $"WINDOW TUMBLING (SIZE {mo} MONTHS)";
        }
        var unit = char.ToLowerInvariant(tf[tf.Length - 1]);
        var val = ParseIntSafe(tf.AsSpan(0, tf.Length - 1));
        if (val <= 0) val = 1;
        return unit switch
        {
            's' => $"WINDOW TUMBLING (SIZE {val} SECONDS)",
            'm' => $"WINDOW TUMBLING (SIZE {val} MINUTES)",
            'h' => $"WINDOW TUMBLING (SIZE {val} HOURS)",
            'd' => $"WINDOW TUMBLING (SIZE {val} DAYS)",
            _ => $"WINDOW TUMBLING (SIZE {val} MINUTES)"
        };
    }

    public static (int Value, string Unit) Decompose(string timeframe)
    {
        if (string.IsNullOrWhiteSpace(timeframe))
            return (1, "m");
        var tf = timeframe.Trim();
        if (tf.EndsWith("mo", StringComparison.OrdinalIgnoreCase))
            return (ParseIntSafe(tf.AsSpan(0, tf.Length - 2)), "mo");
        if (tf.EndsWith("wk", StringComparison.OrdinalIgnoreCase))
            return (ParseIntSafe(tf.AsSpan(0, tf.Length - 2)), "wk");
        var unit = tf[tf.Length - 1].ToString();
        var value = ParseIntSafe(tf.AsSpan(0, tf.Length - 1));
        if (value <= 0) value = 1;
        return (value, unit);
    }

    private static int ParseIntSafe(ReadOnlySpan<char> span)
    {
        var s = span.ToString();
        return int.TryParse(s, out var v) ? v : 0;
    }
}