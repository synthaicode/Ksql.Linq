using System;
using System.Collections.Generic;

namespace DailyComparisonLib;

/// <summary>
/// Simple per-symbol bar limit configuration for sampling latest minute bars
/// in Aggregator. If a symbol is not configured, DefaultLimit is used.
/// </summary>
public sealed class BarLimitOptions
{
    /// <summary>
    /// Fallback limit when no per-symbol override is present.
    /// </summary>
    public int DefaultLimit { get; init; } = 10;

    /// <summary>
    /// Optional per-symbol overrides (case-insensitive).
    /// </summary>
    public Dictionary<string, int> PerSymbol { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Resolve limit for a symbol and entity. Entity name is reserved
    /// for future extension; currently not used.
    /// </summary>
    public int GetLimit(string symbol, string entityName)
        => PerSymbol.TryGetValue(symbol, out var limit) ? limit : DefaultLimit;
}

