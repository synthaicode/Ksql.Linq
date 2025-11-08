using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Ksql.Linq.Query.Hub.Analysis;

/// <summary>
/// Keeps track of available hub input (rows stream) columns per topic to assist
/// mapping aggregate arguments without hard-coded naming.
/// </summary>
internal static class HubInputIntrospector
{
    private static readonly ConcurrentDictionary<string, HashSet<string>> _columnsByTopic
        = new(StringComparer.OrdinalIgnoreCase);

    public static void Record(string topic, IEnumerable<string> columns)
    {
        if (string.IsNullOrWhiteSpace(topic) || columns == null) return;
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var c in columns)
        {
            if (!string.IsNullOrWhiteSpace(c)) set.Add(c.ToUpperInvariant());
        }
        if (set.Count > 0)
            _columnsByTopic[topic] = set;
    }

    public static ISet<string>? TryGetColumns(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic)) return null;
        return _columnsByTopic.TryGetValue(topic, out var set) ? set : null;
    }
}

