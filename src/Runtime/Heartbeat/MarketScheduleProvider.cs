using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Heartbeat;

public interface IMarketScheduleProvider
{
    Task InitializeAsync(Type scheduleType, IEnumerable rows, CancellationToken ct);
    Task RefreshAsync(Type scheduleType, IEnumerable rows, CancellationToken ct);
    bool IsInSession(IReadOnlyList<string> keyParts, DateTime utcTs);
}

internal sealed class MarketScheduleProvider : IMarketScheduleProvider
{
    private readonly MappingRegistry _registry;
    private readonly Dictionary<string, List<(DateTime OpenUtc, DateTime CloseUtc)>> _index = new();
    private PropertyMeta[] _keyMeta = System.Array.Empty<PropertyMeta>();

    public MarketScheduleProvider(MappingRegistry registry) => _registry = registry;

    public Task InitializeAsync(Type scheduleType, IEnumerable rows, CancellationToken ct)
    {
        BuildIndex(scheduleType, rows);
        return Task.CompletedTask;
    }

    public Task RefreshAsync(Type scheduleType, IEnumerable rows, CancellationToken ct)
    {
        BuildIndex(scheduleType, rows);
        return Task.CompletedTask;
    }

    public bool IsInSession(IReadOnlyList<string> keyParts, DateTime utcTs)
    {
        var key = string.Join("\0", keyParts);
        if (!_index.TryGetValue(key, out var list))
            return false;
        var lo = 0;
        var hi = list.Count - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) / 2;
            var (open, close) = list[mid];
            if (utcTs < open)
                hi = mid - 1;
            else if (utcTs >= close)
                lo = mid + 1;
            else
                return true;
        }
        return false;
    }

    private void BuildIndex(Type scheduleType, IEnumerable rows)
    {
        _index.Clear();
        var mapping = _registry.GetMapping(scheduleType);
        _keyMeta = mapping.KeyProperties;
        // Derive open/close properties without fixed names
        static bool IsDateTimeType(Type t)
            => (Nullable.GetUnderlyingType(t) ?? t) == typeof(DateTime);
        PropertyInfo? ResolveProp(string kind)
        {
            // Try mapping meta first
            List<PropertyInfo> candidates = new();
            var metas = mapping.ValueProperties;
            if (metas != null && metas.Length > 0)
            {
                candidates = metas
                    .Where(m => IsDateTimeType(m.PropertyType))
                    .Select(m => m.PropertyInfo)
                    .Where(pi => pi != null)
                    .Cast<PropertyInfo>()
                    .ToList();
            }
            if (candidates.Count == 0)
            {
                candidates = scheduleType
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && IsDateTimeType(p.PropertyType))
                    .ToList();
            }
            string[] prefer = kind.Equals("open", StringComparison.OrdinalIgnoreCase)
                ? new[] { "open", "start", "begin" }
                : new[] { "close", "end", "finish" };
            foreach (var p in candidates)
            {
                var name = p.Name;
                if (prefer.Any(tok => name.IndexOf(tok, StringComparison.OrdinalIgnoreCase) >= 0))
                    return p;
            }
            // Fallback: first for open, last for close
            if (candidates.Count > 0)
            {
                return kind.Equals("open", StringComparison.OrdinalIgnoreCase)
                    ? candidates.First()
                    : candidates.Last();
            }
            return null;
        }
        var openProp = ResolveProp("open") ?? throw new InvalidOperationException("Cannot resolve market open property (DateTime) from schedule type.");
        var closeProp = ResolveProp("close") ?? throw new InvalidOperationException("Cannot resolve market close property (DateTime) from schedule type.");
        foreach (var r in rows)
        {
            var parts = new string[_keyMeta.Length];
            for (int i = 0; i < _keyMeta.Length; i++)
                parts[i] = Convert.ToString(_keyMeta[i].PropertyInfo!.GetValue(r)) ?? string.Empty;
            var key = string.Join("\0", parts);
            if (!_index.TryGetValue(key, out var list))
            {
                list = new List<(DateTime, DateTime)>();
                _index[key] = list;
            }
            var open = ((DateTime)openProp.GetValue(r)!).ToUniversalTime();
            var close = ((DateTime)closeProp.GetValue(r)!).ToUniversalTime();
            list.Add((open, close));
        }
        foreach (var list in _index.Values)
            list.Sort((a, b) => a.OpenUtc.CompareTo(b.OpenUtc));
    }
}
