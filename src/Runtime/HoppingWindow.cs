using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;

namespace Ksql.Linq.Runtime;

/// <summary>
/// Pull-only helper to read hopping window tables by key and time range.
/// Mirrors the usage of TimeBucket (tumbling) but uses WINDOWSTART/WINDOWEND filters.
/// </summary>
public sealed class HoppingWindow<T> where T : class, IWindowedRecord
{
    private readonly KsqlContext _ctx;
    private readonly string _tableName;
    private readonly KeyValueTypeMapping _mapping;

    internal HoppingWindow(KsqlContext ctx, string tableName)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _mapping = _ctx.GetMappingRegistry().GetMapping(typeof(T));
    }

    /// <summary>
    /// Pull rows from the hopping table by key and optional time range.
    /// </summary>
    public async Task<IReadOnlyList<T>> ToListAsync(
        object key,
        DateTime? from = null,
        DateTime? to = null,
        int? limit = null,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var keyNames = (_mapping.KeyProperties ?? Array.Empty<PropertyMeta>()).Select(p => p.Name).ToList();
        var valNames = (_mapping.ValueProperties ?? Array.Empty<PropertyMeta>())
            .Select(p => p.Name)
            .Where(n => !string.Equals(n, "WindowStart", StringComparison.OrdinalIgnoreCase)
                     && !string.Equals(n, "WindowEnd", StringComparison.OrdinalIgnoreCase))
            .ToList();

        var selectNames = new List<string>(keyNames.Count + valNames.Count + 2);
        selectNames.AddRange(keyNames);
        selectNames.AddRange(valNames);
        selectNames.Add("WINDOWSTART");
        selectNames.Add("WINDOWEND");

        // WHERE
        var conditions = new List<string>();
        var keyValues = ResolveKeyValues(key, keyNames);
        for (int i = 0; i < Math.Min(keyNames.Count, keyValues.Count); i++)
        {
            var v = EscapeValue(keyValues[i]);
            conditions.Add($"{keyNames[i].ToUpperInvariant()}='{v}'");
        }
        if (from.HasValue)
        {
            var ms = new DateTimeOffset(DateTime.SpecifyKind(from.Value, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
            conditions.Add($"WINDOWSTART >= {ms}");
        }
        if (to.HasValue)
        {
            var ms = new DateTimeOffset(DateTime.SpecifyKind(to.Value, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
            conditions.Add($"WINDOWEND <= {ms}");
        }
        var where = conditions.Count > 0 ? (" WHERE " + string.Join(" AND ", conditions)) : string.Empty;

        var sql = $"SELECT {string.Join(", ", selectNames.Select(n => n.ToUpperInvariant()))} FROM {_tableName.ToUpperInvariant()}{where}";
        if (limit.HasValue && limit.Value > 0) sql += $" LIMIT {limit.Value}";
        sql += ";";

        var rows = await _ctx.QueryRowsAsync(sql, timeout).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        var list = new List<T>();
        foreach (var cols in rows)
        {
            var t = Activator.CreateInstance(typeof(T)) as T;
            if (t == null) continue;
            for (int i = 0; i < selectNames.Count && i < cols.Length; i++)
            {
                var name = selectNames[i];
                var val = cols[i];
                if (string.Equals(name, "WINDOWSTART", StringComparison.OrdinalIgnoreCase))
                {
                    SetProperty(t, "WindowStart", ConvertToDateTime(val));
                    continue;
                }
                if (string.Equals(name, "WINDOWEND", StringComparison.OrdinalIgnoreCase))
                {
                    SetProperty(t, "WindowEnd", ConvertToDateTime(val));
                    continue;
                }
                SetProperty(t, name, val);
            }
            list.Add(t);
        }
        return list;
    }

    private static List<object?> ResolveKeyValues(object key, IList<string> keyNames)
    {
        var result = new List<object?>();
        if (key == null) return result;
        if (keyNames.Count == 1 && (key is string || key.GetType().IsValueType))
        {
            result.Add(key);
            return result;
        }
        if (key is System.Collections.IDictionary dict)
        {
            foreach (var k in keyNames)
            {
                result.Add(dict.Contains(k) ? dict[k] : null);
            }
            return result;
        }

        var props = key.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        foreach (var k in keyNames)
        {
            var p = props.FirstOrDefault(x => string.Equals(x.Name, k, StringComparison.OrdinalIgnoreCase));
            result.Add(p != null ? p.GetValue(key) : null);
        }
        return result;
    }

    private static string EscapeValue(object? v)
    {
        if (v == null) return string.Empty;
        return v.ToString()?.Replace("'", "''") ?? string.Empty;
    }

    private static DateTime ConvertToDateTime(object? v)
    {
        if (v == null) return default;
        if (v is DateTime dt) return dt;
        if (v is DateTimeOffset dto) return dto.UtcDateTime;
        if (long.TryParse(v.ToString(), out var ms))
            return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
        if (DateTime.TryParse(v.ToString(), out var dt2)) return dt2;
        return default;
    }

    private static void SetProperty(object target, string name, object? value)
    {
        var prop = target.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop == null || !prop.CanWrite) return;
        try
        {
            var converted = value;
            if (value != null && prop.PropertyType != typeof(object))
            {
                converted = Convert.ChangeType(value, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            }
            prop.SetValue(target, converted);
        }
        catch
        {
        }
    }
}

public static class HoppingWindow
{
    public static HoppingWindow<T> Get<T>(KsqlContext ctx, string? tableName = null) where T : class, IWindowedRecord
    {
        if (ctx == null) throw new ArgumentNullException(nameof(ctx));
        var resolved = tableName;
        if (string.IsNullOrWhiteSpace(resolved))
        {
            var models = ctx.GetEntityModels();
            if (models.TryGetValue(typeof(T), out var model) && !string.IsNullOrWhiteSpace(model.TopicName))
            {
                resolved = model.TopicName;
            }
            else
            {
                resolved = typeof(T).Name;
            }
        }
        return new HoppingWindow<T>(ctx, resolved!);
    }
}
