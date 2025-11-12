using Ksql.Linq.Mapping;
using Ksql.Linq.Events;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
namespace Ksql.Linq.Cache.Core;

internal class TableCache<T> : ITableCache<T> where T : class
{
    private const char KeySep = '\u0000'; // NUL
    private readonly MappingRegistry? _mappingRegistry;
    private readonly string _storeName;
    private readonly Func<TimeSpan?, Task> _waitUntilRunning;
    private readonly Lazy<Func<IEnumerable<KeyValuePair<object, object>>>> _enumerateLazy;
    private readonly Func<object, string>? _testKeyStringifier;
    private readonly Func<string, object, Type, object>? _testCombiner;

    public TableCache(MappingRegistry mappingRegistry, string storeName,
                      Func<TimeSpan?, Task> waitUntilRunning,
                      Lazy<Func<IEnumerable<KeyValuePair<object, object>>>> enumerateLazy)
    {
        _mappingRegistry = mappingRegistry;
        _storeName = storeName;
        _waitUntilRunning = waitUntilRunning;
        _enumerateLazy = enumerateLazy;
    }

    internal TableCache(
        Func<TimeSpan?, Task> waitUntilRunning,
        Lazy<Func<IEnumerable<KeyValuePair<object, object>>>> enumerateLazy,
        Func<object, string> keyStringifier,
        Func<string, object, Type, object> combiner)
    {
        _mappingRegistry = null!;            // not used in this constructor
        _storeName = "test";
        _waitUntilRunning = waitUntilRunning;
        _enumerateLazy = enumerateLazy;
        _testKeyStringifier = keyStringifier;
        _testCombiner = combiner;
    }
    public async Task<List<T>> ToListAsync(List<string>? filter = null, TimeSpan? timeout = null)
    {
        var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(90);
        await _waitUntilRunning(effectiveTimeout);
        var mapping = _mappingRegistry is null ? null : _mappingRegistry.GetMapping(typeof(T));

        string? prefix = null;
        if (filter is { Count: > 0 })
            prefix = string.Join(KeySep, filter) + KeySep;

        var deadline = DateTime.UtcNow + effectiveTimeout;
        while (true)
        {
            var list = new List<T>();
            var sampleKeys = new List<string>(capacity: 3);
            var sampleValues = new List<string>(capacity: 3);
            try
            {
                foreach (var kv in _enumerateLazy.Value() )
                {
                    // Key is expected to be a string (stringified by the Streamiz side)
                    var key = kv.Key;
                    var val = kv.Value;
                    var keyStr = key as string
                                  ?? _testKeyStringifier?.Invoke(key)
                                  ?? key?.ToString();
                    if (keyStr == null)
                        continue;

                    if (prefix != null && !keyStr.StartsWith(prefix, StringComparison.Ordinal))
                        continue;

                    T item;
                    if (_testCombiner is not null)
                        item = (T)_testCombiner(keyStr, val, typeof(T));
                    else
                        item = (T)mapping!.CombineFromStringKeyAndAvroValue(keyStr, val, typeof(T));

                    // 追加: 文字列キー末尾の window start(ms) から DTO を優先補完
                    try
                    {
                        var kpLen = mapping?.KeyProperties?.Length ?? 0;
                        if (!string.IsNullOrEmpty(keyStr))
                        {
                            var parts = keyStr.Split(KeySep);
                            if (parts.Length > Math.Max(0, kpLen))
                            {
                                var last = parts[^1];
                                if (long.TryParse(last, NumberStyles.Integer, CultureInfo.InvariantCulture, out var msFromKey))
                                {
                                    var bs = DateTimeOffset.FromUnixTimeMilliseconds(msFromKey).UtcDateTime;
                                    var pBucket2 = typeof(T).GetProperty("BucketStart");
                                    if (pBucket2 != null && pBucket2.CanWrite)
                                    {
                                        var cur = (DateTime)(pBucket2.GetValue(item) ?? DateTime.MinValue);
                                        if (cur == DateTime.MinValue)
                                            try { pBucket2.SetValue(item, bs); } catch { }
                                    }
                                    var pRaw2 = typeof(T).GetProperty("WindowStartRaw", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                                    if (pRaw2 != null && pRaw2.CanWrite)
                                    {
                                        var curRaw = pRaw2.GetValue(item);
                                        var needs = curRaw is null || (curRaw is long l0 && l0 == 0) || (curRaw is int i0 && i0 == 0);
                                        if (needs)
                                        {
                                            var assign = ConvertWindowStartRaw(msFromKey, pRaw2.PropertyType);
                                            try { pRaw2.SetValue(item, assign); } catch { }
                                        }
                                    }
                                    var pYear2 = typeof(T).GetProperty("Year");
                                    if (pYear2 != null && (pYear2.PropertyType == typeof(int) || pYear2.PropertyType == typeof(int?)))
                                    {
                                        var yVal = pYear2.GetValue(item);
                                        var isUnset = yVal is null || (yVal is int yi && yi == 0);
                                        if (isUnset)
                                            try { pYear2.SetValue(item, bs.Year); } catch { }
                                    }
                                }
                            }
                        }
                    }
                    catch { }

                    try
                    {
                        // 補正1: WINDOWED TABLEでキーにBucketStartが含まれず、値側にBUCKETSTARTがある場合に反映
                        var pBucket = typeof(T).GetProperty("BucketStart");
                        DateTime? bucketStartUtc = null;
                        if (pBucket != null && pBucket.PropertyType == typeof(DateTime))
                        {
                            var current = (DateTime)(pBucket.GetValue(item) ?? DateTime.MinValue);
                            if (current != DateTime.MinValue)
                            {
                                bucketStartUtc = current;
                            }
                            else if (val is Avro.Generic.GenericRecord grecord)
                            {
                                var schema = grecord.Schema as Avro.RecordSchema;
                                if (schema != null && schema.Fields != null)
                                {
                                    Avro.Field? match = null;
                                    foreach (var f in schema.Fields)
                                    {
                                        if (string.Equals(f.Name, "BUCKETSTART", StringComparison.OrdinalIgnoreCase))
                                        { match = f; break; }
                                    }
                                    if (match != null)
                                    {
                                        var raw = grecord.GetValue(match.Pos);
                                        if (raw is long ms)
                                        {
                                            bucketStartUtc = DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
                                            pBucket.SetValue(item, bucketStartUtc.Value);
                                        }
                                    }
                                }
                            }
                        }

                        // 補正2: WindowStartRaw が未設定なら GenericRecord または BucketStart から補完
                        try
                        {
                            var pWindowRaw = typeof(T).GetProperty(
                                "WindowStartRaw",
                                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                            if (pWindowRaw != null && pWindowRaw.CanWrite)
                            {
                                var existing = pWindowRaw.GetValue(item);
                                var needsSet = existing is null
                                               || (existing is long l && l == 0)
                                               || (existing is int i && i == 0);
                                if (needsSet)
                                {
                                    long? windowStartMs = null;
                                    if (val is Avro.Generic.GenericRecord grecordRaw)
                                    {
                                        windowStartMs = TryGetTimestampField(grecordRaw, "WINDOWSTARTRAW")
                                                        ?? TryGetTimestampField(grecordRaw, "WINDOWSTART");
                                    }
                                    if (windowStartMs is null && bucketStartUtc.HasValue)
                                        windowStartMs = new DateTimeOffset(bucketStartUtc.Value).ToUnixTimeMilliseconds();

                                    if (windowStartMs is long ms)
                                    {
                                        var converted = ConvertWindowStartRaw(ms, pWindowRaw.PropertyType);
                                        try { pWindowRaw.SetValue(item, converted); } catch { }
                                    }
                                }
                            }
                        }
                        catch { }

                        // 補正3: Year は ksql でサポートされないケースを想定し C# 側で設定
                        // 条件: POCO に Year(int) が存在し、0 または未設定、かつ BucketStart が判明している
                        var pYear = typeof(T).GetProperty("Year");
                        if (pYear != null && (pYear.PropertyType == typeof(int) || pYear.PropertyType == typeof(int?)))
                        {
                            var yVal = pYear.GetValue(item);
                            var isUnset = yVal is null || (yVal is int yi && yi == 0);
                            if (isUnset)
                            {
                                bucketStartUtc ??= (DateTime?)pBucket?.GetValue(item);
                                if (bucketStartUtc.HasValue)
                                {
                                    pYear.SetValue(item, bucketStartUtc.Value.Year);
                                }
                            }
                        }
                    }
                    catch { }

                    // 補4: Live集計列（SUMBID/MAXBID/MINBID/CNT/FIRSTBID/LASTBID 等）が未設定の場合、GenericRecordから補完
                    try
                    {
                        if (val is Avro.Generic.GenericRecord grec)
                        {
                            void SetIfUnset(string fieldName, string propName)
                            {
                                try
                                {
                                    var p = typeof(T).GetProperty(propName, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                                    if (p == null || !p.CanWrite) return;
                                    var cur = p.GetValue(item);
                                    var needs = cur == null
                                                || (cur is double d && Math.Abs(d) < 1e-18)
                                                || (cur is float f && Math.Abs(f) < 1e-12)
                                                || (cur is int i && i == 0)
                                                || (cur is long l && l == 0L)
                                                || (cur is decimal m && m == 0m);
                                    if (!needs) return;

                                    var schema = grec.Schema as Avro.RecordSchema;
                                    if (schema == null || schema.Fields == null) return;
                                    Avro.Field? fld = null;
                                    foreach (var fld2 in schema.Fields)
                                    {
                                        if (string.Equals(fld2.Name, fieldName, StringComparison.OrdinalIgnoreCase))
                                        { fld = fld2; break; }
                                    }
                                    if (fld == null) return;
                                    var raw = grec.GetValue(fld.Pos);
                                    if (raw == null) return;
                                    var targetType = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;
                                    object? converted = ConvertGeneric(raw, targetType);
                                    if (converted != null)
                                        p.SetValue(item, converted);
                                }
                                catch { }
                            }

                            SetIfUnset("SUMBID", "SumBid");
                            SetIfUnset("MAXBID", "MaxBid");
                            SetIfUnset("MINBID", "MinBid");
                            SetIfUnset("CNT", "Cnt");
                            SetIfUnset("FIRSTBID", "FirstBid");
                            SetIfUnset("LASTBID", "LastBid");
                            SetIfUnset("NAMELEN", "NameLen");
                        }
                    }
                    catch { }

                    // Debug: dump a few aggregate properties if present
                    try
                    {
                        var propsToDump = new[] { "SumBid", "MaxBid", "MinBid", "Cnt", "FirstBid", "LastBid" };
                        var sbAgg = new System.Text.StringBuilder();
                        sbAgg.Append("agg:");
                        foreach (var pn in propsToDump)
                        {
                            var pp = typeof(T).GetProperty(pn, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                            if (pp == null) continue;
                            object? vv = null; try { vv = pp.GetValue(item); } catch { }
                            sbAgg.Append(' ').Append(pn).Append('=').Append(vv ?? "<null>");
                        }
                        if (sbAgg.Length > 4)
                            try { System.Console.WriteLine($"[cache] {typeof(T).Name} {sbAgg}"); } catch { }
                    }
                    catch { }

                    list.Add(item);
                    if (sampleKeys.Count < 3)
                        sampleKeys.Add(keyStr.Replace('\u0000', '|'));
                    if (sampleValues.Count < 3)
                    {
                        try
                        {
                            // Try to summarize value payload (GenericRecord preferred)
                            string summary;
                            if (val is Avro.Generic.GenericRecord grec)
                            {
                                object Get(string name)
                                {
                                    try
                                    {
                                        var schema = grec.Schema as Avro.RecordSchema;
                                        if (schema != null)
                                        {
                                            var f = schema.Fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));
                                            if (f != null)
                                            {
                                                var v = grec.GetValue(f.Pos);
                                                return v ?? "<null>";
                                            }
                                        }
                                    }
                                    catch { }
                                    return "?";
                                }
                                // Common bar fields if present
                                var bs = Get("BUCKETSTART");
                                var yr = Get("YEAR");
                                var op = Get("OPEN");
                                var hi = Get("HIGH");
                                var lo = Get("LOW");
                                var cl = Get("CLOSE");
                                summary = $"BUCKETSTART={bs},OPEN={op},HIGH={hi},LOW={lo},CLOSE={cl},YEAR={yr}";
                            }
                            else
                            {
                                summary = val?.ToString() ?? "<null>";
                            }
                            sampleValues.Add(summary);
                        }
                        catch { }
                    }
                }
                try
                {
                    var msg = prefix is null
                        ? $"snapshot size={list.Count}"
                        : $"snapshot size={list.Count} prefix={prefix.Replace('\u0000', '|')}";
                    // Console echo to surface in test output
                    try { System.Console.WriteLine($"[cache] {typeof(T).Name} store={_storeName} {msg}"); } catch { }
                    Ksql.Linq.Events.RuntimeEvents.TryPublishFireAndForget(new RuntimeEvent
                    {
                        Name = "cache.snapshot",
                        Phase = "read",
                        Entity = typeof(T).Name,
                        Topic = _storeName,
                        Success = true,
                        Message = msg
                    });
                    // Emit up to 3 key/value sample rows for precise verification
                    for (int i = 0; i < sampleKeys.Count && i < sampleValues.Count; i++)
                    {
                        try { System.Console.WriteLine($"[cache] key={sampleKeys[i]} value={sampleValues[i]}"); } catch { }
                        Ksql.Linq.Events.RuntimeEvents.TryPublishFireAndForget(new RuntimeEvent
                        {
                            Name = "cache.row",
                            Phase = "observed",
                            Entity = typeof(T).Name,
                            Topic = _storeName,
                            Success = true,
                            Message = $"key={sampleKeys[i]} value={sampleValues[i]}"
                        });
                    }
                    if (list.Count > 0)
                    {
                        var sample = string.Join(",", sampleKeys);
                        Ksql.Linq.Events.RuntimeEvents.TryPublishFireAndForget(new RuntimeEvent
                        {
                            Name = "cache.first_data",
                            Phase = "observed",
                            Entity = typeof(T).Name,
                            Topic = _storeName,
                            Success = true,
                            Message = string.IsNullOrEmpty(sample) ? "keys=?" : $"keys={sample}"
                        });
                    }
                }
                catch { }
                return list;
            }
            catch (Exception ex) when (
                ex.GetType().FullName == "Streamiz.Kafka.Net.Errors.InvalidStateStoreException"
                || (ex.Message?.IndexOf("may have migrated to another instance", StringComparison.OrdinalIgnoreCase) >= 0)
                || (ex.Message?.IndexOf("PENDING_SHUTDOWN", StringComparison.OrdinalIgnoreCase) >= 0)
            )
            {
                try
                {
                    Ksql.Linq.Events.RuntimeEvents.TryPublishFireAndForget(new RuntimeEvent
                    {
                        Name = "cache.error",
                        Phase = "retry",
                        Entity = typeof(T).Name,
                        Topic = _storeName,
                        Success = false,
                        Message = $"{ex.GetType().FullName}: {ex.Message}"
                    });
                }
                catch { }
                if (DateTime.UtcNow >= deadline)
                    throw;
                await Task.Delay(500);
                continue;
            }
        }
    }

    private static long? TryGetTimestampField(Avro.Generic.GenericRecord record, string fieldName)
    {
        if (record.Schema is not Avro.RecordSchema schema || schema.Fields == null)
            return null;

        foreach (var field in schema.Fields)
        {
            if (!string.Equals(field.Name, fieldName, StringComparison.Ordinal)
                && !string.Equals(field.Name, fieldName, StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                var value = record.GetValue(field.Pos);
                if (value is null)
                    return null;
                return value switch
                {
                    long l => l,
                    int i => i,
                    DateTime dt => new DateTimeOffset(dt).ToUnixTimeMilliseconds(),
                    DateTimeOffset dto => dto.ToUnixTimeMilliseconds(),
                    _ => Convert.ToInt64(value, CultureInfo.InvariantCulture)
                };
            }
            catch
            {
                return null;
            }
        }

        return null;
    }

    private static object? ConvertWindowStartRaw(long ms, Type targetType)
    {
        var core = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (core == typeof(long))
            return ms;
        if (core == typeof(DateTime))
            return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
        if (core == typeof(DateTimeOffset))
            return DateTimeOffset.FromUnixTimeMilliseconds(ms);
        try { return Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(ms, core); }
        catch { return ms; }
    }

    private static object? ConvertGeneric(object raw, Type targetType)
    {
        var core = Nullable.GetUnderlyingType(targetType) ?? targetType;
        try
        {
            if (core == typeof(decimal))
            {
                if (raw is double dd) return Convert.ToDecimal(dd, CultureInfo.InvariantCulture);
                if (raw is float ff) return Convert.ToDecimal(ff, CultureInfo.InvariantCulture);
                if (raw is long ll) return Convert.ToDecimal(ll, CultureInfo.InvariantCulture);
                if (raw is int ii) return Convert.ToDecimal(ii, CultureInfo.InvariantCulture);
                return Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(raw, core);
            }
            if (core == typeof(double))
            {
                return Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(raw, core);
            }
            if (core == typeof(long))
            {
                if (raw is int ii) return (long)ii;
                return Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(raw, core);
            }
            if (core == typeof(int))
            {
                if (raw is long ll) return (int)ll;
                return Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(raw, core);
            }
            return Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(raw, core);
        }
        catch { return null; }
    }

    public void Dispose() { }
}
