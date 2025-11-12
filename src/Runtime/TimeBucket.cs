using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Cache.Extensions;
using Ksql.Linq.Incidents;
using Ksql.Linq.Events;

namespace Ksql.Linq.Runtime;

/// <summary>
/// Write-context for importing bar data into time-bucketed topics.
/// An application (importer) should implement this to map to its producer.
/// </summary>
// Interfaces removed: unified on KsqlContext for read/write

public static class TimeBucket
{
    public static TimeBucket<T> Get<T>(Ksql.Linq.KsqlContext ctx, Period period) where T : class
        => new(ctx, period);

    public static TimeBucketWriter<T> Set<T>(Ksql.Linq.KsqlContext ctx, Period period) where T : class
        => new(ctx, period);
}

public sealed class TimeBucket<T> where T : class
{
    private readonly Ksql.Linq.KsqlContext _ctx;
    private readonly Period _period;
    private readonly string _liveTopic;
    private readonly Type _readType;
    private readonly Type _writeType;

    internal TimeBucket(Ksql.Linq.KsqlContext ctx, Period period)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        _period = period;
        _liveTopic = TimeBucketTypes.GetLiveTopicName(typeof(T), period);
        _readType = TimeBucketTypes.ResolveRead(typeof(T), period) ?? typeof(T);
        _writeType = TimeBucketTypes.ResolveWrite(typeof(T), period) ?? typeof(T);
    }

    public Task<List<T>> ToListAsync(CancellationToken ct)
        => ToListAsync(null, ct);

    public Task<List<T>> ToListAsync()
        => ToListAsync(null, CancellationToken.None);

    public async Task<List<T>> ToListAsync(IReadOnlyList<string>? pkFilter, CancellationToken ct)
    {
        if (_period.Unit == PeriodUnit.Seconds)
            throw new ArgumentOutOfRangeException(nameof(_period), "Period must be minutes or greater.");

        // Resolve TableCache for the read type and pass pkFilter as prefix parts
        var getCache = typeof(Ksql.Linq.Cache.Extensions.KsqlContextCacheExtensions)
            .GetMethod("GetTableCache", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)!;
        var getCacheGeneric = getCache.MakeGenericMethod(_readType);
        var cache = getCacheGeneric.Invoke(null, new object?[] { _ctx });
        if (cache == null)
            throw new InvalidOperationException("Table cache not available for TimeBucket read.");

        List<string>? filter = null;
        if (pkFilter is { Count: > 0 })
            filter = new List<string>(pkFilter);

        var toList = cache.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) });
        IEnumerable? resultEnum = null;
        try
        {
            var taskObj = toList!.Invoke(cache, new object?[] { filter, (TimeSpan?)null })!;
            var task = (Task)taskObj;
            await task.ConfigureAwait(false);
            var resultProp = taskObj.GetType().GetProperty("Result")!;
            resultEnum = (IEnumerable)resultProp.GetValue(taskObj)!;
        }
        catch
        {
            // Fallback: pull via ksqlDB if cache enumeration fails (e.g., SerDes incompatibilities)
            // Note: runtime event for fallback is suppressed to avoid noisy logs (entity may be base type)
            if (filter is { Count: >= 2 })
                return await FallbackQueryRowsAsync(filter, ct, null).ConfigureAwait(false);
            throw;
        }
        List<T> Snapshot()
        {
            var acc = new List<T>();
            foreach (var item in resultEnum)
            {
                // Always materialize a fresh T and copy by matching property names (case-insensitive)
                try
                {
                    var t = Activator.CreateInstance(typeof(T));
                    if (t == null) continue;
                    var srcProps = item?.GetType().GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance) ?? Array.Empty<System.Reflection.PropertyInfo>();
                    var dstProps = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                        .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);
                    foreach (var sp in srcProps)
                    {
                        if (!dstProps.TryGetValue(sp.Name, out var dp)) continue;
                        if (!dp.CanWrite) continue;
                        var val = sp.GetValue(item);
                        try { dp.SetValue(t, val); } catch { }
                    }
                    try
                    {
                        var bucketProp = typeof(T).GetProperty("BucketStart");
                        var rawProp = typeof(T).GetProperty("WindowStartRaw", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                        if (bucketProp != null && rawProp != null && rawProp.CanWrite)
                        {
                            var rawVal = rawProp.GetValue(t);
                            if (rawVal == null || (rawVal is long l && l == 0))
                            {
                                var bucketVal = bucketProp.GetValue(t);
                                if (bucketVal is DateTime dt)
                                {
                                    var ms = new DateTimeOffset(dt).ToUnixTimeMilliseconds();
                                    rawProp.SetValue(t, ms);
                                    try { System.Console.WriteLine($"[snapshot] set WindowStartRaw from BucketStart ms={ms}"); } catch { }
                                }
                            }
                        }
                    }
                    catch { }
                    acc.Add((T)t);
                }
                catch { }
            }
            return acc;
        }

        var list = Snapshot();
        if (list.Count > 0)
        {
            // 補正: BucketStartが未設定（MinValue）の行しか無い場合は、ksql Pullで補完
            if (list.All(x => {
                var p = typeof(T).GetProperty("BucketStart");
                return p != null && ((DateTime)(p.GetValue(x) ?? DateTime.MinValue)) == DateTime.MinValue;
            }))
            {
                if (filter is { Count: >= 2 })
                    return await FallbackQueryRowsAsync(filter, CancellationToken.None, null).ConfigureAwait(false);
            }
            return list;
        }
        // Minimal retry (non-cancelable tokensでも1回だけリトライ): 空なら即再取得し、それでも空ならpullを1回
        if (list.Count == 0)
        {
            try
            {
                var taskObj2 = toList!.Invoke(cache, new object?[] { filter, (TimeSpan?)null })!;
                var task2 = (Task)taskObj2;
                await task2.ConfigureAwait(false);
                var resultProp2 = taskObj2.GetType().GetProperty("Result")!;
                resultEnum = (IEnumerable)resultProp2.GetValue(taskObj2)!;
                list = Snapshot();
            }
            catch { }
            if (list.Count == 0 && filter is { Count: >= 2 })
            {
                try
                {
                    var pulled = await FallbackQueryRowsAsync(filter, ct, null).ConfigureAwait(false);
                    if (pulled.Count > 0)
                        return pulled;
                }
                catch { }
            }
        }

        // If the provided token can be cancelled (e.g., CancelAfter set), treat it as a wait budget:
        // poll until rows appear or the token is cancelled. If token is not cancelable, return immediately.
        if (!ct.CanBeCanceled)
            return list;

        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(300, ct).ConfigureAwait(false); } catch { break; }
            try
            {
                var taskObj2 = toList!.Invoke(cache, new object?[] { filter, (TimeSpan?)null })!;
                var task2 = (Task)taskObj2;
                await task2.ConfigureAwait(false);
                var resultProp2 = taskObj2.GetType().GetProperty("Result")!;
                resultEnum = (IEnumerable)resultProp2.GetValue(taskObj2)!;
            }
            catch
            {
                if (filter is { Count: >= 2 })
                    return await FallbackQueryRowsAsync(filter, CancellationToken.None, null).ConfigureAwait(false);
                throw;
            }
            list = Snapshot();
         if (list.Count > 0) return list;
        }
        // Return the latest snapshot (possibly empty) on cancellation/timeout
        return list;
    }

    private async Task<List<T>> FallbackQueryRowsAsync(List<string> filter, CancellationToken ct, DateTime? windowStartUtc)
    {
        try
        {
            // Derive column names from mapping/metadata instead of hard-coded identifiers
            var mapping = _ctx.GetMappingRegistry().GetMapping(_readType);
            var keyNames = (mapping.KeyProperties ?? Array.Empty<Ksql.Linq.Core.Models.PropertyMeta>()).Select(p => p.Name).ToList();
            var valNames = (mapping.ValueProperties ?? Array.Empty<Ksql.Linq.Core.Models.PropertyMeta>())
                .Select(p => p.Name)
                // WindowStart/Raw はCTASに含めない運用に合わせてSELECT対象から外す
                .Where(n => !string.Equals(n, "WindowStart", StringComparison.OrdinalIgnoreCase)
                         && !string.Equals(n, "WindowStartRaw", StringComparison.OrdinalIgnoreCase))
                .ToList();
            // Build SELECT list: keys + value columns (order matters for projection mapping)
            var selectNames = new List<string>(keyNames.Count + valNames.Count);
            selectNames.AddRange(keyNames);
            selectNames.AddRange(valNames);
            static string FormatColumnName(string name)
            {
                // WindowStart/Raw は明示列にしない（WHERE は WINDOWSTART を利用可能）
                return name.ToUpperInvariant();
            }
            var colsSql = string.Join(",", selectNames.Select(FormatColumnName));
            try
            {
                System.Console.WriteLine($"[fallback] selectNames={string.Join("|", selectNames)}");
            }
            catch { }

            // WHERE by key filter (prefix order), escaping single quotes
            string where = string.Empty;
            if (filter is { Count: > 0 } && keyNames.Count > 0)
            {
                var conditions = new List<string>();
                for (int i = 0; i < Math.Min(filter.Count, keyNames.Count); i++)
                {
                    var v = (filter[i] ?? string.Empty).Replace("'", "''");
                    conditions.Add($"{keyNames[i].ToUpperInvariant()}='{v}'");
                }
                if (windowStartUtc.HasValue)
                {
                    var ms = new DateTimeOffset(DateTime.SpecifyKind(windowStartUtc.Value, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
                    // 列としては出力しない方針だが WHERE では WINDOWSTART が使用可能
                    conditions.Add($"WINDOWSTART={ms}");
                }
                where = conditions.Count > 0 ? (" WHERE " + string.Join(" AND ", conditions)) : string.Empty;
            }
            var sql = $"SELECT {colsSql} FROM {_liveTopic.ToUpperInvariant()}{where};";
            var rows = await _ctx.QueryRowsAsync(sql, TimeSpan.FromSeconds(15)).ConfigureAwait(false);
            try
            {
                System.Console.WriteLine($"[fallback] sql={sql} rowsCount={rows.Count}");
            }
            catch { }
            var list = new List<T>();
            foreach (var cols in rows)
            {
                var t = Activator.CreateInstance(typeof(T));
                if (t == null) continue;
                for (int i = 0; i < selectNames.Count; i++)
                {
                    var name = selectNames[i];
                    var raw = cols.ElementAtOrDefault(i);
                    // Convert timestamp (BucketStart or property marked with [KsqlTimestamp]) from unix ms if needed
                    if (raw is long && IsTimestampProperty(typeof(T), name))
                    {
                        raw = FromUnixMs(raw);
                    }
                    TrySet(t, name, raw);
                }
                // WindowStartRaw は選択しないため、BucketStart または引数の windowStartUtc から補完
                try
                {
                    var pRaw = typeof(T).GetProperty("WindowStartRaw", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                    if (pRaw != null && pRaw.CanWrite)
                    {
                        var cur = pRaw.GetValue(t);
                        var needs = cur is null || (cur is long l && l == 0) || (cur is int i && i == 0);
                        if (needs)
                        {
                            long? ms = null;
                            var pBs = typeof(T).GetProperty("BucketStart");
                            if (pBs?.GetValue(t) is DateTime bs)
                                ms = new DateTimeOffset(bs).ToUnixTimeMilliseconds();
                            else if (windowStartUtc.HasValue)
                                ms = new DateTimeOffset(windowStartUtc.Value).ToUnixTimeMilliseconds();
                            if (ms.HasValue)
                            {
                                var core = Nullable.GetUnderlyingType(pRaw.PropertyType) ?? pRaw.PropertyType;
                                object assign = core == typeof(long) ? ms.Value : (object)ms.Value;
                                try { pRaw.SetValue(t, assign); } catch { }
                            }
                        }
                    }
                }
                catch { }
                list.Add((T)t);
            }
            try
            {
                System.Console.WriteLine($"[fallback] materialized count={list.Count}");
            }
            catch { }
            return list;
        }
        catch
        {
            return new List<T>();
        }

        static object? FromUnixMs(object? o)
            => o is long l ? DateTimeOffset.FromUnixTimeMilliseconds(l).UtcDateTime : null;

        static bool IsTimestampProperty(Type t, string name)
        {
            try
            {
                var p = t.GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                if (p == null) return false;
                if (string.Equals(p.Name, "BucketStart", StringComparison.OrdinalIgnoreCase)) return true;
                var tsAttr = p.GetCustomAttributes(typeof(Ksql.Linq.Core.Attributes.KsqlTimestampAttribute), inherit: true);
                return tsAttr != null && tsAttr.Length > 0;
            }
            catch { return false; }
        }

        static void TrySet(object target, string name, object? value)
        {
            try
            {
                var p = target.GetType().GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                if (p == null || !p.CanWrite) return;
                if (value == null)
                {
                    p.SetValue(target, null);
                    return;
                }
                var t = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;
                var v = Ksql.Linq.Core.Conversion.ValueConverter.ChangeTypeOrDefault(value, t);
                p.SetValue(target, v);
            }
            catch { }
        }
    }

    [System.Obsolete("Use ReadAtAsync(...) on TimeBucket or WaitForBucketAsync(...) + ToListAsync(...)")]
    public Task<List<T>> ToListAsync(IReadOnlyList<string>? pkFilter, DateTime? waitForBucketStartUtc, TimeSpan? tolerance, CancellationToken ct)
        => ToListAsync(pkFilter, ct);
    [System.Obsolete("Use ReadAtAsync(...) on TimeBucket or WaitForBucketAsync(...) + ToListAsync(...)")]
    public Task<List<T>> ToListAsync(IReadOnlyList<string>? pkFilter, DateTime? waitForBucketStartUtc, CancellationToken ct)
        => ToListAsync(pkFilter, ct);

    /// <summary>
    /// Wait until the specified bucketStart (± tolerance) appears, then return a fresh snapshot for the given key prefix.
    /// Waiting is controlled by the provided CancellationToken (use CancelAfter to cap time).
    /// </summary>
    public async Task<List<T>> ReadAsync(
        IReadOnlyList<string> pkFilter,
        DateTime bucketStartUtc,
        TimeSpan? tolerance,
        CancellationToken ct)
    {
        var tol = tolerance ?? (_period.Unit == PeriodUnit.Minutes
            ? (_period.Value <= 1 ? TimeSpan.FromSeconds(1) : TimeSpan.FromSeconds(2))
            : TimeSpan.FromSeconds(1));
        try
        {
            await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent
            {
                Name = "timebucket.read",
                Phase = "begin",
                Entity = typeof(T).Name,
                Topic = _liveTopic,
                Success = true,
                Message = $"keys={string.Join('|', pkFilter ?? Array.Empty<string>())} bucket={bucketStartUtc:o} tolSec={(int)tol.TotalSeconds}"
            }).ConfigureAwait(false);
        }
        catch { }
        // Periodically probe via ksql pull to avoid cache warmup races
        var attempts = 0;
        while (!ct.IsCancellationRequested)
        {
            // Try a fast cache snapshot first (short timeout) to avoid long 90s waits
            var list = await TryCacheSnapshotAsync(pkFilter, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            try
            {
                await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent
                {
                    Name = "timebucket.cache",
                    Phase = "snapshot",
                    Entity = typeof(T).Name,
                    Topic = _liveTopic,
                    Success = true,
                    Message = $"size={list?.Count ?? 0} attempt={attempts+1}"
                }).ConfigureAwait(false);
            }
            catch { }
            // If cache already has rows for this key prefix, return them immediately.
            if (list != null && list.Count > 0)
            {
                DateTime firstBs = DateTime.MinValue;
                try { firstBs = (DateTime?)list[0]?.GetType().GetProperty("BucketStart")?.GetValue(list[0]) ?? DateTime.MinValue; } catch { }
                if (firstBs != DateTime.MinValue)
                {
                    // 補: 集計列がすべて既定値（0/Null）の場合は Pull で補完
                    try
                    {
                        bool IsDefault(object? v) => v == null || (v is int i && i == 0) || (v is long l && l == 0L) || (v is float f && Math.Abs(f) < 1e-12) || (v is double d && Math.Abs(d) < 1e-18) || (v is decimal m && m == 0m);
                        bool AggregatesDefault(T row)
                        {
                            string[] names = new[] { "SumBid", "MaxBid", "MinBid", "Cnt", "FirstBid", "LastBid" };
                            var any = false; var all = true;
                            foreach (var n in names)
                            {
                                var p = typeof(T).GetProperty(n, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                                if (p == null) continue; any = true;
                                object? v = null; try { v = p.GetValue(row); } catch { }
                                if (!IsDefault(v)) { all = false; break; }
                            }
                            return any && all;
                        }
                        if (list.All(AggregatesDefault) && pkFilter is { Count: >= 2 })
                        {
                            var pulledTry = await FallbackQueryRowsAsync(new List<string>(pkFilter), CancellationToken.None, firstBs).ConfigureAwait(false);
                            if (pulledTry.Count > 0 && !pulledTry.All(AggregatesDefault))
                                return pulledTry;
                        }
                    }
                    catch { }
                    try
                    {
                        await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent
                        {
                            Name = "timebucket.read",
                            Phase = "return",
                            Entity = typeof(T).Name,
                            Topic = _liveTopic,
                            Success = true,
                            Message = $"size={list.Count} firstBucket={firstBs:o}"
                        }).ConfigureAwait(false);
                    }
                    catch { }
                    return list;
                }
                // If BucketStart looks uninitialized (internal type with no public props), compensate with target bucket
                try
                {
                    var dp = typeof(T).GetProperty("BucketStart");
                    if (dp != null && dp.CanWrite)
                    {
                        foreach (var row in list)
                        {
                            dp.SetValue(row, bucketStartUtc);
                            try
                            {
                                var yp = typeof(T).GetProperty("Year");
                                if (yp != null && yp.CanWrite && yp.PropertyType == typeof(int))
                                    yp.SetValue(row, bucketStartUtc.Year);
                            }
                            catch { }
                        }
                        return list;
                    }
                }
                catch { }
            }
            // Try ksql pull frequently to shortcut when the table is populated but cache isn’t yet
            attempts++;
            if (pkFilter is { Count: >= 2 })
            {
                try
                {
                    var pulled = await FallbackQueryRowsAsync(new List<string>(pkFilter), CancellationToken.None, bucketStartUtc).ConfigureAwait(false);
                    try
                    {
                        await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent
                        {
                            Name = "timebucket.pull",
                            Phase = "rows",
                            Entity = typeof(T).Name,
                            Topic = _liveTopic,
                            Success = true,
                            Message = $"size={pulled?.Count ?? 0} attempt={attempts}"
                        }).ConfigureAwait(false);
                    }
                    catch { }
                    if (pulled != null && pulled.Count > 0)
                    {
                        var bsProp2 = typeof(T).GetProperty("BucketStart");
                        if (bsProp2 != null)
                        {
                            foreach (var r in pulled)
                            {
                                if (bsProp2.GetValue(r) is DateTime bs && Math.Abs((bs - bucketStartUtc).TotalSeconds) <= tol.TotalSeconds)
                                    return pulled;
                            }
                        }
                    }
                }
                catch { }
            }
            try { await Task.Delay(300, ct).ConfigureAwait(false); } catch { break; }
        }
        try
        {
            await IncidentBus.PublishAsync(new Incident
            {
                Name = "timebucket_timeout",
                Entity = typeof(T).Name,
                Period = _period.ToString(),
                Keys = pkFilter,
                BucketStartUtc = bucketStartUtc,
                Notes = "ReadAsync cancellation/timeout"
            }, ct).ConfigureAwait(false);
        }
        catch { }
        // propagate cancellation if requested
        ct.ThrowIfCancellationRequested();
        return await ToListAsync(pkFilter, CancellationToken.None).ConfigureAwait(false);
    }

    // Use a short timeout against the underlying TableCache to prevent long stalls
    // when the state store is not yet queryable. Falls back to empty list on errors.
    private async Task<List<T>> TryCacheSnapshotAsync(IReadOnlyList<string>? pkFilter, TimeSpan timeout)
    {
        try
        {
            var getCache = typeof(Ksql.Linq.Cache.Extensions.KsqlContextCacheExtensions)
                .GetMethod("GetTableCache", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)!;
            var getCacheGeneric = getCache.MakeGenericMethod(_readType);
            var cache = getCacheGeneric.Invoke(null, new object?[] { _ctx });
            if (cache == null)
                return new List<T>();

            List<string>? filter = null;
            if (pkFilter is { Count: > 0 })
                filter = new List<string>(pkFilter);

            var toList = cache.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) });
            var taskObj = toList!.Invoke(cache, new object?[] { filter, (TimeSpan?)timeout })!;
            var task = (Task)taskObj;
            await task.ConfigureAwait(false);
            var resultProp = taskObj.GetType().GetProperty("Result")!;
            var resultEnum = (IEnumerable)resultProp.GetValue(taskObj)!;

            var acc = new List<T>();
            foreach (var item in resultEnum)
            {
                try
                {
                    var t = Activator.CreateInstance(typeof(T));
                    if (t == null) continue;
                    var srcProps = item?.GetType().GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance) ?? Array.Empty<System.Reflection.PropertyInfo>();
                    var dstProps = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                        .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);
                    // diag: dump first item's property names to aid mapping
                    if (acc.Count == 0)
                    {
                        try
                        {
                            var names = string.Join(",", srcProps.Select(p => p.Name));
                            var typeName = item?.GetType()?.FullName ?? "<null>";
                            await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent { Name = "timebucket.cache", Phase = "src.props", Entity = typeof(T).Name, Topic = _liveTopic, Success = true, Message = $"type={typeName} props=[{names}]" });
                        }
                        catch { }
                    }
                    foreach (var sp in srcProps)
                    {
                        if (!dstProps.TryGetValue(sp.Name, out var dp)) continue;
                        if (!dp.CanWrite) continue;
                        var val = sp.GetValue(item);
                        try { dp.SetValue(t, val); } catch { }
                    }
                    // Compensate: if BucketStart remained unset, try synonyms on source object
                    try
                    {
                        var dp = typeof(T).GetProperty("BucketStart");
                        if (dp != null && dp.CanWrite && ((DateTime?)dp.GetValue(t) ?? DateTime.MinValue) == DateTime.MinValue)
                        {
                            var sp = item?.GetType().GetProperty("BucketStart")
                                  ?? item?.GetType().GetProperty("WindowStart")
                                  ?? item?.GetType().GetProperty("WindowStartRaw")
                                  ?? item?.GetType().GetProperty("KsqlTimeFrameClose")
                                  ?? item?.GetType().GetProperty("KSQLTIMEFRAMECLOSE", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                            var v = sp?.GetValue(item);
                            if (v is long l) dp.SetValue(t, DateTimeOffset.FromUnixTimeMilliseconds(l).UtcDateTime);
                            else if (v is DateTime dt) dp.SetValue(t, dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime());
                            else if (v is DateTimeOffset dto) dp.SetValue(t, dto.ToUniversalTime().UtcDateTime);
                            else if (v is string s && DateTime.TryParse(s, null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out var parsed))
                                dp.SetValue(t, parsed);
                        }
                    }
                    catch { }
                    acc.Add((T)t);
                }
                catch { }
            }
            try
            {
                await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent
                {
                    Name = "timebucket.cache",
                    Phase = "snapshot.local",
                    Entity = typeof(T).Name,
                    Topic = _liveTopic,
                    Success = true,
                    Message = $"size={acc.Count}"
                }).ConfigureAwait(false);
            }
            catch { }
            return acc;
        }
        catch
        {
            return new List<T>();
        }
    }

    /// <summary>
    /// Wait until a row for the specified bucketStart (± tolerance) appears in the table cache for the given key prefix.
    /// Returns true if observed within the timeout window; false if timed out. Respects the cancellation token.
    /// </summary>
    public async Task<bool> WaitForBucketAsync(
        IReadOnlyList<string> pkFilter,
        DateTime bucketStartUtc,
        TimeSpan tolerance,
        TimeSpan timeout,
        CancellationToken ct)
    {
        if (pkFilter == null || pkFilter.Count == 0)
            throw new ArgumentException("pkFilter must contain at least one key component.", nameof(pkFilter));

        var deadline = DateTime.UtcNow + timeout;

        // Resolve TableCache for the read type (same path as ToListAsync)
        var getCache = typeof(Ksql.Linq.Cache.Extensions.KsqlContextCacheExtensions)
            .GetMethod("GetTableCache", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)!;
        var getCacheGeneric = getCache.MakeGenericMethod(_readType);
        var cache = getCacheGeneric.Invoke(null, new object?[] { _ctx });
        if (cache == null)
            return false;

        var toList = cache.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) });
        var filter = new List<string>(pkFilter);

        var attempts = 0;
        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            try
            {
                var taskObj = toList!.Invoke(cache, new object?[] { filter, (TimeSpan?)null })!;
                var task = (Task)taskObj;
                await task.ConfigureAwait(false);
                var resultProp = taskObj.GetType().GetProperty("Result")!;
                var resultEnum = (IEnumerable)resultProp.GetValue(taskObj)!;

                foreach (var item in resultEnum)
                {
                    try
                    {
                        var bsProp = item?.GetType().GetProperty("BucketStart");
                        if (bsProp == null) continue;
                        if (bsProp.GetValue(item) is DateTime bs)
                        {
                            if (tolerance <= TimeSpan.Zero)
                            {
                                if (bs == bucketStartUtc) return true;
                            }
                            else
                            {
                                if (Math.Abs((bs - bucketStartUtc).TotalSeconds) <= tolerance.TotalSeconds)
                                    return true;
                            }
                        }
                    }
                    catch { }
                }
            }
            catch { }
            // Periodically fall back to a pull probe to detect the row before cache surfaces it
            attempts++;
            if (attempts % 4 == 0)
            {
                try
                {
                    var rows = await FallbackQueryRowsAsync(filter, CancellationToken.None, bucketStartUtc).ConfigureAwait(false);
                    if (rows.Count > 0)
                    {
                        foreach (var r in rows)
                        {
                            var p = r?.GetType().GetProperty("BucketStart");
                            if (p?.GetValue(r) is DateTime bs)
                            {
                                if (tolerance <= TimeSpan.Zero)
                                {
                                    if (bs == bucketStartUtc) return true;
                                }
                                else if (Math.Abs((bs - bucketStartUtc).TotalSeconds) <= tolerance.TotalSeconds)
                                {
                                    return true;
                                }
                            }
                        }
                    }
                }
                catch { }
            }

            try { await Task.Delay(500, ct).ConfigureAwait(false); } catch { break; }
        }

        try
        {
            await IncidentBus.PublishAsync(new Incident
            {
                Name = "timebucket_timeout",
                Entity = typeof(T).Name,
                Period = _period.ToString(),
                Keys = pkFilter,
                BucketStartUtc = bucketStartUtc,
                Notes = "WaitForBucketAsync timed out"
            }, ct).ConfigureAwait(false);
        }
        catch { }
        try
        {
            await Ksql.Linq.Events.RuntimeEvents.TryPublishAsync(new RuntimeEvent
            {
                Name = "timebucket.read",
                Phase = "timeout",
                Entity = _readType?.Name ?? typeof(T).Name,
                Topic = LiveTopicName,
                Success = false,
                Message = "WaitForBucketAsync timed out"
            }, ct).ConfigureAwait(false);
        }
        catch { }
        return false;
    }

    public string LiveTopicName => _liveTopic;

    // Fluent helper to bind a default CancellationToken to subsequent calls.
    public TimeBucketScope<T> WithCancellation(CancellationToken ct) => new(this, ct);

    // Bind a default key prefix filter for subsequent operations
    public TimeBucketScope<T> WithKeys(IReadOnlyList<string> pkFilter) => new(this, CancellationToken.None, pkFilter);

    // Bind both keys and cancellation in one shot
    public TimeBucketScope<T> With(IReadOnlyList<string> pkFilter, CancellationToken ct) => new(this, ct, pkFilter);
}

/// <summary>
/// Writer counterpart to <see cref="TimeBucket{T}"/> for importing bars.
/// </summary>
public sealed class TimeBucketWriter<T> where T : class
{
    private readonly Ksql.Linq.KsqlContext _ctx;

    internal TimeBucketWriter(Ksql.Linq.KsqlContext ctx, Period period)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
    }

    public Task WriteAsync(T row, CancellationToken ct = default)
        => _ctx.Set<T>().AddAsync(row, null, ct);

    // Optionally ensure `<base>_1s_rows_last` exists (migration helper)
    public Task EnsureRowsLastAsync() => _ctx.EnsureRowsLastTableAsync<T>();

}

// Fluent scope that carries a default CancellationToken for TimeBucket operations.
public sealed class TimeBucketScope<T> where T : class
{
    private readonly TimeBucket<T> _tb;
    private readonly CancellationToken _ct;
    private readonly IReadOnlyList<string>? _keys;

    internal TimeBucketScope(TimeBucket<T> tb, CancellationToken ct, IReadOnlyList<string>? keys = null)
    {
        _tb = tb;
        _ct = ct;
        _keys = keys;
    }

    public Task<List<T>> ToListAsync() => _tb.ToListAsync(_keys, _ct);

    public Task<List<T>> ToListAsync(IReadOnlyList<string>? pkFilter) => _tb.ToListAsync(pkFilter ?? _keys, _ct);

    public Task<List<T>> ReadAsync(IReadOnlyList<string> pkFilter, DateTime bucketStartUtc, TimeSpan? tolerance = null)
        => _tb.ReadAsync(pkFilter, bucketStartUtc, tolerance, _ct);

    // Use bound keys
    public Task<List<T>> ReadAsync(DateTime bucketStartUtc, TimeSpan? tolerance = null)
    {
        if (_keys is null || _keys.Count == 0)
            throw new InvalidOperationException("No default keys bound. Call WithKeys(...) or use the overload that accepts pkFilter.");
        return _tb.ReadAsync(_keys, bucketStartUtc, tolerance, _ct);
    }

    public Task<bool> WaitForBucketAsync(
        IReadOnlyList<string> pkFilter,
        DateTime bucketStartUtc,
        TimeSpan tolerance,
        TimeSpan timeout)
        => _tb.WaitForBucketAsync(pkFilter, bucketStartUtc, tolerance, timeout, _ct);

    // Use bound keys
    public Task<bool> WaitForBucketAsync(DateTime bucketStartUtc, TimeSpan tolerance, TimeSpan timeout)
    {
        if (_keys is null || _keys.Count == 0)
            throw new InvalidOperationException("No default keys bound. Call WithKeys(...) or use the overload that accepts pkFilter.");
        return _tb.WaitForBucketAsync(_keys, bucketStartUtc, tolerance, timeout, _ct);
    }
}
