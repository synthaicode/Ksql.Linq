using Avro;
using Confluent.Kafka;
using Ksql.Linq.Cache.Core;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.SerDes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Confluent.SchemaRegistry;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using KafkaStreamState = Streamiz.Kafka.Net.KafkaStream.State;
using Avro.Generic;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Ksql.Linq.Events;

namespace Ksql.Linq.Cache.Extensions;

internal static class KsqlContextCacheExtensions
{
    private static readonly Dictionary<IKsqlContext, TableCacheRegistry> _registries = new();
    private static readonly object _lock = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<KafkaStream, bool> _streamRunning
        = new System.Collections.Concurrent.ConcurrentDictionary<KafkaStream, bool>(ReferenceEqualityComparer.Instance);
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, System.Collections.Concurrent.ConcurrentDictionary<string, object>> _memStores
        = new System.Collections.Concurrent.ConcurrentDictionary<string, System.Collections.Concurrent.ConcurrentDictionary<string, object>>(StringComparer.Ordinal);
    // Diagnostics can be enabled by setting env KSQL_LINQ_DIAG=1
    private static readonly bool Diag = string.Equals(
        Environment.GetEnvironmentVariable("KSQL_LINQ_DIAG"),
        "1",
        StringComparison.Ordinal);
    // Reflection-based fallback is enabled by default; set KSQL_LINQ_STREAMIZ_REFLECT=0 to disable.
    private static readonly bool ReflectionFallbackEnabled = !string.Equals(
        Environment.GetEnvironmentVariable("KSQL_LINQ_STREAMIZ_REFLECT"),
        "0",
        StringComparison.Ordinal);

    internal static void UseTableCache(this IKsqlContext context, KsqlDslOptions options, ILoggerFactory? loggerFactory = null)
    {
        lock (_lock)
        {
            loggerFactory ??= NullLoggerFactory.Instance;
            if (!_registries.TryGetValue(context, out var registry))
            {
                registry = new TableCacheRegistry();
                _registries[context] = registry;
            }

            var mapping = ((KsqlContext)context).GetMappingRegistry();
            var models = context.GetEntityModels();
            var anyRequested = options.Entities.Any(e => e.EnableCache);

            var bootstrap = options.Common.BootstrapServers;
            var appIdBase = options.Common.ApplicationId;
            var schemaUrl = options.SchemaRegistry.Url;
            // Reuse existing registry; register or update entries below

            var diag = loggerFactory?.CreateLogger("Ksql.Linq.Cache");

            // 1) Explicitly requested caches via options.Entities
            foreach (var e in options.Entities.Where(e => e.EnableCache))
            {
                var kvp = models.FirstOrDefault(kv => string.Equals(kv.Value.EntityType.Name, e.Entity, StringComparison.OrdinalIgnoreCase));
                var model = kvp.Value;
                if (model == null)
                    continue;

                if (!ShouldRegisterForStreamizCache(model, isExplicit: true))
                {
                    try { diag?.LogDebug("TableCache skip(explicit): entity={Entity} topic={Topic}", model.EntityType.Name, model.GetTopicName()); } catch { }
                    continue;
                }

                var storeName = e.StoreName ?? model.GetTopicName();
                var topic = model.GetTopicName();
                try { diag?.LogDebug("TableCache register(explicit): entity={Entity} topic={Topic} store={Store}", model.EntityType.Name, topic, storeName); } catch { }
                // Explicit entity registration: use the dictionary key when present; otherwise model.EntityType
                var regType = kvp.Key != null && kvp.Key != default(System.Type) ? kvp.Key : model.EntityType;
                RegisterCacheForModel(context, registry, mapping, model, storeName, topic, appIdBase, bootstrap, schemaUrl, loggerFactory, regType);
            }

            // 2) Auto-register caches for derived TABLE entities (e.g., bar_{tf}_live)
            // This covers per-timeframe types used by TimeBucket<T>.
            foreach (var pair in models)
            {
                var model = pair.Value;
                if (!ShouldRegisterForStreamizCache(model, isExplicit: false))
                {
                    try { diag?.LogDebug("TableCache skip(auto): entity={Entity} topic={Topic} kind={Kind}", model.EntityType.Name, model.GetTopicName(), model.GetExplicitStreamTableType()); } catch { }
                    continue;
                }
                var storeName = model.GetTopicName(); // stable per topic
                var topic = model.GetTopicName();
                try { diag?.LogDebug("TableCache register(auto): entity={Entity} topic={Topic} store={Store}", model.EntityType.Name, topic, storeName); } catch { }
                // Auto-registration: register under the POCO entity type so TimeBucket<T> resolves TableCache<T> by DTO
                var regType = model.EntityType;
                RegisterCacheForModel(context,registry, mapping, model, storeName, topic, appIdBase, bootstrap, schemaUrl, loggerFactory, regType);
            }

            context.AttachTableCacheRegistry(registry);
        }
    }

    private static IEnumerable<System.Collections.Generic.KeyValuePair<object, object>> EnumerateInternal<TKey, TValue>(KafkaStream ks, string storeName)
        where TKey : class where TValue : class
    {
        if (_memStores.TryGetValue(storeName, out var dict) && dict != null && !dict.IsEmpty)
        {
            int mc = 0;
            foreach (var kv in dict)
            {
                mc++;
                yield return new System.Collections.Generic.KeyValuePair<object, object>(kv.Key, kv.Value);
            }
            if (Diag)
            {
                try { System.Console.WriteLine($"[cache.enum] MemStore rows={mc} store={storeName}"); } catch { }
                _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "cache.enum", Phase = "mem.rows", Topic = storeName, Success = true, Message = mc.ToString() });
            }
            yield break;
        }
        // For local cache we materialize a string-keyed TABLE; prefer TimestampedKV -> KV paths.
        var tsParams = StoreQueryParameters.FromNameAndType(
            storeName, QueryableStoreTypes.TimestampedKeyValueStore<TKey, TValue>());

        // Buffer results to avoid `yield` inside try/catch
        List<System.Collections.Generic.KeyValuePair<object, object>> tsResults = new();
        bool tsHasAny = false;
        try
        {
            var tsStoreObj = GetStoreWithRetry(ks, tsParams, storeName);
            if (tsStoreObj != null)
            {
                if (Diag)
                {
                    try { System.Console.WriteLine($"[cache.enum] TimestampedKV opened store={storeName} type={tsStoreObj.GetType().Name}"); } catch { }
                    _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "cache.enum", Phase = "tskv.open", Topic = storeName, Success = true, Message = tsStoreObj.GetType().Name });
                }
                var storeType = tsStoreObj.GetType();
                var allMethod = storeType.GetMethod("All", BindingFlags.Public | BindingFlags.Instance);
                if (allMethod != null)
                {
                    var enumerable = allMethod.Invoke(tsStoreObj, Array.Empty<object>()) as System.Collections.IEnumerable;
                    if (enumerable != null)
                    {
                        int c = 0;
                        foreach (var it in enumerable)
                        {
                            var kvType = it.GetType();
                            var keyProp = kvType.GetProperty("Key");
                            var valPropOuter = kvType.GetProperty("Value");
                            var keyObj = keyProp?.GetValue(it);
                            var vo = valPropOuter?.GetValue(it);
                            if (vo != null)
                            {
                                var voType = vo.GetType();
                                var innerProp = voType.GetProperty("Value") ?? voType.GetProperty("value", System.Reflection.BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                                var inner = innerProp != null ? innerProp.GetValue(vo) : vo;
                                c++;
                                tsResults.Add(new System.Collections.Generic.KeyValuePair<object, object>(keyObj!, inner!));
                            }
                            else
                            {
                                c++;
                                tsResults.Add(new System.Collections.Generic.KeyValuePair<object, object>(keyObj!, null!));
                            }
                        }
                        if (Diag)
                        {
                            try { System.Console.WriteLine($"[cache.enum] TimestampedKV rows={c} store={storeName}"); } catch { }
                            _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "cache.enum", Phase = "tskv.rows", Topic = storeName, Success = true, Message = c.ToString() });
                        }
                        if (c > 0)
                            tsHasAny = true;
                    }
                }
            }
        }
        catch
        {
            // swallow and try KV / reflection
        }

        if (tsResults.Count > 0)
        {
            foreach (var kv in tsResults)
                yield return kv;
            if (tsHasAny)
                yield break;
        }

        var parameters = StoreQueryParameters.FromNameAndType(
            storeName, QueryableStoreTypes.KeyValueStore<TKey, TValue>());

        List<System.Collections.Generic.KeyValuePair<object, object>> kvResults = new();
        List<System.Collections.Generic.KeyValuePair<object, object>>? reflResults = null;
        Exception? pending = null;
        try
        {
            var store = GetStoreWithRetry(ks, parameters, storeName);
            if (Diag)
            {
                try { System.Console.WriteLine($"[cache.enum] KV opened store={storeName} type={store?.GetType().Name ?? "<null>"}"); } catch { }
                _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "cache.enum", Phase = "kv.open", Topic = storeName, Success = true, Message = store?.GetType().Name ?? "<null>" });
            }

            if (store == null)
            {
                if (ReflectionFallbackEnabled)
                {
                    var reflectionResults = EnumerateViaReflection<TKey, TValue>(ks, storeName)?.ToList();
                    if (reflectionResults != null && reflectionResults.Count > 0)
                    {
                        reflResults = reflectionResults;
                    }
                    else
                    {
                        pending = new InvalidOperationException($"Streamiz store '{storeName}' resolved to null.");
                    }
                }
                else
                {
                    pending = new InvalidOperationException($"Streamiz store '{storeName}' resolved to null.");
                }
            }
            else
            {
                int c2 = 0;
                var storeType = store.GetType();
                var allMethod = storeType.GetMethod("All", BindingFlags.Public | BindingFlags.Instance);
                if (allMethod == null)
                    throw new InvalidOperationException($"Store type '{storeType.FullName}' does not expose All().");
                var enumerable = allMethod.Invoke(store, Array.Empty<object>()) as System.Collections.IEnumerable;
                if (enumerable != null)
                {
                    foreach (var it in enumerable)
                    {
                        c2++;
                        var kvType = it?.GetType();
                        var keyProp = kvType?.GetProperty("Key");
                        var valProp = kvType?.GetProperty("Value");
                        var keyObj = keyProp?.GetValue(it);
                        var valObj = valProp?.GetValue(it);
                        kvResults.Add(new System.Collections.Generic.KeyValuePair<object, object>(keyObj!, valObj!));
                    }
                }
                if (Diag)
                {
                    try { System.Console.WriteLine($"[cache.enum] KV rows={c2} store={storeName}"); } catch { }
                    _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "cache.enum", Phase = "kv.rows", Topic = storeName, Success = true, Message = c2.ToString() });
                }
                if (c2 == 0 && ReflectionFallbackEnabled)
                {
                    reflResults = EnumerateViaReflection<TKey, TValue>(ks, storeName)?.ToList();
                }
            }
        }
        catch (Exception ex)
        {
            // On any failure, attempt reflection fallback if enabled
            if (ReflectionFallbackEnabled)
            {
                reflResults = EnumerateViaReflection<TKey, TValue>(ks, storeName)?.ToList();
                if (reflResults == null || reflResults.Count == 0)
                    pending = ex;
            }
            else
            {
                pending = ex;
            }
        }

        if (reflResults != null && reflResults.Count > 0)
        {
            foreach (var kv in reflResults)
                yield return kv;
            yield break;
        }

        if (kvResults.Count > 0)
        {
            foreach (var kv in kvResults)
                yield return kv;
            yield break;
        }

        if (pending != null)
        {
            // Propagate original failure when reflection is disabled and KV path failed
            throw pending;
        }
    }

    private static IEnumerable<System.Collections.Generic.KeyValuePair<object, object>>? EnumerateViaReflection<TKey, TValue>(KafkaStream ks, string storeName)
        where TKey : class where TValue : class
    {
        try
        {
            var t = ks.GetType();
            var threadsField = t.GetField("threads", BindingFlags.NonPublic | BindingFlags.Instance);
            if (threadsField == null) return null;
            var threads = threadsField.GetValue(ks) as System.Collections.IEnumerable;
            if (threads == null) return null;
            var results = new List<System.Collections.Generic.KeyValuePair<object, object>>();
            foreach (var th in threads)
            {
                if (th == null) continue;
                var activeTasksProp = th.GetType().GetProperty("ActiveTasks", BindingFlags.Public | BindingFlags.Instance);
                var tasksEnum = activeTasksProp?.GetValue(th) as System.Collections.IEnumerable;
                if (tasksEnum == null) continue;
                foreach (var task in tasksEnum)
                {
                    if (task == null) continue;
                    var getStore = task.GetType().GetMethod("GetStore", BindingFlags.Public | BindingFlags.Instance);
                    var storeObj = getStore?.Invoke(task, new object[] { storeName });
                    if (storeObj == null) continue;
                    var isOpenProp = storeObj.GetType().GetProperty("IsOpen", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    if (isOpenProp != null && isOpenProp.GetValue(storeObj) is bool isOpen && !isOpen) continue;
                    var allMethod = storeObj.GetType().GetMethod("All", BindingFlags.Public | BindingFlags.Instance);
                    if (allMethod == null) continue;
                    var enumerable = allMethod.Invoke(storeObj, Array.Empty<object>()) as System.Collections.IEnumerable;
                    if (enumerable == null) continue;
                    int count = 0;
                    foreach (var it in enumerable)
                    {
                        var kvType = it.GetType();
                        var keyProp = kvType.GetProperty("Key");
                        var valProp = kvType.GetProperty("Value");
                        var keyObj = keyProp?.GetValue(it);
                        var valObj = valProp?.GetValue(it);
                        // unwrap ValueAndTimestamp<V>
                        if (valObj != null)
                        {
                            var vot = valObj.GetType();
                            var innerProp = vot.GetProperty("Value") ?? vot.GetProperty("value", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                            if (innerProp != null)
                                valObj = innerProp.GetValue(valObj);
                        }
                        results.Add(new System.Collections.Generic.KeyValuePair<object, object>(keyObj!, valObj!));
                        count++;
                    }
                    if (count > 0)
                        return results;
                }
            }
        }
        catch { }
        return null;
    }

    private static dynamic? GetStoreWithRetry(KafkaStream ks, object parameters, string storeName, int maxAttempts = 30, int delayMs = 250)
    {
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                return ((dynamic)ks).Store((dynamic)parameters);
            }
            catch (Streamiz.Kafka.Net.Errors.InvalidStateStoreException ex)
            {
                if (Diag)
                {
                    try { System.Console.WriteLine($"[cache.retry] attempt={attempt} store={storeName} msg={ex.Message}"); } catch { }
                    _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "cache.error", Phase = "retry", Topic = storeName, Success = false, Message = ex.Message });
                }
                System.Threading.Thread.Sleep(delayMs);
                continue;
            }
        }
        // Final attempt without catch to bubble the error
        return ((dynamic)ks).Store((dynamic)parameters);
    }

    // Eligible table registration is delegated to TableCacheRegistry via configured registrar.

    private static void RegisterCacheForModel(
        IKsqlContext context,
        TableCacheRegistry registry,
        Mapping.MappingRegistry mapping,
        EntityModel model,
        string storeName,
        string topic,
        string appIdBase,
        string bootstrap,
        string schemaUrl,
        ILoggerFactory? loggerFactory,
        Type? registerTypeOverride = null)
    {
        var kv = mapping.GetMapping(model.EntityType);
        AlignKeyMappingWithSchema(context, schemaUrl, topic, kv, model, loggerFactory);
        AlignValueMappingWithSchema(context, schemaUrl, topic, kv, model, loggerFactory);
        var applicationId = $"{appIdBase}-{storeName}";
        var stateDir = Path.Combine(Path.GetTempPath(), applicationId);
        try
        {
            if (Directory.Exists(stateDir))
                Directory.Delete(stateDir, recursive: true);
        }
        catch { }

        var windowSizeMs = ResolveWindowSizeMs(model);
        // Tumbling/Windowed topics must use GenericRecord key with FixedTimeWindowedAvroSerDes
        // to avoid schema mismatch and Streamiz TimeWindowedSerDes bugs.
        var avroKeyType = windowSizeMs.HasValue
            ? typeof(Avro.Generic.GenericRecord)
            : (kv.AvroKeyType ?? typeof(Avro.Generic.GenericRecord));
        var streamKeyType = windowSizeMs.HasValue
            ? typeof(Streamiz.Kafka.Net.State.Windowed<>).MakeGenericType(avroKeyType)
            : avroKeyType;

        var valueAvroType = typeof(Avro.Generic.GenericRecord);
        kv.AvroValueType = valueAvroType;

        var builder = new StreamBuilder();
        var materialized = CreateStringKeyMaterializedGeneric(valueAvroType, storeName);
        var partitions = model.Partitions > 0 ? model.Partitions : 1;
        if (Diag) { try { System.Console.WriteLine($"[stream.cache] build store={storeName} topic={topic} windowMs={(windowSizeMs.HasValue ? windowSizeMs.Value.ToString() : "null")}"); } catch { } }
        StreamToStringKeyTableGeneric(builder, streamKeyType, avroKeyType, valueAvroType, topic, materialized, kv, windowSizeMs, partitions, storeName);

        var config = CreateStreamConfigGeneric(streamKeyType, avroKeyType, valueAvroType, applicationId, bootstrap, schemaUrl, stateDir, loggerFactory, windowSizeMs);
        var ks = new KafkaStream(builder.Build(), (IStreamConfig)config);
        var diagLogger = loggerFactory?.CreateLogger("Ksql.Linq.Cache");
        ks.StateChanged += (_, s) =>
        {
            try { diagLogger?.LogDebug("KafkaStream state changed: app={AppId} topic={Topic} state={State}", applicationId, topic, s); } catch { }
            try
            {
                if (s == KafkaStream.State.RUNNING)
                {
                    RuntimeEventBus.PublishAsync(new RuntimeEvent
                    {
                        Name = "streamiz.state",
                        Phase = "running",
                        Entity = model.EntityType?.Name,
                        Topic = topic,
                        AppId = applicationId,
                        State = "RUNNING",
                        Success = true,
                        Message = "KafkaStream reached RUNNING"
                    });
                }
                else if (s == KafkaStream.State.ERROR)
                {
                    RuntimeEventBus.PublishAsync(new RuntimeEvent
                    {
                        Name = "streamiz.state",
                        Phase = "error",
                        Entity = model.EntityType?.Name,
                        Topic = topic,
                        AppId = applicationId,
                        State = "ERROR",
                        Success = false,
                        Message = "KafkaStream entered ERROR"
                    });
                }
                else if (s == KafkaStream.State.NOT_RUNNING)
                {
                    RuntimeEventBus.PublishAsync(new RuntimeEvent
                    {
                        Name = "streamiz.state",
                        Phase = "not_running",
                        Entity = model.EntityType?.Name,
                        Topic = topic,
                        AppId = applicationId,
                        State = "NOT_RUNNING",
                        Success = false,
                        Message = "KafkaStream is NOT_RUNNING"
                    });
                }
            }
            catch { }
        };
        var wait = CreateWaitUntilRunning(ks, schemaUrl, topic, storeName, kv.AvroValueType!, loggerFactory);
        var enumerateLazy = CreateEnumeratorLazyGeneric(typeof(string), kv.AvroValueType!, ks, storeName);

        var cache = CreateTableCacheGeneric(model.EntityType, mapping, storeName, wait, enumerateLazy);

        // Diagnostic: probe RocksDB state without key filter to confirm arrival (keyless scan)
        try
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(300).ConfigureAwait(false);
                    var f = enumerateLazy.Value;
                    int count = 0; int samples = 0;
                    foreach (var kvp in f())
                    {
                        count++;
                        if (samples < 3)
                        {
                            samples++;
                            var keyS = kvp.Key?.ToString() ?? "<null>";
                            var valS = kvp.Value?.ToString() ?? "<null>";
                            if (Diag) { try { System.Console.WriteLine($"[rocks.probe] store={storeName} key={keyS.Replace('\u0000','|')} value={valS}"); } catch { } }
                        }
                    }
                    if (Diag)
                    {
                        try { System.Console.WriteLine($"[rocks.probe] store={storeName} rows={count}"); } catch { }
                        try { _ = RuntimeEventBus.PublishAsync(new RuntimeEvent { Name = "rocks.probe", Phase = "rows", Topic = storeName, Success = true, Message = count.ToString() }); } catch { }
                    }
                }
                catch { }
            });
        }
        catch { }

        // Register cache under the concrete model type key used by callers.
        // For derived timeframe TABLEs, the context entity-model registry key is the dynamic "dt" type,
        // while model.EntityType may be typeof(object). Allow an override to bind correctly.
        var regType = registerTypeOverride ?? model.EntityType;
        registry.Register(regType, cache);
        try
        {
            _ = RuntimeEventBus.PublishAsync(new RuntimeEvent
            {
                Name = "streamiz.register",
                Phase = "init",
                Entity = model.EntityType?.Name,
                Topic = topic,
                AppId = applicationId,
                Success = true,
                Message = $"store={storeName}"
            });
        }
        catch { }

        // Fire-and-forget: trigger an initial snapshot read to materialize cache and emit cache.* diagnostics
        try
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    // small settle before initial snapshot
                    try { await Task.Delay(200).ConfigureAwait(false); } catch { }
                    var toList = cache.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) });
                    if (toList != null)
                    {
                        var taskObj = toList.Invoke(cache, new object?[] { null, TimeSpan.FromSeconds(5) });
                        if (taskObj is Task t) await t.ConfigureAwait(false);
                    }
                }
                catch { }
            });
        }
        catch { }
        // Track the KafkaStream lifetime and dispose it when the context is disposed
        try { registry.RegisterResource(new SafeKafkaStreamResource(ks)); } catch { }
        try { registry.RegisterStateDir(stateDir); } catch { }
        // Do not start immediately. Start lazily within the wait delegate to avoid
        // racing ksqlDB subject/DDL readiness that can push KafkaStreams into ERROR.
    }

    // 繝ｬ繧ｸ繧ｹ繝医Λ逕滓・繝倥Ν繝代・縺ｯ荳崎ｦ・ｼ亥・縺ｮ繧ｷ繝ｳ繝励Ν螳溯｣・↓謌ｻ縺吶◆繧∝炎髯､・・
    private sealed class SafeKafkaStreamResource : IDisposable
    {
        private readonly KafkaStream _stream;
        public SafeKafkaStreamResource(KafkaStream stream) => _stream = stream;
        public void Dispose()
        {
            try
            {
                var shouldDispose = false;
                try { shouldDispose = _streamRunning.TryGetValue(_stream, out var run) && run; } catch { }
                if (!shouldDispose)
                    return;
                _stream?.Dispose();
            }
            catch { }
        }
    }
    private static Func<TimeSpan?, Task> CreateWaitUntilRunning(KafkaStream stream, string schemaUrl, string topic, string storeName, Type valueType, ILoggerFactory? loggerFactory)
    {
        var running = false;
        var started = false;
        var startLock = new object();

        stream.StateChanged += (_, s) =>
        {
            // Track RUNNING; if an ERROR occurs, allow retry by clearing the started flag
            if (s == KafkaStream.State.RUNNING)
            {
                running = true;
                try { _streamRunning[stream] = true; } catch { }
            }
            else if (s == KafkaStream.State.ERROR || s == KafkaStream.State.NOT_RUNNING)
            {
                // allow subsequent StartAsync attempts
                started = false;
            }
        };

        // If we've seen this stream reach RUNNING earlier, short-circuit waits
        try { running = _streamRunning.TryGetValue(stream, out var run) && run; } catch { running = false; }

        return async (TimeSpan? timeout) =>
        {
            var until = timeout.HasValue ? DateTime.UtcNow + timeout.Value : (DateTime?)null;
            // If not yet RUNNING, wait until it is
            while (!running)
            {
                // Preflight: ensure SR subjects exist to avoid early ERROR due to missing key/value schemas
                try
                {
                    if (!string.IsNullOrWhiteSpace(schemaUrl))
                    {
                        var ok = await WaitForSchemaSubjectsAsync(schemaUrl, topic, until).ConfigureAwait(false);
                        if (!ok && until.HasValue && DateTime.UtcNow >= until.Value)
                            throw new TimeoutException($"Schema subjects for {topic} not ready");
                    }
                }
                catch { }

                // Lazy-start or re-start when needed
                if (!started)
                {
                    lock (startLock)
                    {
                        if (!started)
                        {
                            started = true;
                            try
                            {
                                _ = RuntimeEventBus.PublishAsync(new RuntimeEvent
                                {
                                    Name = "streamiz.start",
                                    Phase = "begin",
                                    Topic = topic,
                                    Success = true,
                                    Message = "KafkaStream.StartAsync()"
                                });
                            }
                            catch { }
                            _ = stream.StartAsync();
                        }
                    }
                }

                if (running) break;
                if (until.HasValue && DateTime.UtcNow >= until.Value)
                {
                    try
                    {
                        await RuntimeEventBus.PublishAsync(new RuntimeEvent
                        {
                            Name = "streamiz.wait",
                            Phase = "timeout",
                            Topic = topic,
                            Success = false,
                            Message = "KafkaStream failed to reach RUNNING state within timeout"
                        }).ConfigureAwait(false);
                    }
                    catch { }
                    throw new TimeoutException("KafkaStream failed to reach RUNNING state within timeout");
                }
                try { await Task.Delay(250).ConfigureAwait(false); } catch { }
            }

            // Once RUNNING (or if it was already), probe until state store becomes queryable to avoid early InvalidStateStoreException
            var probed = false;
            while (!probed)
            {
                try
                {
                    var sqpType = typeof(StoreQueryParameters);
                    var fromNameAndType = sqpType.GetMethod("FromNameAndType", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

                    // 1) Try timestamped KV store first
                    try
                    {
                        var tsMethod = typeof(QueryableStoreTypes).GetMethod("TimestampedKeyValueStore", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                        var tsGeneric = tsMethod!.MakeGenericMethod(typeof(string), valueType);
                        var tsInstance = tsGeneric.Invoke(null, null);
                        var tsParams = fromNameAndType!.Invoke(null, new object?[] { storeName, tsInstance! });
                        _ = stream.Store((dynamic)tsParams!);
                        probed = true;
                    }
                    catch { probed = false; }

                    if (!probed)
                    {
                        // 2) Fallback to plain KV store
                        var qstMethod = typeof(QueryableStoreTypes).GetMethod("KeyValueStore", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                        var qstGeneric = qstMethod!.MakeGenericMethod(typeof(string), valueType);
                        var qstInstance = qstGeneric.Invoke(null, null);
                        var parameters = fromNameAndType!.Invoke(null, new object?[] { storeName, qstInstance! });
                        _ = stream.Store((dynamic)parameters!);
                        probed = true;
                    }

                    try
                    {
                        await RuntimeEventBus.PublishAsync(new RuntimeEvent
                        {
                            Name = "store.ready",
                            Phase = "open",
                            Topic = topic,
                            Success = true,
                            Message = $"store={storeName}"
                        }).ConfigureAwait(false);
                    }
                    catch { }
                    break;
                }
                catch
                {
                    if (until.HasValue && DateTime.UtcNow >= until.Value)
                        break;
                    try { await Task.Delay(250).ConfigureAwait(false); } catch { break; }
                }
            }
        };
    }

    private static async Task<bool> WaitForSchemaSubjectsAsync(string schemaUrl, string topic, DateTime? until)
    {
        try
        {
            var config = new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = schemaUrl };
            using var client = new Confluent.SchemaRegistry.CachedSchemaRegistryClient(config);
            var keySubject = $"{topic}-key";
            var valueSubject = $"{topic}-value";
            while (true)
            {
                try
                {
                    var keyTask = client.GetLatestSchemaAsync(keySubject);
                    var valTask = client.GetLatestSchemaAsync(valueSubject);
                    await Task.WhenAll(keyTask, valTask).ConfigureAwait(false);
                    return true;
                }
                catch (Confluent.SchemaRegistry.SchemaRegistryException)
                {
                    if (until.HasValue && DateTime.UtcNow >= until.Value)
                        return false;
                    await Task.Delay(250).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            return false;
        }
    }

    // Wrap ks.Store(...).All() in a type-safe enumerator function
    private static Lazy<Func<IEnumerable<System.Collections.Generic.KeyValuePair<object, object>>>> CreateEnumeratorLazyGeneric(
        Type keyType, Type valueType, KafkaStream ks, string storeName)
    {
        var m = typeof(KsqlContextCacheExtensions)
                 .GetMethod(nameof(CreateEnumeratorLazy), BindingFlags.NonPublic | BindingFlags.Static)!;
        return (Lazy<Func<IEnumerable<System.Collections.Generic.KeyValuePair<object, object>>>>)
                m.MakeGenericMethod(keyType, valueType)
             .Invoke(null, new object[] { ks, storeName })!;
    }

    private static Lazy<Func<IEnumerable<System.Collections.Generic.KeyValuePair<object, object>>>> CreateEnumeratorLazy<TKey, TValue>(
        KafkaStream ks, string storeName)
        where TKey : class where TValue : class
    {
        return new Lazy<Func<IEnumerable<System.Collections.Generic.KeyValuePair<object, object>>>>(() => () => EnumerateInternal<TKey, TValue>(ks, storeName));
    }
    private static object CreateStreamConfigGeneric(
        Type streamKeyType,
        Type avroKeyType,
        Type valueType,
        string appId,
        string bootstrap,
        string schemaUrl,
        string stateDir,
        ILoggerFactory? loggerFactory,
        long? windowSizeMs)
    {
        var keySerDesType = typeof(SchemaAvroSerDes<>).MakeGenericType(avroKeyType);
        var valueSerDesType = typeof(SchemaAvroSerDes<>).MakeGenericType(valueType);

        var cfgType = typeof(StreamConfig<,>).MakeGenericType(keySerDesType, valueSerDesType);
        var cfg = Activator.CreateInstance(cfgType)!;

        SetProperty(cfg, cfgType, "ApplicationId", appId);
        SetProperty(cfg, cfgType, "BootstrapServers", bootstrap);
        SetProperty(cfg, cfgType, "SchemaRegistryUrl", schemaUrl);
        SetProperty(cfg, cfgType, "StateDir", stateDir);
        SetProperty(cfg, cfgType, "AutoOffsetReset", AutoOffsetReset.Earliest);
        SetProperty(cfg, cfgType, "Logger", loggerFactory);
        SetProperty(cfg, cfgType, "LogLevel", Microsoft.Extensions.Logging.LogLevel.Debug, optional: true);
        SetProperty(cfg, cfgType, "CommitIntervalMs", 100, optional: true);
        // Disable state store caching to make updates immediately visible to interactive queries
        SetProperty(cfg, cfgType, "DefaultStateStoreCacheMaxBytes", 0L, optional: true);
        // Legacy/Java compatibility key (no-op in Streamiz):
        SetProperty(cfg, cfgType, "CacheMaxBytesBuffering", 0L, optional: true);
        // Reduce startup delay to surface state quickly
        SetProperty(cfg, cfgType, "StartTaskDelayMs", 0, optional: true);

        // Explicit low-latency settings for interactive reads
        // Keep AT_LEAST_ONCE to avoid transactional delays that slow visibility
        SetProperty(cfg, cfgType, "Guarantee", ProcessingGuarantee.AT_LEAST_ONCE, optional: true);
        // Prefer reading uncommitted data to minimize lag until commit
        SetProperty(cfg, cfgType, "IsolationLevel", IsolationLevel.ReadUncommitted, optional: true);
        // Poll more frequently to shrink reaction time to new records
        SetProperty(cfg, cfgType, "PollMs", 10L, optional: true);
        // Single-thread processing for deterministic behavior in tests (topics are single-partition)
        SetProperty(cfg, cfgType, "NumStreamThreads", 1, optional: true);

        // Emit diagnostics about effective config (best-effort)
        try
        {
            var lf = loggerFactory ?? NullLoggerFactory.Instance;
            var logger = lf.CreateLogger("Ksql.Linq.Cache");
            var pollMs = GetProperty(cfg, cfgType, "PollMs");
            var iso = GetProperty(cfg, cfgType, "IsolationLevel");
            var guar = GetProperty(cfg, cfgType, "Guarantee");
            var threads = GetProperty(cfg, cfgType, "NumStreamThreads");
            var cacheBytes = GetProperty(cfg, cfgType, "DefaultStateStoreCacheMaxBytes");
            var commit = GetProperty(cfg, cfgType, "CommitIntervalMs");
            var bootstrapServers = GetProperty(cfg, cfgType, "BootstrapServers");
            var stateDirCfg = GetProperty(cfg, cfgType, "StateDir");
            logger.LogDebug(
                "StreamizConfig appId={AppId} stateDir={StateDir} bootstrap={Bootstrap} pollMs={PollMs} isolation={Isolation} guarantee={Guarantee} threads={Threads} cacheBytes={CacheBytes} commitMs={Commit}",
                appId, stateDirCfg, bootstrapServers, pollMs, iso, guar, threads, cacheBytes, commit);

            // Full dump of public properties
            try
            {
                var dump = new System.Collections.Generic.Dictionary<string, object?>(System.StringComparer.OrdinalIgnoreCase);
                foreach (var p in cfgType.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public))
                {
                    if (!p.CanRead) continue;
                    object? val = null;
                    try { val = p.GetValue(cfg); } catch { }
                    if (val == null)
                    {
                        dump[p.Name] = null;
                    }
                    else
                    {
                        var t = val.GetType();
                        dump[p.Name] = (t.IsPrimitive || t == typeof(string) || t.IsEnum) ? val : t.FullName;
                    }
                }
                var json = System.Text.Json.JsonSerializer.Serialize(dump);
                logger.LogDebug("StreamizConfigDump appId={AppId} cfg={Cfg}", appId, json);
            }
            catch { }
        }
        catch { }

        // Do not set DefaultKeySerDes globally. We pass explicit key serdes to source/repartition/materialization.

        var defaultValueSerde = WrapWithTombstoneSafeSerDes(Activator.CreateInstance(valueSerDesType)!);

        SetProperty(cfg, cfgType, "DefaultValueSerDes", defaultValueSerde, optional: true);

        // Avoid setting DefaultKeySerDes even for windowed keys; explicit serdes are supplied per operator.

        return cfg;
    }

    private static object CreateStringKeyMaterializedGeneric(Type valueType, string storeName)
    {
        var m = typeof(KsqlContextCacheExtensions)
            .GetMethod(nameof(CreateStringKeyMaterialized), BindingFlags.NonPublic | BindingFlags.Static)!;
        return m.MakeGenericMethod(valueType).Invoke(null, new object[] { storeName })!;
    }

    private static Materialized<string, TValue, IKeyValueStore<Bytes, byte[]>> CreateStringKeyMaterialized<TValue>(string storeName)
    {
        return Materialized<string, TValue, IKeyValueStore<Bytes, byte[]>>.Create<
            StringSerDes, SchemaAvroSerDes<TValue>>(storeName);
    }

    private static void StreamToStringKeyTableGeneric(
        StreamBuilder builder,
        Type streamKeyType,
        Type avroKeyType,
        Type valueType,
        string topic,
        object materialized,
        object mapping,
        long? windowSizeMs,
        int partitions,
        string storeName)
    {
        var m = typeof(KsqlContextCacheExtensions)
            .GetMethod(nameof(StreamToStringKeyTable), BindingFlags.NonPublic | BindingFlags.Static)!;
        m.MakeGenericMethod(streamKeyType, valueType)
         .Invoke(null, new object?[] { builder, topic, materialized, mapping, avroKeyType, windowSizeMs, partitions, storeName });
    }

    private static void StreamToStringKeyTable<TKey, TValue>(
        StreamBuilder builder, string topic,
        Materialized<string, TValue, IKeyValueStore<Bytes, byte[]>> materialized,
        object mapping,
        Type avroKeyType,
        long? windowSizeMs,
        int partitions,
        string storeName)
        where TKey : class where TValue : class
    {
        var formatKey = (Func<object, string>)(k =>
            (string)mapping.GetType().GetMethod("FormatKeyForPrefix")!.Invoke(mapping, new[] { k })!);

        ISerDes<TKey> keySerde;
        if (windowSizeMs.HasValue)
        {
            var innerSerde = Activator.CreateInstance(typeof(SchemaAvroSerDes<>).MakeGenericType(avroKeyType))
                ?? throw new InvalidOperationException("Failed to create inner Avro key serdes.");
            if (avroKeyType == typeof(Avro.Generic.GenericRecord))
            {
                // Use fixed-time windowed Avro serdes to handle both topic/store key encodings deterministically
                var fixedSerde = new Ksql.Linq.SerDes.FixedTimeWindowedAvroSerDes((ISerDes<Avro.Generic.GenericRecord>)innerSerde, windowSizeMs.Value);
                if (fixedSerde is not ISerDes<TKey> fixedTyped)
                    throw new InvalidOperationException($"FixedTimeWindowedAvroSerDes is not assignable to {typeof(ISerDes<TKey>).FullName}.");
                keySerde = fixedTyped;
            }
            else
            {
                var timeWindowedType = typeof(TimeWindowedSerDes<>).MakeGenericType(avroKeyType);
                var windowedSerde = CreateTimeWindowedSerde(timeWindowedType, innerSerde, windowSizeMs.Value)
                    ?? throw new InvalidOperationException("Failed to create TimeWindowedSerDes for the supplied window.");
                if (windowedSerde is not ISerDes<TKey> typedSerde)
                    throw new InvalidOperationException($"TimeWindowedSerDes {timeWindowedType.FullName} is not assignable to {typeof(ISerDes<TKey>).FullName}.");
                keySerde = typedSerde;
            }
        }
        else
        {
            keySerde = (ISerDes<TKey>)Activator.CreateInstance(
                typeof(SchemaAvroSerDes<>).MakeGenericType(avroKeyType))!;
        }

        keySerde = (ISerDes<TKey>)WrapWithTombstoneSafeSerDes(keySerde);

        var valueSerde = (ISerDes<TValue>)WrapWithTombstoneSafeSerDes(new SchemaAvroSerDes<TValue>());
        try
        {
            var keySerdeName = keySerde?.GetType().FullName ?? "(null)";
            var valueSerdeName = valueSerde?.GetType().FullName ?? "(null)";
            var lf = (GetProperty(materialized, materialized.GetType(), "LoggerFactory") as ILoggerFactory) ?? NullLoggerFactory.Instance;
            var logger = lf.CreateLogger("Ksql.Linq.Cache");
            logger.LogDebug("CacheSerDes topic={Topic} windowMs={WindowMs} keySerde={KeySerde} avroKey={AvroKey} valueSerde={ValueSerde}",
                topic,
                windowSizeMs.HasValue ? windowSizeMs.Value.ToString() : "null",
                keySerdeName,
                avroKeyType?.FullName,
                valueSerdeName);
        }
        catch { }
        var stream = builder.Stream(topic, keySerde, valueSerde);
        if (Diag) { try { System.Console.WriteLine($"[stream.cache] source topic={topic} keySerde={keySerde?.GetType().Name} valueSerde={valueSerde?.GetType().Name}"); } catch { } }

        var isWindowed = windowSizeMs.HasValue;
        var keyAccessor = CreateKeyAccessor<TKey>(isWindowed);
        var sanitizedStream = stream.Filter((key, value, _) => keyAccessor(key) != null);
        var keyFormatter = CreateKeyFormatter<TKey>(formatKey, keyAccessor, isWindowed);
        var withStringKey = sanitizedStream.SelectKey(new Mapper<TKey, TValue>(keyFormatter));

        var repartitioned = withStringKey.Repartition(
            Repartitioned<string, TValue>.As($"{topic}-by-stringkey")
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes((ISerDes<TValue>)WrapWithTombstoneSafeSerDes(new SchemaAvroSerDes<TValue>()))
                .WithNumberOfPartitions(partitions));

        _ = repartitioned.ToTable(materialized);

        // Inline MemStore for fast local reads; log first few keys for verification
        withStringKey.Foreach((k, v, ctx) =>
        {
            if (k == null)
                return;
            try
            {
                var dict = _memStores.GetOrAdd(storeName, _ => new System.Collections.Concurrent.ConcurrentDictionary<string, object>(StringComparer.Ordinal));
                dict[k] = v!;
                if (Diag && dict.Count <= 3)
                {
                    try { System.Console.WriteLine($"[mem.put] store={storeName} key={k.Replace('\u0000','|')} valueType={typeof(TValue).Name}"); } catch { }
                }
            }
            catch { }
        });
    }

    private static Func<TKey, string> CreateKeyFormatter<TKey>(Func<object, string> formatter, Func<TKey, object?> keyAccessor, bool isWindowed)
        where TKey : class
    {
        return key =>
        {
            var inner = keyAccessor(key);
            if (inner == null)
                throw new InvalidOperationException("Unable to format cache key because the underlying key is null after filtering.");

            var baseKey = formatter(inner);
            if (!isWindowed)
                return baseKey;

            try
            {
                // TKey is expected to be Streamiz Windowed<AvroKey> type
                // Append window start (ms) to preserve per-window history in the string key
                var winProp = typeof(TKey).GetProperty("Window", BindingFlags.Public | BindingFlags.Instance)
                             ?? typeof(TKey).GetProperty("window", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                if (winProp != null)
                {
                    var winObj = winProp.GetValue(key);
                    if (winObj != null)
                    {
                        // Try StartMs first; fallback to Start (DateTime or long)
                        var startMsProp = winObj.GetType().GetProperty("StartMs", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                        long ms;
                        if (startMsProp != null && startMsProp.GetValue(winObj) is long lms)
                        {
                            ms = lms;
                        }
                        else
                        {
                            var startProp = winObj.GetType().GetProperty("Start", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                            var sv = startProp?.GetValue(winObj);
                            if (sv is DateTime dt) ms = new DateTimeOffset(dt).ToUnixTimeMilliseconds();
                            else if (sv is long l2) ms = l2;
                            else ms = 0;
                        }
                        var sep = Ksql.Linq.Mapping.KeyValueTypeMapping.KeySep;
                        return baseKey + sep + ms.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    }
                }
            }
            catch { }
            return baseKey;
        };
    }

    private static Func<TKey, object?> CreateKeyAccessor<TKey>(bool isWindowed)
        where TKey : class
    {
        if (!isWindowed)
        {
            return key => key;
        }

        var keyProperty = typeof(TKey).GetProperty("Key", BindingFlags.Public | BindingFlags.Instance)
            ?? throw new InvalidOperationException($"Windowed key type {typeof(TKey).FullName} does not expose a Key property.");

        return key =>
        {
            if (key == null)
            {
                return null;
            }

            return keyProperty.GetValue(key);
        };
    }
    private static object WrapWithTombstoneSafeSerDes(object serDes)

    {

        if (serDes == null)

        {

            return serDes!;

        }

        var targetInterface = serDes.GetType().GetInterfaces()

            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISerDes<>));

        if (targetInterface == null)

        {

            return serDes;

        }

        var valueType = targetInterface.GetGenericArguments()[0];

        var wrapperType = typeof(TombstoneSafeSerDes<>).MakeGenericType(valueType);

        return Activator.CreateInstance(wrapperType, serDes)!;

    }

    private class Mapper<TKeyLocal, TValueLocal> : IKeyValueMapper<TKeyLocal, TValueLocal, string>
    {
        private readonly Func<TKeyLocal, string> _f;
        public Mapper(Func<TKeyLocal, string> f) => _f = f;
        public string Apply(TKeyLocal key, TValueLocal value, IRecordContext context) => _f(key);
    }

    private static object CreateTableCacheGeneric(Type entityType, MappingRegistry mapping,
        string storeName, Func<TimeSpan?, Task> wait,
        Lazy<Func<IEnumerable<System.Collections.Generic.KeyValuePair<object, object>>>> enumerateLazy)
    {
        var cacheType = typeof(TableCache<>).MakeGenericType(entityType);
        return Activator.CreateInstance(cacheType, mapping, storeName, wait, enumerateLazy)!;
    }

    internal static void AttachTableCacheRegistry(this IKsqlContext context, TableCacheRegistry registry)
    {
        _registries[context] = registry;
    }

    internal static TableCacheRegistry? GetTableCacheRegistry(this IKsqlContext context)
    {
        lock (_lock)
        {
            return _registries.TryGetValue(context, out var reg) ? reg : null;
        }
    }

    internal static void ResetTableCache(this IKsqlContext context)
    {
        lock (_lock)
        {
            if (_registries.TryGetValue(context, out var reg))
            {
                try { reg.Clear(deleteStateDirs: false); } catch { }
                _registries.Remove(context);
            }
        }
    }

    internal static ITableCache<T>? GetTableCache<T>(this IKsqlContext context) where T : class
    {
        var reg = context.GetTableCacheRegistry();
        return reg?.GetCache<T>();
    }

    private static bool ShouldRegisterForStreamizCache(EntityModel model, bool isExplicit)
    {
        // Skip abstract/unresolved/basic types
        var et = model.EntityType;
        if (et == null || et == typeof(object) || et.IsAbstract || et.IsInterface)
            return false;

        // Only register timeframe-derived TABLEs (e.g., bar_1m_live). Base DTOs are not eligible.
        if (model.GetExplicitStreamTableType() != Ksql.Linq.Query.Abstractions.StreamTableType.Table)
            return false;

        if (TryGetTimeframeMetadata(model, out _, out var role))
            return IsEligibleTimeframeRole(role);

        // No registration for non-timeframe models, even if explicitly requested.
        return false;
    }

    private static bool TryGetTimeframeMetadata(EntityModel model, out string timeframe, out string role)
    {
        timeframe = string.Empty;
        role = string.Empty;

        var metadata = model.GetOrCreateMetadata();
        if (!string.IsNullOrWhiteSpace(metadata.TimeframeRaw) && !string.IsNullOrWhiteSpace(metadata.Role))
        {
            timeframe = metadata.TimeframeRaw!;
            role = metadata.Role!;
            return true;
        }

        return false;
    }

    private static bool IsEligibleTimeframeRole(string role)
    {
        // Only Live timeframe TABLEs are eligible; legacy Prev/Hb/Fill removed
        return role.Equals("Live", StringComparison.OrdinalIgnoreCase);
    }

    private static long? ResolveWindowSizeMs(EntityModel model)
    {
        var metadata = model.GetOrCreateMetadata();
        var tfValue = metadata.TimeframeRaw;
        if (string.IsNullOrWhiteSpace(tfValue))
            return null;

        var roleValue = metadata.Role;
        if (!string.IsNullOrWhiteSpace(roleValue)
            && roleValue.Equals("Final1sStream", StringComparison.OrdinalIgnoreCase))
            return null;

        return Ksql.Linq.Query.Builders.Common.TimeframeUtils.TryToMilliseconds(tfValue, out var ms) ? ms : null;
    }

    private static void AlignKeyMappingWithSchema(IKsqlContext context, string schemaUrl, string topic, KeyValueTypeMapping mapping, EntityModel model, ILoggerFactory? loggerFactory)
    {
        if (string.IsNullOrWhiteSpace(schemaUrl))
            return;
        if (mapping == null || mapping.KeyProperties.Length == 0)
            return;

        try
        {
            var config = new SchemaRegistryConfig { Url = schemaUrl };
            using var client = new CachedSchemaRegistryClient(config);
            var subject = $"{topic}-key";
            var metadata = client.GetLatestSchemaAsync(subject).GetAwaiter().GetResult();
            var schemaString = metadata?.SchemaString;
            if (string.IsNullOrWhiteSpace(schemaString))
                return;

            if (Avro.Schema.Parse(schemaString) is not RecordSchema recordSchema)
                return;

            var existing = mapping.KeyProperties;
            if (existing == null || existing.Length == 0)
            {
                mapping.AvroKeySchema = schemaString;
                mapping.AvroKeyRecordSchema = recordSchema;
                if (string.IsNullOrWhiteSpace(model.KeySchemaFullName))
                    model.KeySchemaFullName = recordSchema.Fullname;
                return;
            }

            var source = existing ?? Array.Empty<PropertyMeta>();
            var updated = new PropertyMeta[source.Length];
            for (int i = 0; i < source.Length; i++)
            {
                var meta = source[i];
                var candidate = meta.SourceName ?? meta.PropertyInfo?.Name ?? meta.Name;
                var field = recordSchema.Fields.FirstOrDefault(f => string.Equals(f.Name, candidate, StringComparison.Ordinal))
                    ?? recordSchema.Fields.FirstOrDefault(f => string.Equals(f.Name, candidate, StringComparison.OrdinalIgnoreCase))
                    ?? (i < recordSchema.Fields.Count ? recordSchema.Fields[i] : null);

                if (field == null)
                {
                    updated[i] = meta;
                    continue;
                }

                updated[i] = new PropertyMeta
                {
                    Name = meta.Name,
                    SourceName = field.Name,
                    PropertyType = meta.PropertyType,
                    IsNullable = meta.IsNullable,
                    Precision = meta.Precision,
                    Scale = meta.Scale,
                    Format = meta.Format,
                    Attributes = meta.Attributes,
                    PropertyInfo = meta.PropertyInfo,
                    IsAutoGenerated = meta.IsAutoGenerated
                };
            }

            mapping.KeyProperties = updated;
            mapping.AvroKeySchema = schemaString;
            mapping.AvroKeyRecordSchema = recordSchema;
            if (string.IsNullOrWhiteSpace(model.KeySchemaFullName))
                model.KeySchemaFullName = recordSchema.Fullname;

            try
            {
                var logger = loggerFactory?.CreateLogger(typeof(KsqlContextCacheExtensions));
                var fields = string.Join(",", recordSchema.Fields.Select(f => f.Name));
                logger?.LogDebug("Aligned KEY schema from SR: subject={Subject} fields=[{Fields}]", subject, fields);
            }
            catch { }
        }
        catch (SchemaRegistryException ex) when (ex.ErrorCode == 404 || ex.ErrorCode == 40401)
        {
            loggerFactory?.CreateLogger(typeof(KsqlContextCacheExtensions))?.LogDebug(ex, "Schema subject {Subject} not found while aligning cache mapping.", $"{topic}-key");
        }
        catch (Exception ex)
        {
            loggerFactory?.CreateLogger(typeof(KsqlContextCacheExtensions))?.LogDebug(ex, "Failed to align cache mapping for {Topic}", topic);
        }
    }

    private static void AlignValueMappingWithSchema(
        IKsqlContext context,
        string schemaUrl,
        string topic,
        KeyValueTypeMapping mapping,
        EntityModel model,
        ILoggerFactory? loggerFactory)
    {
        if (string.IsNullOrWhiteSpace(schemaUrl))
            return;

        try
        {
            var config = new SchemaRegistryConfig { Url = schemaUrl };
            using var client = new CachedSchemaRegistryClient(config);
            var subject = $"{topic}-value";
            var metadata = client.GetLatestSchemaAsync(subject).GetAwaiter().GetResult();
            var schemaString = metadata?.SchemaString;
            if (string.IsNullOrWhiteSpace(schemaString))
                return;

            if (Avro.Schema.Parse(schemaString) is not RecordSchema recordSchema)
                return;

            // Force value mapping to SR schema and use GenericRecord to tolerate CTAS evolution
            mapping.AvroValueSchema = schemaString;
            mapping.AvroValueRecordSchema = recordSchema;
            mapping.AvroValueType = typeof(GenericRecord);
            if (string.IsNullOrWhiteSpace(model.ValueSchemaFullName))
                model.ValueSchemaFullName = recordSchema.Fullname;

            try
            {
                var logger = loggerFactory?.CreateLogger(typeof(KsqlContextCacheExtensions));
                var fields = string.Join(",", recordSchema.Fields.Select(f => f.Name));
                logger?.LogDebug("Aligned VALUE schema from SR: subject={Subject} fields=[{Fields}]", subject, fields);
            }
            catch { }
        }
        catch (SchemaRegistryException)
        {
            // Subject may not exist yet; ignore
        }
        catch (Exception ex)
        {
            loggerFactory?.CreateLogger(typeof(KsqlContextCacheExtensions))?.LogDebug(ex, "Failed to align value mapping for {Topic}", topic);
        }
    }

    // Parsing moved to TimeframeUtils

    private static object? CreateTimeWindowedSerde(Type timeWindowedType, object innerSerde, long windowSizeMs)
    {
        foreach (var ctor in timeWindowedType.GetConstructors())
        {
            var parameters = ctor.GetParameters();
            if (parameters.Length >= 2 &&
                parameters[0].ParameterType.IsInstanceOfType(innerSerde) &&
                parameters[1].ParameterType == typeof(long))
            {
                var args = new object?[parameters.Length];
                args[0] = innerSerde;
                args[1] = windowSizeMs;
                for (int i = 2; i < parameters.Length; i++)
                {
                    args[i] = parameters[i].HasDefaultValue
                        ? parameters[i].DefaultValue
                        : (parameters[i].ParameterType.IsValueType
                            ? Activator.CreateInstance(parameters[i].ParameterType)
                            : null);
                }
                return ctor.Invoke(args);
            }
        }

        foreach (var ctor in timeWindowedType.GetConstructors())
        {
            var parameters = ctor.GetParameters();
            if (parameters.Length == 1 &&
                parameters[0].ParameterType.IsInstanceOfType(innerSerde))
            {
                return ctor.Invoke(new object?[] { innerSerde });
            }
        }

        return null;
    }

    private static object? GetProperty(object target, Type targetType, string propertyName)
    {
        var property = targetType.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        return property?.GetValue(target);
    }

    private static void SetProperty(object target, Type targetType, string propertyName, object? value, bool optional = false)
    {
        var property = targetType.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (property == null)
        {
            if (!optional)
                throw new InvalidOperationException($"Property '{propertyName}' not found on type '{targetType.FullName}'.");
            return;
        }

        if (value == null)
        {
            if (property.PropertyType.IsValueType && Nullable.GetUnderlyingType(property.PropertyType) == null)
            {
                if (!optional)
                    throw new InvalidOperationException($"Cannot assign null to non-nullable property '{propertyName}'.");
                return;
            }
        }
        else if (!property.PropertyType.IsInstanceOfType(value))
        {
            try
            {
                if (property.PropertyType.IsEnum && value is string s)
                {
                    value = Enum.Parse(property.PropertyType, s, ignoreCase: true);
                }
                else if (property.PropertyType.IsEnum && value.GetType().IsValueType)
                {
                    value = Enum.ToObject(property.PropertyType, value);
                }
                else
                {
                    value = Convert.ChangeType(value, Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
                }
            }
            catch
            {
                if (!optional)
                    throw;
                return;
            }
        }

        property.SetValue(target, value);
    }
}

