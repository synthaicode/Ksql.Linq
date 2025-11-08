using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Ksql.Linq.Configuration;
using Ksql.Linq.Configuration.Messaging;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Ksql.Linq.Messaging.Producers;

internal class KafkaProducerManager : IDisposable
{
    private readonly KsqlDslOptions _options;
    private readonly ILogger? _logger;
    private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
    private readonly bool _schemaRegistryDisabled;
    private readonly ConcurrentDictionary<Type, ProducerHolder> _producers = new();
    private readonly ConcurrentDictionary<(Type, string), ProducerHolder> _topicProducers = new();
    private bool _disposed;
    private readonly MappingRegistry _mappingRegistry;

    internal sealed class ProducerHolder : IDisposable
    {
        private readonly Func<object?, object?, KafkaMessageContext?, CancellationToken, Task> _sendAsync;
        private readonly Action<TimeSpan> _flush;
        private readonly Action _dispose;
        public string TopicName { get; }
        public bool IsValueOnly { get; }

        public ProducerHolder(string topicName,
            Func<object?, object?, KafkaMessageContext?, CancellationToken, Task> sendAsync,
            Action<TimeSpan> flush,
            Action dispose,
            bool isValueOnly)
        {
            TopicName = topicName;
            _sendAsync = sendAsync;
            _flush = flush;
            _dispose = dispose;
            IsValueOnly = isValueOnly;
        }

        public Task SendAsync(object? key, object? value, KafkaMessageContext? context, CancellationToken cancellationToken)
            => _sendAsync(key, value, context, cancellationToken);

        public Task FlushAsync(TimeSpan timeout)
        {
            _flush(timeout);
            return Task.CompletedTask;
        }

        public void Dispose() => _dispose();
    }
    public KafkaProducerManager(MappingRegistry mapping, IOptions<KsqlDslOptions> options, ILoggerFactory? loggerFactory = null)
    {
        _mappingRegistry = mapping;
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        DefaultValueBinder.ApplyDefaults(_options);
        _logger = loggerFactory?.CreateLogger<KafkaProducerManager>();
        _schemaRegistryDisabled = string.IsNullOrWhiteSpace(_options.SchemaRegistry.Url);
        _schemaRegistryClient = CreateSchemaRegistryClient();
    }

    private EntityModel GetEntityModel<T>() where T : class
    {
        var type = typeof(T);
        return new EntityModel
        {
            EntityType = type,
            TopicName = type.Name.ToLowerInvariant(),
            KeyProperties = Array.Empty<PropertyInfo>(),
            AllProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
        };
    }

    private ProducerConfig BuildProducerConfig(string topicName)
    {
        var section = _options.Topics.TryGetValue(topicName, out var cfg) ? cfg : new TopicSection();
        DefaultValueBinder.ApplyDefaults(section);
        DefaultValueBinder.ApplyDefaults(section.Producer);

        var pc = new ProducerConfig
        {
            BootstrapServers = _options.Common.BootstrapServers,
            ClientId = _options.Common.ClientId,
            Acks = Enum.Parse<Acks>(section.Producer.Acks),
            CompressionType = Enum.Parse<CompressionType>(section.Producer.CompressionType),
            EnableIdempotence = section.Producer.EnableIdempotence,
            MaxInFlight = section.Producer.MaxInFlightRequestsPerConnection,
            LingerMs = section.Producer.LingerMs,
            BatchSize = section.Producer.BatchSize,
            BatchNumMessages = section.Producer.BatchNumMessages,
            RetryBackoffMs = section.Producer.RetryBackoffMs,
            MessageTimeoutMs = section.Producer.DeliveryTimeoutMs
        };
        foreach (var kv in section.Producer.AdditionalProperties)
            pc.Set(kv.Key, kv.Value);
        _logger.LogClientConfig($"producer:{topicName}", pc, section.Producer.AdditionalProperties);
        return pc;
    }

    private ConfluentSchemaRegistry.ISchemaRegistryClient CreateSchemaRegistryClient()
    {
        var url = _options.SchemaRegistry.Url;
        if (_schemaRegistryDisabled)
        {
            // Use a benign placeholder; network calls are guarded elsewhere when Url is empty
            url = "http://localhost";
        }
        var cfg = new ConfluentSchemaRegistry.SchemaRegistryConfig { Url = url };
        _logger.LogClientConfig("schema-registry(producer)", cfg);
        return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(cfg);
    }


    private ProducerHolder CreateKeyedProducer<TKey, TValue>(string topicName) where TKey : class where TValue : class
    {
        var config = BuildProducerConfig(topicName);
        var prod = new ProducerBuilder<TKey, TValue>(config)
            .SetKeySerializer(new AvroSerializer<TKey>(_schemaRegistryClient).AsSyncOverAsync())
            .SetValueSerializer(new AvroSerializer<TValue>(_schemaRegistryClient).AsSyncOverAsync())
            .Build();
        return new ProducerHolder(
            topicName,
            (k, v, ctx, ct) =>
            {
                var msg = new Message<TKey, TValue> { Key = (TKey?)k!, Value = (TValue?)v! };
                if (ctx?.Headers?.Count > 0)
                    msg.Headers = BuildHeaders(ctx);
                return prod.ProduceAsync(topicName, msg, ct);
            },
            t => prod.Flush(t),
            () => { prod.Flush(System.TimeSpan.FromSeconds(5)); prod.Dispose(); },
            isValueOnly: false);
    }

    private ProducerHolder CreateValueOnlyProducer<TValue>(string topicName) where TValue : class
    {
        var config = BuildProducerConfig(topicName);
        var prod = new ProducerBuilder<Null, TValue>(config)
            .SetValueSerializer(new AvroSerializer<TValue>(_schemaRegistryClient).AsSyncOverAsync())
            .Build();
        return new ProducerHolder(
            topicName,
            (k, v, ctx, ct) =>
                {
                    var msg = new Message<Null, TValue> { Key = default!, Value = (TValue?)v! };
                    if (ctx?.Headers?.Count > 0)
                        msg.Headers = BuildHeaders(ctx);
                    return prod.ProduceAsync(topicName, msg, ct);
                },
            t => prod.Flush(t),
            () => { prod.Flush(System.TimeSpan.FromSeconds(5)); prod.Dispose(); },
            isValueOnly: true);
    }


    private ProducerHolder CreateProducer(Type? keyType, Type valueType, string topicName)
    {
        if (keyType == null || keyType == typeof(Confluent.Kafka.Null))
        {
            var m = typeof(KafkaProducerManager).GetMethod(nameof(CreateValueOnlyProducer), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(valueType);
            return (ProducerHolder)m.Invoke(this, new object[] { topicName })!;
        }
        else
        {
            var method = typeof(KafkaProducerManager).GetMethod(nameof(CreateKeyedProducer), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(keyType, valueType);
            return (ProducerHolder)method.Invoke(this, new object[] { topicName })!;
        }
    }

    private Task<ProducerHolder> GetProducerAsync<TPOCO>(string? topicName = null) where TPOCO : class
    {
        var model = GetEntityModel<TPOCO>();
        var name = (topicName ?? model.TopicName ?? typeof(TPOCO).Name).ToLowerInvariant();
        var mapping = _mappingRegistry.GetMapping(typeof(TPOCO));

        if (topicName == null)
        {
            if (_producers.TryGetValue(typeof(TPOCO), out var existing))
            {
                // Upgrade to keyed producer if mapping now has keys but cached holder is value-only
                if (existing.IsValueOnly && mapping.AvroKeyType != null && mapping.KeyProperties.Length > 0)
                {
                    try { existing.Dispose(); } catch { }
                    var keyTypeNew = mapping.AvroKeyType;
                    existing = CreateProducer(keyTypeNew, mapping.AvroValueType!, name);
                    _producers[typeof(TPOCO)] = existing;
                }
                return Task.FromResult(existing);
            }

            var keyType = mapping.AvroKeyType ?? typeof(Confluent.Kafka.Null);
            ProducerHolder producer = CreateProducer(keyType, mapping.AvroValueType!, name);

            _producers[typeof(TPOCO)] = producer;
            return Task.FromResult(producer);
        }
        else
        {
            var key = (typeof(TPOCO), name);
            if (_topicProducers.TryGetValue(key, out var existing))
            {
                if (existing.IsValueOnly && mapping.AvroKeyType != null && mapping.KeyProperties.Length > 0)
                {
                    try { existing.Dispose(); } catch { }
                    var keyTypeNew = mapping.AvroKeyType;
                    existing = CreateProducer(keyTypeNew, mapping.AvroValueType!, name);
                    _topicProducers[key] = existing;
                }
                return Task.FromResult(existing);
            }

            var keyType2 = mapping.AvroKeyType ?? typeof(Confluent.Kafka.Null);
            ProducerHolder producer = CreateProducer(keyType2, mapping.AvroValueType!, name);

            _topicProducers[key] = producer;
            return Task.FromResult(producer);
        }
    }


    public async Task SendAsync<TPOCO>(string topicName, TPOCO entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) where TPOCO : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        var producer = await GetProducerAsync<TPOCO>(topicName);
        var mapping = _mappingRegistry.GetMapping(typeof(TPOCO));

        // Debug: mapping configuration before alignment
        try
        {
            var keyPropsJoined = mapping.KeyProperties != null && mapping.KeyProperties.Length > 0
                ? string.Join(",", mapping.KeyProperties.Select(p => p.Name))
                : string.Empty;
            _logger?.LogDebug("[SendAsync] Mapping(before align) topic={Topic} KeyType={KeyType} KeyProperties={KeyProps} ValueType={ValueType}",
                producer.TopicName,
                mapping.AvroKeyType?.Name ?? "null",
                keyPropsJoined,
                mapping.AvroValueType?.Name ?? "null");
            if (mapping.KeyProperties == null || mapping.KeyProperties.Length == 0)
            {
                _logger?.LogDebug("[SendAsync] Mapping has no KeyProperties. This will produce null keys unless value-only is intended. topic={Topic}", producer.TopicName);
            }
            if (mapping.AvroKeyType == null)
            {
                _logger?.LogDebug("[SendAsync] AvroKeyType is null (value-only serializer). topic={Topic}", producer.TopicName);
            }
        }
        catch { }

        // すべてのProduceで、SR最新のスキーマに追随（CachedSchemaRegistryClientがプロセス内キャッシュ）
        // GenericRecord の場合のみ反映。SpecificRecord は型固定のため参照のみ。
        // SR URL 未設定時はネットワークアクセスを行わない
        var aligned = false;
        if (!_schemaRegistryDisabled)
        {
            aligned = TryAlignSchemasWithSchemaRegistry(producer.TopicName, mapping);
        }
        try
        {
            if (aligned)
            {
                var keyFields = mapping.AvroKeyRecordSchema?.Fields?.Select(f => f.Name) ?? Enumerable.Empty<string>();
                var valFields = mapping.AvroValueRecordSchema?.Fields?.Select(f => f.Name) ?? Enumerable.Empty<string>();
                _logger?.LogDebug("[SendAsync] SR aligned. keySchema={KeySchema} keyFields=[{KF}] valueSchema={ValSchema} valueFields=[{VF}]",
                    mapping.AvroKeyRecordSchema?.Fullname ?? "-",
                    string.Join(",", keyFields),
                    mapping.AvroValueRecordSchema?.Fullname ?? "-",
                    string.Join(",", valFields));
            }
        }
        catch { }

        // Safety: mappingにキーがあるのに値専用Producerなら、即時にキーありProducerへ切替
        if (producer.IsValueOnly && (mapping.KeyProperties?.Length ?? 0) > 0 && mapping.AvroKeyType != null)
        {
            try { producer.Dispose(); } catch { }
            var keyTypeNew = mapping.AvroKeyType;
            var valueTypeNew = mapping.AvroValueType ?? typeof(GenericRecord);
            var replacement = CreateProducer(keyTypeNew, valueTypeNew, producer.TopicName);
            if (_topicProducers.TryGetValue((typeof(TPOCO), producer.TopicName), out var current) && ReferenceEquals(current, producer))
                _topicProducers[(typeof(TPOCO), producer.TopicName)] = replacement;
            else
                _producers[typeof(TPOCO)] = replacement;
            producer = replacement;
            _logger?.LogDebug("Upgraded producer to keyed for topic={Topic} entity={Entity}", producer.TopicName, typeof(TPOCO).Name);
        }

        object? keyObj = mapping.AvroKeyType switch
        {
            null => null,
            Type t when t == typeof(GenericRecord) => new GenericRecord(mapping.AvroKeyRecordSchema ?? (RecordSchema)Schema.Parse(mapping.AvroKeySchema!)),
            Type t => Activator.CreateInstance(t)!
        };
        // Debug: log entity key-source values (mapping-driven, no fixed names)
        try
        {
            var eType = entity.GetType();
            var keyMeta = mapping.KeyProperties ?? Array.Empty<Ksql.Linq.Core.Models.PropertyMeta>();
            if (keyMeta.Length > 0)
            {
                var sb = new System.Text.StringBuilder();
                sb.Append("[SendAsync] Entity keys snapshot:");
                for (int i = 0; i < keyMeta.Length; i++)
                {
                    var pn = keyMeta[i].PropertyInfo?.Name ?? keyMeta[i].Name;
                    object? v = null;
                    try { v = eType.GetProperty(pn, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)?.GetValue(entity); } catch { }
                    sb.Append(' ').Append(pn).Append('=').Append(v);
                }
                _logger?.LogDebug(sb.ToString());
            }
        }
        catch { }

        object valueObj = mapping.AvroValueType == typeof(GenericRecord)
            ? new GenericRecord(mapping.AvroValueRecordSchema ?? (RecordSchema)Schema.Parse(mapping.AvroValueSchema!))
            : Activator.CreateInstance(mapping.AvroValueType!)!;
        mapping.PopulateAvroKeyValue(entity, keyObj, valueObj);

        // Align GenericRecord value with SR schema by explicitly assigning nulls
        // for any missing fields in the writer payload. This avoids Avro default
        // resolution paths (which may carry non-null defaults incompatible with
        // ksqlDB's nullable-union expectations) and prevents
        //   AvroException: Default null value is invalid, expected is json null.
        if (valueObj is GenericRecord grec && mapping.AvroValueRecordSchema is RecordSchema vSchema)
        {
            try { EnsureAllFieldsAssigned(grec, vSchema); } catch { }
        }

        // Debug: show key object contents that will be sent
        try
        {
            if (keyObj == null)
            {
                _logger?.LogDebug("[SendAsync] keyObj is NULL - producing null keys. topic={Topic}", producer.TopicName);
            }
            else if (keyObj is GenericRecord genKey)
            {
                var fields = genKey.Schema is RecordSchema rs ? rs.Fields : null;
                _logger?.LogDebug("[SendAsync] GenericRecord Key (topic={Topic}):", producer.TopicName);
                if (fields != null)
                {
                    foreach (var f in fields)
                    {
                        object? val = null;
                        try { val = genKey[f.Name]; } catch { }
                        _logger?.LogDebug("  {FieldName} = {Value}", f.Name, val);
                    }
                }
            }
            else
            {
                var props = keyObj.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
                _logger?.LogDebug("[SendAsync] Key object type: {Type}", keyObj.GetType().Name);
                foreach (var p in props)
                {
                    object? v = null; try { v = p.GetValue(keyObj); } catch { }
                    _logger?.LogDebug("  {PropName} = {Value}", p.Name, v);
                }
            }
        }
        catch { }

        var context = new KafkaMessageContext
        {
            MessageId = Guid.NewGuid().ToString(),
            Tags = new Dictionary<string, object>
            {
                ["entity_type"] = typeof(TPOCO).Name,
                ["method"] = "SendAsync"
            }
        };
        if (headers != null)
        {
            foreach (var kvp in headers)
                context.Headers[kvp.Key] = kvp.Value;
        }

        _logger?.LogInformation("kafka produce: topic={Topic}, entity={Entity}, method={Method}",
            producer.TopicName, typeof(TPOCO).Name, "SendAsync");

        var retried = false;
        while (true)
        {
            try
            {
                await producer.SendAsync(keyObj, valueObj, context, cancellationToken).ConfigureAwait(false);
                break;
            }
                        catch (Exception ex)
            {
                // One-shot: align mapping with SR and retry
                if (!retried && IsSchemaMismatch(ex))
                {
                    if (TryAlignSchemasWithSchemaRegistry(producer.TopicName, mapping))
                    {
                        _logger?.LogWarning("Schema mismatch detected; aligned mapping with SR latest and retrying once. topic={Topic}", producer.TopicName);
                        if (mapping.AvroKeyType == typeof(GenericRecord))
                            keyObj = new GenericRecord(mapping.AvroKeyRecordSchema!);
                        if (mapping.AvroValueType == typeof(GenericRecord))
                            valueObj = new GenericRecord(mapping.AvroValueRecordSchema!);

                        mapping.PopulateAvroKeyValue(entity, keyObj, valueObj!);
                        if (valueObj is GenericRecord grec2 && mapping.AvroValueRecordSchema is RecordSchema vrs2)
                        {
                            try { EnsureAllFieldsAssigned(grec2, vrs2); } catch { }
                        }
                        retried = true;
                        continue;
                    }
        }
                _logger?.LogError(ex,
                    "Produce failed. Topic={Topic}, Entity={Entity}, Method=SendAsync, Error={ErrorType}: {Message}",
                    producer.TopicName, typeof(TPOCO).Name, ex.GetType().Name, ex.Message);
                throw;
            }
        }
    }private static void EnsureAllFieldsAssigned(GenericRecord record, RecordSchema schema)
    {
        if (record == null || schema == null) return;
        for (int i = 0; i < schema.Fields.Count; i++)
        {
            var f = schema.Fields[i];
            object? current = null;
            var hasValue = true;
            try { current = record.GetValue(f.Pos); }
            catch { hasValue = false; }

            if (!hasValue)
            {
                record.Add(f.Name, null);
                continue;
            }

            // Optional: if union includes null and current is empty-string for string field,
            // prefer explicit null to avoid default confusion.
            try
            {
                if (current is string s && string.IsNullOrEmpty(s) && IsNullableUnion(f.Schema))
                {
                    record.Add(f.Name, null);
                }
            }
            catch { }
        }
    }

    private static bool IsNullableUnion(Schema schema)
    {
        if (schema == null) return false;
        if (schema is Avro.UnionSchema us)
        {
            foreach (var s in us.Schemas)
                if (s.Tag == Schema.Type.Null) return true;
        }
        return false;
    }

    

    private bool IsSchemaMismatch(Exception ex)
    {
        // 判定: SchemaRegistryException を内包し、名前不一致/非互換/未知スキーマを示唆するメッセージ
        var msg = ex.ToString();
        return msg.IndexOf("NAME_MISMATCH", StringComparison.OrdinalIgnoreCase) >= 0
            || msg.IndexOf("incompatible", StringComparison.OrdinalIgnoreCase) >= 0
            || msg.IndexOf("Schema being registered is incompatible", StringComparison.OrdinalIgnoreCase) >= 0
            || msg.IndexOf("unknown magic byte", StringComparison.OrdinalIgnoreCase) >= 0
            || msg.IndexOf("subject not found", StringComparison.OrdinalIgnoreCase) >= 0
            || msg.IndexOf("error code: 409", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private bool TryAlignSchemasWithSchemaRegistry(string topic, KeyValueTypeMapping mapping)
    {
        bool aligned = false;

        // Key subject: 常に SR を参照（GenericRecord なら mapping に反映、Specific はウォームアップのみ）
        try
        {
            var latestKey = _schemaRegistryClient.GetLatestSchemaAsync($"{topic}-key").GetAwaiter().GetResult();
            var keySchemaString = latestKey?.SchemaString;
            if (!string.IsNullOrWhiteSpace(keySchemaString))
            {
                if (mapping.AvroKeyType == typeof(Avro.Generic.GenericRecord))
                {
                    if (Avro.Schema.Parse(keySchemaString) is Avro.RecordSchema krs)
                    {
                        mapping.AvroKeyRecordSchema = krs;
                        mapping.AvroKeySchema = keySchemaString;
                        aligned = true;
                    }
                }
                else
                {
                    // SpecificRecord: 反映不要（型に内包）。SR キャッシュのみウォームアップ。
                }
            }
        }
        catch (ConfluentSchemaRegistry.SchemaRegistryException)
        {
            // key subject 不在でも処理継続（値側だけ存在する場合に備える）
        }

        // Value subject: 常に SR を参照（GenericRecord なら mapping に反映、Specific はウォームアップのみ）
        try
        {
            var latestVal = _schemaRegistryClient.GetLatestSchemaAsync($"{topic}-value").GetAwaiter().GetResult();
            var valSchemaString = latestVal?.SchemaString;
            if (!string.IsNullOrWhiteSpace(valSchemaString))
            {
                if (mapping.AvroValueType == typeof(Avro.Generic.GenericRecord))
                {
                    if (Avro.Schema.Parse(valSchemaString) is Avro.RecordSchema vrs)
                    {
                        mapping.AvroValueRecordSchema = vrs;
                        mapping.AvroValueSchema = valSchemaString;
                        aligned = true;
                    }
                }
                else
                {
                    // SpecificRecord: 反映不要。SR キャッシュのみウォームアップ。
                }
            }
        }
        catch (ConfluentSchemaRegistry.SchemaRegistryException)
        {
            // value subject 不在: そのまま返す
        }

        return aligned;
    }

    public async Task DeleteAsync<TPOCO>(TPOCO entity, CancellationToken cancellationToken = default) where TPOCO : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        var producer = await GetProducerAsync<TPOCO>();
        var mapping = _mappingRegistry.GetMapping(typeof(TPOCO));

        // 削除メッセージでもSR最新に追随（GenericRecordキー向け）。SR URL 未設定時はスキップ
        if (!_schemaRegistryDisabled)
        {
            TryAlignSchemasWithSchemaRegistry(producer.TopicName, mapping);
        }

        object? keyObj = mapping.AvroKeyType switch
        {
            null => null,
            Type t when t == typeof(GenericRecord) => new GenericRecord(mapping.AvroKeyRecordSchema ?? (RecordSchema)Schema.Parse(mapping.AvroKeySchema!)),
            Type t => Activator.CreateInstance(t)!
        };
        if (mapping.KeyProperties.Length > 0)
        {
            var tmp = mapping.AvroValueType == typeof(GenericRecord)
                ? new GenericRecord(mapping.AvroValueRecordSchema ?? (RecordSchema)Schema.Parse(mapping.AvroValueSchema!))
                : Activator.CreateInstance(mapping.AvroValueType!)!;
            mapping.PopulateAvroKeyValue(entity, keyObj, tmp);
        }

        var context = new KafkaMessageContext
        {
            MessageId = Guid.NewGuid().ToString(),
            Tags = new Dictionary<string, object>
            {
                ["entity_type"] = typeof(TPOCO).Name,
                ["method"] = "DeleteAsync"
            }
        };
        await producer.SendAsync(keyObj, null, context, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        foreach (var p in _producers.Values) p.Dispose();
        foreach (var p in _topicProducers.Values) p.Dispose();
        _schemaRegistryClient.Dispose();
        _producers.Clear();
        _topicProducers.Clear();
        _disposed = true;
    }

    private static Headers? BuildHeaders(KafkaMessageContext context)
    {
        if (context.Headers == null || context.Headers.Count == 0)
            return null;
        var headers = new Headers();
        foreach (var kvp in context.Headers)
        {
            if (kvp.Value != null)
            {
                var valueString = kvp.Value is bool b ? b.ToString().ToLowerInvariant() : kvp.Value.ToString() ?? string.Empty;
                headers.Add(kvp.Key, System.Text.Encoding.UTF8.GetBytes(valueString));
            }
        }
        return headers;
    }
}

