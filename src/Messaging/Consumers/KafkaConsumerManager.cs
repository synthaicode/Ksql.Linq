using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Ksql.Linq.Configuration;
using Ksql.Linq.Configuration.Abstractions;
using Ksql.Linq.Configuration.Messaging;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Dlq;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Mapping;
using Ksql.Linq.Messaging.Producers;
using Ksql.Linq.Query.Metadata;
// Heartbeat-based leader election removed; this manager no longer depends on runtime heartbeat types
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Ksql.Linq.Messaging.Consumers;

internal class KafkaConsumerManager : IDisposable
{
    private readonly KsqlDslOptions _options;
    private readonly ILogger? _logger;
    private readonly Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient> _schemaRegistryClient;
    private readonly ConcurrentDictionary<Type, EntityModel> _entityModels;
    private readonly MappingRegistry _mappingRegistry;
    private readonly DlqOptions _dlq;
    private readonly IRateLimiter _limiter;
    private readonly IDlqProducer _dlqProducer;
    private readonly ICommitManager _commitManager;
    private bool _disposed;
    // Legacy leader election fields removed

    public event Action<IReadOnlyList<TopicPartition>>? PartitionsAssigned;
    public event Action<IReadOnlyList<TopicPartitionOffset>>? PartitionsRevoked;

#pragma warning disable CS0067 // Event is never used
    public event Func<byte[]?, Exception, string, int, long, DateTime, Headers?, string, string, Task>? DeserializationError;
#pragma warning restore CS0067

    public KafkaConsumerManager(
        MappingRegistry mapping,
        IOptions<KsqlDslOptions> options,
        ConcurrentDictionary<Type, EntityModel> entityModels,
        IDlqProducer dlqProducer,
        ICommitManager commitManager,
        ILoggerFactory? loggerFactory = null,
        IRateLimiter? limiter = null)
    {
        _mappingRegistry = mapping;
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        DefaultValueBinder.ApplyDefaults(_options);
        _entityModels = entityModels ?? new();
        _dlqProducer = dlqProducer;
        _commitManager = commitManager;
        _logger = loggerFactory?.CreateLogger<KafkaConsumerManager>();
        _schemaRegistryClient = new Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>(CreateSchemaRegistryClient);
        _dlq = _options.DlqOptions;
        _limiter = limiter ?? new SimpleRateLimiter(_dlq.MaxPerSecond);
    }



    public async IAsyncEnumerable<(TPOCO, Dictionary<string, string>, MessageMeta)> ConsumeAsync<TPOCO>(
        bool fromBeginning = false,
        bool autoCommit = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where TPOCO : class
    {
        var model = GetEntityModel<TPOCO>();
        var topic = model.GetTopicName();
        var mapping = _mappingRegistry.GetMapping(typeof(TPOCO));
        var configuredFromBeginning = ResolveBridgeFromBeginning(model);
        var effectiveFromBeginning = fromBeginning || configuredFromBeginning;
        var config = BuildConsumerConfig(topic, null, model.GroupId, autoCommit);
        if (effectiveFromBeginning && config.AutoOffsetReset != AutoOffsetReset.Earliest)
        {
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
        }
        _logger?.LogInformation("ConsumeAsync subscribe topic={Topic} entity={Entity} autoCommit={AutoCommit} autoOffsetReset={OffsetReset} fromBeginning={FromBeginning}", topic, typeof(TPOCO).Name, autoCommit, config.AutoOffsetReset, effectiveFromBeginning);

        var isWindowed = TryResolveWindowSizeMs(model, out var windowSizeMs);
        var keyType = mapping.AvroKeyType ?? typeof(Confluent.Kafka.Ignore);
        if (isWindowed)
        {
            keyType = typeof(GenericRecord);
        }

        var method = typeof(KafkaConsumerManager)
            .GetMethod(nameof(ConsumeInternal), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(keyType, mapping.AvroValueType!, typeof(TPOCO));

        var windowSizeArg = isWindowed ? (long?)windowSizeMs : null;
        var enumerable = (IAsyncEnumerable<(TPOCO, Dictionary<string, string>, MessageMeta)>)method
            .Invoke(this, new object?[] { topic, config, mapping, effectiveFromBeginning, model.Partitions, windowSizeArg, cancellationToken })!;

        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            yield return item;
    }

    // Leader election via dedicated heartbeat topic removed.

    private async IAsyncEnumerable<(TPOCO, Dictionary<string, string>, MessageMeta)> ConsumeInternal<TKey, TValue, TPOCO>(
        string topicName,
        ConsumerConfig config,
        KeyValueTypeMapping mapping,
        bool fromBeginning,
        int partitions,
        long? windowSizeMs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TKey : class where TValue : class where TPOCO : class
    {
        using var consumer = CreateConsumer<TKey, TValue>(topicName, config, mapping, windowSizeMs);
        if (fromBeginning)
        {
            _logger?.LogInformation("ConsumeInternal assigning beginning offsets topic={Topic} partitions={Partitions}", topicName, partitions);
            var tps = new List<TopicPartitionOffset>(partitions);
            for (var i = 0; i < partitions; i++)
                tps.Add(new TopicPartitionOffset(topicName, new Partition(i), new Offset(0)));
            consumer.Assign(tps);
            consumer.Commit(tps);
        }
        consumer.Subscribe(topicName);
        _logger?.LogInformation("ConsumeInternal subscribed topic={Topic} group={Group} autoCommit={AutoCommit} autoOffsetReset={OffsetReset}", topicName, config.GroupId, config.EnableAutoCommit, config.AutoOffsetReset);
        if (config.EnableAutoCommit != true)
            _commitManager.Bind(typeof(TPOCO), topicName, consumer);

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<TKey, TValue>? result;
            try
            {
                result = consumer.Consume(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            if (result == null || result.IsPartitionEOF)
                continue;

            _logger?.LogInformation("ConsumeInternal message topic={Topic} partition={Partition} offset={Offset} timestamp={Timestamp}", result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Timestamp.UtcDateTime);

            TPOCO entity;
            Dictionary<string, string> headers;
            MessageMeta meta;
            try
            {
                entity = (TPOCO)mapping.CombineFromAvroKeyValue(result.Message.Key, result.Message.Value!, typeof(TPOCO));
                headers = new Dictionary<string, string>();
                if (result.Message.Headers != null)
                {
                    foreach (var h in result.Message.Headers)
                        headers[h.Key] = System.Text.Encoding.UTF8.GetString(h.GetValueBytes());
                }

                meta = new MessageMeta(
                    Topic: result.Topic,
                    Partition: result.Partition,
                    Offset: result.Offset,
                    TimestampUtc: result.Message.Timestamp.UtcDateTime,
                    SchemaIdKey: TryGetSchemaId(result.Message.Key as byte[]),
                    SchemaIdValue: TryGetSchemaId(result.Message.Value as byte[]),
                    KeyIsNull: result.Message.Key is null,
                    HeaderAllowList: ExtractAllowedHeaders(result.Message.Headers, _dlq.HeaderAllowList, _dlq.HeaderValueMaxLength)
                );
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex,
                    "Consume mapping failed. Topic={Topic}, Partition={Partition}, Offset={Offset}, Error={ErrorType}: {Message}",
                    result.Topic, result.Partition.Value, result.Offset.Value, ex.GetType().Name, ex.Message);
                await HandleMappingException(result, ex, _dlqProducer, consumer, _dlq, _limiter, cancellationToken).ConfigureAwait(false);
                continue;
            }

            yield return (entity, headers, meta);
            await Task.CompletedTask;
        }
    }

    internal static async Task HandleMappingException<TKey, TValue>(
        ConsumeResult<TKey, TValue> result,
        Exception ex,
        IDlqProducer dlqProducer,
        IConsumer<TKey, TValue> consumer,
        DlqOptions options,
        IRateLimiter limiter,
        CancellationToken cancellationToken)
        where TKey : class where TValue : class
    {
        if (options.EnableForDeserializationError && DlqGuard.ShouldSend(options, limiter, ex.GetType()))
        {
            var allowHeaders = ExtractAllowedHeaders(result.Message.Headers, options.HeaderAllowList, options.HeaderValueMaxLength);
            var env = DlqEnvelopeFactory.From(result, ex,
                options.ApplicationId, options.ConsumerGroup, options.Host, allowHeaders,
                options.ErrorMessageMaxLength, options.StackTraceMaxLength, options.NormalizeStackTraceWhitespace);
            await dlqProducer.ProduceAsync(env, cancellationToken).ConfigureAwait(false);
        }
        consumer.Commit(result);
    }

    private IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topicName, ConsumerConfig config, KeyValueTypeMapping mapping, long? windowSizeMs)
        where TKey : class where TValue : class
    {
        // When using Ignore as TKey (key-less topics), do not attach Avro deserializer for the key.
        if (typeof(TKey) == typeof(Confluent.Kafka.Ignore))
        {
            var builder = new ConsumerBuilder<Confluent.Kafka.Ignore, TValue>(config)
                .SetValueDeserializer(new AvroDeserializer<TValue>(_schemaRegistryClient.Value).AsSyncOverAsync());
            builder = builder
                .SetPartitionsAssignedHandler((_, parts) => HandlePartitionsAssigned(parts))
                .SetPartitionsRevokedHandler((_, parts) => HandlePartitionsRevoked(parts));
            return (IConsumer<TKey, TValue>)(object)builder.Build();
        }

        if (windowSizeMs.HasValue && typeof(TKey) == typeof(GenericRecord))
        {
            var keyDeserializer = new WindowedAvroKeyDeserializer(
                new AvroDeserializer<GenericRecord>(_schemaRegistryClient.Value).AsSyncOverAsync(),
                _logger,
                topicName);

            var builder = new ConsumerBuilder<GenericRecord, TValue>(config)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(new AvroDeserializer<TValue>(_schemaRegistryClient.Value).AsSyncOverAsync());
            builder = builder
                .SetPartitionsAssignedHandler((_, parts) => HandlePartitionsAssigned(parts))
                .SetPartitionsRevokedHandler((_, parts) => HandlePartitionsRevoked(parts));
            return (IConsumer<TKey, TValue>)(object)builder.Build();
        }

        var builderGeneric = new ConsumerBuilder<TKey, TValue>(config)
            .SetKeyDeserializer(new AvroDeserializer<TKey>(_schemaRegistryClient.Value).AsSyncOverAsync())
            .SetValueDeserializer(new AvroDeserializer<TValue>(_schemaRegistryClient.Value).AsSyncOverAsync())
            .SetPartitionsAssignedHandler((_, parts) => HandlePartitionsAssigned(parts))
            .SetPartitionsRevokedHandler((_, parts) => HandlePartitionsRevoked(parts));
        return builderGeneric.Build();
    }


    private static bool TryResolveWindowSizeMs(EntityModel model, out long windowSizeMs)
    {
        windowSizeMs = 0;
        var metadata = model.GetOrCreateMetadata();
        var role = PromoteRole(metadata, model);
        if (role == null)
            return false;

        if (role.Equals("Final1sStream", StringComparison.OrdinalIgnoreCase))
            return false;

        if (!role.StartsWith("Final", StringComparison.OrdinalIgnoreCase))
            return false;

        var timeframe = PromoteTimeframe(metadata, model);
        if (string.IsNullOrWhiteSpace(timeframe))
            return false;

        return Ksql.Linq.Query.Builders.Common.TimeframeUtils.TryToMilliseconds(timeframe, out windowSizeMs);
    }

    // Parsing moved to TimeframeUtils
    private void HandlePartitionsAssigned(IReadOnlyList<TopicPartition> parts)
    {
        PartitionsAssigned?.Invoke(parts);
    }

    private void HandlePartitionsRevoked(IReadOnlyList<TopicPartitionOffset> parts)
    {
        PartitionsRevoked?.Invoke(parts);
    }

    private ConfluentSchemaRegistry.ISchemaRegistryClient CreateSchemaRegistryClient()
    {
        var cfg = new ConfluentSchemaRegistry.SchemaRegistryConfig { Url = _options.SchemaRegistry.Url };
        _logger.LogClientConfig("schema-registry(consumer)", cfg);
        return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(cfg);
    }

    private EntityModel GetEntityModel<T>() where T : class
    {
        if (_entityModels.TryGetValue(typeof(T), out var model))
            return model;
        throw new InvalidOperationException($"Entity model not found for {typeof(T).Name}");
    }

    private ConsumerConfig BuildConsumerConfig(string topicName, KafkaSubscriptionOptions? subscriptionOptions, string? modelGroupId, bool autoCommit)
    {
        var hasConfig = _options.Topics.TryGetValue(topicName, out var cfg);
        TopicSection topicConfig = hasConfig && cfg is not null ? cfg : new TopicSection();
        DefaultValueBinder.ApplyDefaults(topicConfig);

        string? groupId = null;
        if (hasConfig && !string.IsNullOrWhiteSpace(topicConfig.Consumer!.GroupId))
            groupId = topicConfig.Consumer.GroupId;
        else if (!string.IsNullOrWhiteSpace(subscriptionOptions?.GroupId))
            groupId = subscriptionOptions.GroupId;
        else if (!string.IsNullOrWhiteSpace(modelGroupId))
            groupId = modelGroupId;

        DefaultValueBinder.ApplyDefaults(topicConfig.Consumer!);
        if (string.IsNullOrWhiteSpace(groupId))
            groupId = topicConfig.Consumer.GroupId;
        // Respect the caller's intent: prefer the explicit autoCommit flag from the API
        // Topic configuration may provide defaults, but should not override the method parameter.
        var enableAutoCommit = autoCommit;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _options.Common.BootstrapServers,
            ClientId = _options.Common.ClientId,
            GroupId = groupId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(topicConfig.Consumer.AutoOffsetReset),
            EnableAutoCommit = enableAutoCommit,
            AutoCommitIntervalMs = topicConfig.Consumer.AutoCommitIntervalMs,
            SessionTimeoutMs = topicConfig.Consumer.SessionTimeoutMs,
            HeartbeatIntervalMs = topicConfig.Consumer.HeartbeatIntervalMs,
            MaxPollIntervalMs = topicConfig.Consumer.MaxPollIntervalMs,
            FetchMinBytes = topicConfig.Consumer.FetchMinBytes,
            FetchMaxBytes = topicConfig.Consumer.FetchMaxBytes,
            IsolationLevel = Enum.Parse<IsolationLevel>(topicConfig.Consumer.IsolationLevel)
        };
        _logger?.LogInformation("ConsumerConfig topic={Topic} group={Group} autoCommit={AutoCommit} autoOffsetReset={Offset}", topicName, groupId, enableAutoCommit, topicConfig.Consumer.AutoOffsetReset);


        try { Console.WriteLine($"[ConsumerConfig] topic={topicName} group={groupId} enableAutoCommit={enableAutoCommit} autoOffsetReset={topicConfig.Consumer.AutoOffsetReset}"); } catch {}

        if (!hasConfig && subscriptionOptions != null)
        {
            if (subscriptionOptions.SessionTimeout.HasValue)
                consumerConfig.SessionTimeoutMs = (int)subscriptionOptions.SessionTimeout.Value.TotalMilliseconds;
            if (subscriptionOptions.HeartbeatInterval.HasValue)
                consumerConfig.HeartbeatIntervalMs = (int)subscriptionOptions.HeartbeatInterval.Value.TotalMilliseconds;
            if (subscriptionOptions.MaxPollInterval.HasValue)
                consumerConfig.MaxPollIntervalMs = (int)subscriptionOptions.MaxPollInterval.Value.TotalMilliseconds;
            if (subscriptionOptions.AutoOffsetReset.HasValue)
                consumerConfig.AutoOffsetReset = subscriptionOptions.AutoOffsetReset.Value;
        }

        if (_options.Common.SecurityProtocol != SecurityProtocol.Plaintext)
        {
            consumerConfig.SecurityProtocol = _options.Common.SecurityProtocol;
            if (_options.Common.SaslMechanism.HasValue)
            {
                consumerConfig.SaslMechanism = _options.Common.SaslMechanism.Value;
                consumerConfig.SaslUsername = _options.Common.SaslUsername;
                consumerConfig.SaslPassword = _options.Common.SaslPassword;
            }

            if (!string.IsNullOrEmpty(_options.Common.SslCaLocation))
            {
                consumerConfig.SslCaLocation = _options.Common.SslCaLocation;
                consumerConfig.SslCertificateLocation = _options.Common.SslCertificateLocation;
                consumerConfig.SslKeyLocation = _options.Common.SslKeyLocation;
                consumerConfig.SslKeyPassword = _options.Common.SslKeyPassword;
            }
        }

        foreach (var kvp in topicConfig.Consumer.AdditionalProperties)
            consumerConfig.Set(kvp.Key, kvp.Value);
        _logger.LogClientConfig($"consumer:{topicName}", consumerConfig, topicConfig.Consumer.AdditionalProperties);
        return consumerConfig;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Legacy leader election resources removed

        if (_schemaRegistryClient.IsValueCreated)
            _schemaRegistryClient.Value.Dispose();
    }

    private static int? TryGetSchemaId(byte[]? payload)
    {
        if (payload is { Length: >= 5 } && payload[0] == 0)
            return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(1, 4));
        return null;
    }

    private static bool ResolveBridgeFromBeginning(EntityModel model)
    {
        var metadata = model.GetOrCreateMetadata();
        if (metadata.Extras != null && metadata.Extras.TryGetValue("bridge_from_beginning", out var value))
        {
            if (TryCoerceBool(value) is { } parsed)
                return parsed;
        }

        return false;
    }

    private static string? PromoteRole(QueryMetadata metadata, EntityModel model)
    {
        _ = model;
        return string.IsNullOrWhiteSpace(metadata.Role) ? null : metadata.Role;
    }

    private static string? PromoteTimeframe(QueryMetadata metadata, EntityModel model)
    {
        _ = model;
        return string.IsNullOrWhiteSpace(metadata.TimeframeRaw) ? null : metadata.TimeframeRaw;
    }

    private static bool? TryCoerceBool(object? value)
        => value switch
        {
            bool b => b,
            string s when bool.TryParse(s, out var parsed) => parsed,
            _ => (bool?)null
        };

    private static System.Collections.Generic.IReadOnlyDictionary<string, string> ExtractAllowedHeaders(
        Headers? headers, System.Collections.Generic.IEnumerable<string> allowList, int maxLen = 1024)
    {
        var dict = new System.Collections.Generic.Dictionary<string, string>(System.StringComparer.OrdinalIgnoreCase);
        if (headers is null) return dict;

        var set = allowList is System.Collections.Generic.HashSet<string> hs ? hs :
                  new System.Collections.Generic.HashSet<string>(allowList ?? System.Array.Empty<string>(), System.StringComparer.OrdinalIgnoreCase);

        foreach (var h in headers)
        {
            if (!set.Contains(h.Key)) continue;
            var bytes = h.GetValueBytes() ?? System.Array.Empty<byte>();
            string val;
            try
            {
                val = System.Text.Encoding.UTF8.GetString(bytes);
                if (!System.Text.Encoding.UTF8.GetBytes(val).AsSpan().SequenceEqual(bytes))
                    val = "base64:" + System.Convert.ToBase64String(bytes);
            }
            catch
            {
                val = "base64:" + System.Convert.ToBase64String(bytes);
            }
            if (val.Length > maxLen) val = val[..maxLen];
            dict[h.Key] = val;
        }
        return dict;
    }
}






