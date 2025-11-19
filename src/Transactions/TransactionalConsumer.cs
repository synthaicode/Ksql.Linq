using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Ksql.Linq.Transactions;

/// <summary>
/// Provides Exactly-Once Semantics (EOS) through consume-transform-produce pattern.
/// </summary>
public class TransactionalConsumer<TSource, TTarget> : IAsyncDisposable
    where TSource : class
    where TTarget : class
{
    private readonly KsqlDslOptions _dslOptions;
    private readonly TransactionOptions _options;
    private readonly MappingRegistry _mappingRegistry;
    private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
    private readonly ILogger? _logger;
    private readonly string _sourceTopicName;
    private readonly string _targetTopicName;
    private IConsumer<GenericRecord, GenericRecord>? _consumer;
    private IProducer<byte[], byte[]>? _producer;
    private bool _disposed;

    public TransactionalConsumer(
        KsqlDslOptions dslOptions,
        TransactionOptions options,
        MappingRegistry mappingRegistry,
        ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
        string sourceTopicName,
        string targetTopicName,
        ILogger? logger)
    {
        _dslOptions = dslOptions ?? throw new ArgumentNullException(nameof(dslOptions));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _mappingRegistry = mappingRegistry ?? throw new ArgumentNullException(nameof(mappingRegistry));
        _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
        _sourceTopicName = sourceTopicName ?? throw new ArgumentNullException(nameof(sourceTopicName));
        _targetTopicName = targetTopicName ?? throw new ArgumentNullException(nameof(targetTopicName));
        _logger = logger;

        InitializeConsumerAndProducer();
    }

    private void InitializeConsumerAndProducer()
    {
        // Create consumer with read_committed isolation
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _dslOptions.Common.BootstrapServers,
            GroupId = $"{_options.TransactionalId}-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            IsolationLevel = _options.IsolationLevel == KafkaIsolationLevel.ReadCommitted
                ? Confluent.Kafka.IsolationLevel.ReadCommitted
                : Confluent.Kafka.IsolationLevel.ReadUncommitted
        };

        _consumer = new ConsumerBuilder<GenericRecord, GenericRecord>(consumerConfig)
            .SetKeyDeserializer(new AvroDeserializer<GenericRecord>(_schemaRegistryClient).AsSyncOverAsync())
            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(_schemaRegistryClient).AsSyncOverAsync())
            .Build();

        _consumer.Subscribe(_sourceTopicName);

        // Create transactional producer
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _dslOptions.Common.BootstrapServers,
            ClientId = _dslOptions.Common.ClientId,
            TransactionalId = _options.TransactionalId,
            EnableIdempotence = _options.EnableIdempotence,
            Acks = Acks.All,
            MaxInFlight = _options.MaxInFlight,
            TransactionTimeoutMs = (int)_options.TransactionTimeout.TotalMilliseconds
        };

        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
        _producer.InitTransactions(_options.TransactionTimeout);

        _logger?.LogDebug("Initialized transactional consumer for {Source} -> {Target}",
            _sourceTopicName, _targetTopicName);
    }

    /// <summary>
    /// Processes messages with exactly-once semantics.
    /// </summary>
    /// <param name="transform">Transform function from source to target</param>
    /// <param name="batchSize">Number of messages to process per transaction</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ProcessAsync(
        Func<TSource, TTarget> transform,
        int batchSize = 100,
        CancellationToken cancellationToken = default)
    {
        if (transform == null)
            throw new ArgumentNullException(nameof(transform));

        var sourceMapping = _mappingRegistry.GetMapping(typeof(TSource));
        var targetMapping = _mappingRegistry.GetMapping(typeof(TTarget));

        while (!cancellationToken.IsCancellationRequested)
        {
            var messages = new List<(ConsumeResult<GenericRecord, GenericRecord> result, TSource entity)>();

            // Consume batch
            for (int i = 0; i < batchSize; i++)
            {
                try
                {
                    var result = _consumer!.Consume(TimeSpan.FromMilliseconds(100));
                    if (result == null) break;

                    var entity = (TSource)sourceMapping.ConvertFromAvro(result.Message.Key, result.Message.Value);
                    messages.Add((result, entity));
                }
                catch (ConsumeException)
                {
                    break;
                }
            }

            if (messages.Count == 0)
            {
                await Task.Delay(100, cancellationToken);
                continue;
            }

            // Process batch in transaction
            _producer!.BeginTransaction();

            try
            {
                var offsets = new List<TopicPartitionOffset>();

                foreach (var (result, source) in messages)
                {
                    var target = transform(source);

                    // Serialize and produce
                    var valueBytes = await SerializeValueAsync(target, targetMapping);
                    byte[]? keyBytes = null;

                    if (targetMapping.AvroKeyType != null)
                    {
                        keyBytes = await SerializeKeyAsync(target, targetMapping);
                    }

                    var message = new Message<byte[], byte[]>
                    {
                        Key = keyBytes!,
                        Value = valueBytes
                    };

                    await _producer.ProduceAsync(_targetTopicName, message, cancellationToken);

                    offsets.Add(new TopicPartitionOffset(
                        result.TopicPartition,
                        result.Offset + 1));
                }

                // Send offsets and commit
                _producer.SendOffsetsToTransaction(
                    offsets,
                    _consumer!.ConsumerGroupMetadata,
                    _options.TransactionTimeout);

                _producer.CommitTransaction(_options.TransactionTimeout);

                _logger?.LogDebug("Committed transaction with {Count} messages", messages.Count);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Transaction failed, aborting");
                _producer.AbortTransaction(_options.TransactionTimeout);
                throw;
            }
        }
    }

    /// <summary>
    /// Processes messages with exactly-once semantics using async transform.
    /// </summary>
    public async Task ProcessAsync(
        Func<TSource, Task<TTarget>> transformAsync,
        int batchSize = 100,
        CancellationToken cancellationToken = default)
    {
        await ProcessAsync(
            source => transformAsync(source).GetAwaiter().GetResult(),
            batchSize,
            cancellationToken);
    }

    private async Task<byte[]> SerializeValueAsync(TTarget entity, KeyValueTypeMapping mapping)
    {
        object valueObj = mapping.AvroValueType == typeof(GenericRecord)
            ? new GenericRecord(mapping.AvroValueRecordSchema!)
            : Activator.CreateInstance(mapping.AvroValueType!)!;

        mapping.PopulateAvroKeyValue(entity, null, valueObj);

        if (valueObj is GenericRecord genericRecord)
        {
            var serializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient);
            var context = new SerializationContext(MessageComponentType.Value, _targetTopicName);
            return await serializer.SerializeAsync(genericRecord, context);
        }
        else
        {
            var serializerType = typeof(AvroSerializer<>).MakeGenericType(valueObj.GetType());
            var serializer = Activator.CreateInstance(serializerType, _schemaRegistryClient);
            var context = new SerializationContext(MessageComponentType.Value, _targetTopicName);
            var method = serializerType.GetMethod("SerializeAsync");
            return await (Task<byte[]>)method!.Invoke(serializer, new object[] { valueObj, context })!;
        }
    }

    private async Task<byte[]> SerializeKeyAsync(TTarget entity, KeyValueTypeMapping mapping)
    {
        object? keyObj = mapping.AvroKeyType switch
        {
            null => null,
            Type t when t == typeof(GenericRecord) => new GenericRecord(mapping.AvroKeyRecordSchema!),
            Type t => Activator.CreateInstance(t)!
        };

        if (keyObj == null)
            return Array.Empty<byte>();

        // Create temporary value to populate key
        object valueObj = mapping.AvroValueType == typeof(GenericRecord)
            ? new GenericRecord(mapping.AvroValueRecordSchema!)
            : Activator.CreateInstance(mapping.AvroValueType!)!;

        mapping.PopulateAvroKeyValue(entity, keyObj, valueObj);

        if (keyObj is GenericRecord genericRecord)
        {
            var serializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient);
            var context = new SerializationContext(MessageComponentType.Key, _targetTopicName);
            return await serializer.SerializeAsync(genericRecord, context);
        }
        else
        {
            var serializerType = typeof(AvroSerializer<>).MakeGenericType(keyObj.GetType());
            var serializer = Activator.CreateInstance(serializerType, _schemaRegistryClient);
            var context = new SerializationContext(MessageComponentType.Key, _targetTopicName);
            var method = serializerType.GetMethod("SerializeAsync");
            return await (Task<byte[]>)method!.Invoke(serializer, new object[] { keyObj, context })!;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _consumer?.Close();
        _consumer?.Dispose();
        _producer?.Dispose();

        _disposed = true;
        await Task.CompletedTask;
    }
}
