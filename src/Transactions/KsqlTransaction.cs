using Avro;
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Ksql.Linq.Transactions;

/// <summary>
/// Represents a Kafka transaction for atomic writes across multiple topics.
/// </summary>
public class KsqlTransaction : IKsqlTransaction
{
    private readonly IProducer<byte[], byte[]> _transactionalProducer;
    private readonly List<TopicPartitionOffset> _consumedOffsets = new();
    private readonly IConsumerGroupMetadata? _consumerGroupMetadata;
    private readonly MappingRegistry _mappingRegistry;
    private readonly ConfluentSchemaRegistry.ISchemaRegistryClient _schemaRegistryClient;
    private readonly ILogger? _logger;
    private readonly TransactionOptions _options;
    private bool _committed;
    private bool _aborted;
    private bool _disposed;

    public string TransactionalId => _options.TransactionalId;
    public bool IsCommitted => _committed;
    public bool IsAborted => _aborted;

    internal KsqlTransaction(
        KsqlDslOptions dslOptions,
        TransactionOptions options,
        MappingRegistry mappingRegistry,
        ConfluentSchemaRegistry.ISchemaRegistryClient schemaRegistryClient,
        IConsumerGroupMetadata? consumerGroupMetadata,
        ILogger? logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _mappingRegistry = mappingRegistry ?? throw new ArgumentNullException(nameof(mappingRegistry));
        _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
        _consumerGroupMetadata = consumerGroupMetadata;
        _logger = logger;

        if (string.IsNullOrWhiteSpace(options.TransactionalId))
            throw new ArgumentException("TransactionalId is required for transactions", nameof(options));

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = dslOptions.Common.BootstrapServers,
            ClientId = dslOptions.Common.ClientId,
            TransactionalId = options.TransactionalId,
            EnableIdempotence = options.EnableIdempotence,
            Acks = Acks.All,
            MaxInFlight = options.MaxInFlight,
            TransactionTimeoutMs = (int)options.TransactionTimeout.TotalMilliseconds
        };

        _transactionalProducer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();

        _logger?.LogDebug("Initializing transaction with ID: {TransactionalId}", options.TransactionalId);
        _transactionalProducer.InitTransactions(options.TransactionTimeout);
        _transactionalProducer.BeginTransaction();
        _logger?.LogDebug("Transaction begun: {TransactionalId}", options.TransactionalId);
    }

    public async Task ProduceAsync<T>(string topicName, T entity, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) where T : class
    {
        if (_committed || _aborted)
            throw new InvalidOperationException("Cannot produce to a completed transaction");

        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        var mapping = _mappingRegistry.GetMapping(typeof(T));
        var topic = topicName ?? typeof(T).GetKafkaTopicName();

        // Create key and value objects
        object? keyObj = mapping.AvroKeyType switch
        {
            null => null,
            Type t when t == typeof(GenericRecord) => new GenericRecord(mapping.AvroKeyRecordSchema ?? (RecordSchema)Schema.Parse(mapping.AvroKeySchema!)),
            Type t => Activator.CreateInstance(t)!
        };

        object valueObj = mapping.AvroValueType == typeof(GenericRecord)
            ? new GenericRecord(mapping.AvroValueRecordSchema ?? (RecordSchema)Schema.Parse(mapping.AvroValueSchema!))
            : Activator.CreateInstance(mapping.AvroValueType!)!;

        mapping.PopulateAvroKeyValue(entity, keyObj, valueObj);

        // Serialize key and value
        byte[]? keyBytes = null;
        byte[] valueBytes;

        if (keyObj != null)
        {
            keyBytes = await SerializeAsync(keyObj, topic, true);
        }
        valueBytes = await SerializeAsync(valueObj, topic, false);

        // Build message
        var message = new Message<byte[], byte[]>
        {
            Key = keyBytes!,
            Value = valueBytes
        };

        if (headers?.Count > 0)
        {
            message.Headers = new Headers();
            foreach (var kvp in headers)
            {
                if (kvp.Value != null)
                {
                    message.Headers.Add(kvp.Key, System.Text.Encoding.UTF8.GetBytes(kvp.Value));
                }
            }
        }

        _logger?.LogDebug("Producing to topic {Topic} within transaction {TransactionalId}", topic, _options.TransactionalId);
        await _transactionalProducer.ProduceAsync(topic, message, cancellationToken);
    }

    private async Task<byte[]> SerializeAsync(object obj, string topic, bool isKey)
    {
        if (obj is GenericRecord genericRecord)
        {
            var serializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient);
            var context = new SerializationContext(
                isKey ? MessageComponentType.Key : MessageComponentType.Value,
                topic);
            return await serializer.SerializeAsync(genericRecord, context);
        }
        else
        {
            // Use reflection to create the appropriate serializer for specific records
            var serializerType = typeof(AvroSerializer<>).MakeGenericType(obj.GetType());
            var serializer = Activator.CreateInstance(serializerType, _schemaRegistryClient);
            var context = new SerializationContext(
                isKey ? MessageComponentType.Key : MessageComponentType.Value,
                topic);

            var method = serializerType.GetMethod("SerializeAsync");
            var task = (Task<byte[]>)method!.Invoke(serializer, new object[] { obj, context })!;
            return await task;
        }
    }

    public void TrackConsumedOffset(string topic, int partition, long offset)
    {
        if (_committed || _aborted)
            throw new InvalidOperationException("Cannot track offsets on a completed transaction");

        _consumedOffsets.Add(new TopicPartitionOffset(topic, partition, offset + 1)); // +1 for next offset
        _logger?.LogDebug("Tracked offset: {Topic}[{Partition}]@{Offset}", topic, partition, offset);
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_committed)
        {
            _logger?.LogWarning("Transaction {TransactionalId} already committed", _options.TransactionalId);
            return Task.CompletedTask;
        }

        if (_aborted)
            throw new InvalidOperationException("Cannot commit an aborted transaction");

        try
        {
            // Send offsets to transaction if we have consumer metadata (for EOS)
            if (_consumedOffsets.Count > 0 && _consumerGroupMetadata != null)
            {
                _logger?.LogDebug("Sending {Count} offsets to transaction {TransactionalId}",
                    _consumedOffsets.Count, _options.TransactionalId);
                _transactionalProducer.SendOffsetsToTransaction(
                    _consumedOffsets,
                    _consumerGroupMetadata,
                    _options.TransactionTimeout);
            }

            _logger?.LogDebug("Committing transaction {TransactionalId}", _options.TransactionalId);
            _transactionalProducer.CommitTransaction(_options.TransactionTimeout);
            _committed = true;
            _logger?.LogInformation("Transaction {TransactionalId} committed successfully", _options.TransactionalId);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to commit transaction {TransactionalId}", _options.TransactionalId);
            throw;
        }

        return Task.CompletedTask;
    }

    public void Abort()
    {
        if (_aborted)
        {
            _logger?.LogWarning("Transaction {TransactionalId} already aborted", _options.TransactionalId);
            return;
        }

        if (_committed)
        {
            _logger?.LogWarning("Cannot abort a committed transaction {TransactionalId}", _options.TransactionalId);
            return;
        }

        try
        {
            _logger?.LogDebug("Aborting transaction {TransactionalId}", _options.TransactionalId);
            _transactionalProducer.AbortTransaction(_options.TransactionTimeout);
            _aborted = true;
            _logger?.LogInformation("Transaction {TransactionalId} aborted", _options.TransactionalId);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to abort transaction {TransactionalId}", _options.TransactionalId);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        if (!_committed && !_aborted)
        {
            try
            {
                Abort();
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Error aborting transaction during disposal");
            }
        }

        _transactionalProducer?.Dispose();
        _disposed = true;

        await Task.CompletedTask;
    }
}
