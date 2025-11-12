using System;
using System.Buffers.Binary;
using System.Text;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Messaging.Consumers;

/// <summary>
/// Deserializes ksqlDB windowed table keys by stripping trailing window bytes before
/// delegating to the standard Avro deserializer. When環境変数 KSQL_DEBUG_WINDOW_KEYS=1、
/// raw bytes/境界をログ出力する。
/// </summary>
internal sealed class WindowedAvroKeyDeserializer : IDeserializer<GenericRecord>
{
    private const int StoreSuffixBytes = 12; // sequence (4) + window start (8)

    private static readonly bool DebugEnabled = string.Equals(
        Environment.GetEnvironmentVariable("KSQL_DEBUG_WINDOW_KEYS"),
        "1",
        StringComparison.OrdinalIgnoreCase);

    private readonly IDeserializer<GenericRecord> _inner;
    private readonly ILogger? _logger;
    private readonly string? _topic;

    public WindowedAvroKeyDeserializer(IDeserializer<GenericRecord> inner, ILogger? logger, string? topic)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _logger = logger;
        _topic = topic;
    }

    public GenericRecord Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return null!;

        if (DebugEnabled)
            LogBytes("raw", data);

        if (TrySliceTopicFormat(data, out var topicPayload, out var windowEnd) &&
            TryDeserialize(topicPayload, isNull, context, out var topicRecord))
        {
            if (DebugEnabled)
                LogWindow("topic", windowEnd, topicPayload.Length);
            return topicRecord;
        }

        if (TrySliceStoreFormat(data, out var storePayload, out var windowStart) &&
            TryDeserialize(storePayload, isNull, context, out var storeRecord))
        {
            if (DebugEnabled)
                LogWindow("store", windowStart, storePayload.Length);
            return storeRecord;
        }

        return _inner.Deserialize(data, isNull, context);
    }

    private bool TryDeserialize(ReadOnlySpan<byte> payload, bool isNull, SerializationContext context, out GenericRecord record)
    {
        try
        {
            record = _inner.Deserialize(payload, isNull, context);
            return true;
        }
        catch (Exception ex)
        {
            record = null!;
            if (DebugEnabled)
                LogMessage($"Avro inner deserialize failed: {ex.Message}");
            return false;
        }
    }

    private static bool TrySliceTopicFormat(ReadOnlySpan<byte> data, out ReadOnlySpan<byte> payload, out long windowEnd)
    {
        payload = default;
        windowEnd = 0;
        if (data.Length <= 8)
            return false;

        var keyLength = data.Length - 8;
        payload = data[..keyLength];
        windowEnd = BinaryPrimitives.ReadInt64BigEndian(data.Slice(keyLength, 8));
        return true;
    }

    private static bool TrySliceStoreFormat(ReadOnlySpan<byte> data, out ReadOnlySpan<byte> payload, out long windowStart)
    {
        payload = default;
        windowStart = 0;
        if (data.Length <= StoreSuffixBytes)
            return false;

        var keyLength = data.Length - StoreSuffixBytes;
        payload = data[..keyLength];
        windowStart = BinaryPrimitives.ReadInt64BigEndian(data.Slice(data.Length - 8, 8));
        return true;
    }

    private void LogBytes(string stage, ReadOnlySpan<byte> bytes)
    {
        var hex = BitConverter.ToString(bytes.ToArray());
        if (hex.Length > 240)
            hex = hex.Substring(0, 240) + "...";
        LogMessage($"[{stage}] len={bytes.Length} hex={hex}");
    }

    private void LogWindow(string stage, long boundary, int payloadLength)
    {
        LogMessage($"[{stage}] boundary={boundary} payloadLen={payloadLength}");
    }

    private void LogMessage(string message)
    {
        if (_logger != null)
            _logger.LogInformation("[WindowKey][{Topic}] {Message}", _topic ?? "?", message);
        else
        {
            try { Console.WriteLine($"[WindowKey][{_topic ?? "?"}] {message}"); } catch { }
        }
    }
}
