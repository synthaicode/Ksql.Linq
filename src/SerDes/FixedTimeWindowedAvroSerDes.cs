using System;
using System.Buffers.Binary;
using Avro.Generic;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System.Threading;
using Ksql.Linq.Events;

namespace Ksql.Linq.SerDes;

internal sealed class FixedTimeWindowedAvroSerDes : AbstractSerDes<Windowed<GenericRecord>>
{
    private const int StoreSuffixBytes = 12; // sequence (4) + window start (8)

    private readonly ISerDes<GenericRecord> _inner;
    private readonly long _windowSizeMs;
    private static readonly bool _diagEnabled =
        string.Equals(Environment.GetEnvironmentVariable("PHYS_SERDES_DIAG"), "1", StringComparison.Ordinal);
    private static int _diagCount = 0;
    private const int _diagMax = 50;

    public FixedTimeWindowedAvroSerDes()
        : this(new SchemaAvroSerDes<GenericRecord>(), -1)
    {
    }

    public FixedTimeWindowedAvroSerDes(ISerDes<GenericRecord> inner, long windowSizeMs)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _windowSizeMs = windowSizeMs;
    }

    public override void Initialize(SerDesContext context)
    {
        base.Initialize(context);
        _inner.Initialize(context);
        TryPublishDiag("init", 0, 0, _windowSizeMs, _windowSizeMs, topic: string.Empty, note: $"windowMs={_windowSizeMs}");
    }

    public override Windowed<GenericRecord> Deserialize(byte[] data, SerializationContext context)
    {
        if (data == null || data.Length == 0)
            return null!;
        var dd = data!;

        if (TrySliceTopicFormat(dd, out var topicKey, out var topicWindowStart))
        {
            try
            {
                var record = DeserializeRecord(topicKey, context);
                var windowStart = topicWindowStart;
                var windowEnd = _windowSizeMs > 0 ? windowStart + _windowSizeMs : windowStart;
                var w = new Windowed<GenericRecord>(record, new TimeWindow(windowStart, windowEnd));
                TryPublishDiag("topic", topicKey?.Length ?? 0, dd.Length, windowStart, windowEnd, context.Topic);
                return w;
            }
            catch (Exception ex)
            {
                TryPublishDiag("topic_fail", topicKey?.Length ?? 0, dd.Length, 0, 0, context.Topic, error: ex);
                // fall through to try store format
            }
        }

        if (TrySliceStoreFormat(dd, out var storeKey, out var storeWindowStart))
        {
            try
            {
                var record = DeserializeRecord(storeKey, context);
                var storeWindowEnd = _windowSizeMs > 0 ? storeWindowStart + _windowSizeMs : storeWindowStart;
                var w = new Windowed<GenericRecord>(record, new TimeWindow(storeWindowStart, storeWindowEnd));
                TryPublishDiag("store", storeKey?.Length ?? 0, dd.Length, storeWindowStart, storeWindowEnd, context.Topic);
                return w;
            }
            catch (Exception ex)
            {
                TryPublishDiag("store_fail", storeKey?.Length ?? 0, dd.Length, 0, 0, context.Topic, error: ex);
            }
        }

        TryPublishDiag("fail", 0, dd.Length, 0, 0, context.Topic, note: "no_format_match");
        throw new Avro.AvroException($"Unable to deserialize windowed Avro key (length={dd.Length}).");
    }

    public override byte[] Serialize(Windowed<GenericRecord> data, SerializationContext context)
    {
        if (data == null)
            return null!;

        var keyBytes = _inner.Serialize(data.Key, context) ?? Array.Empty<byte>();
        var buffer = new byte[keyBytes.Length + StoreSuffixBytes];
        if (keyBytes.Length > 0)
            Array.Copy(keyBytes, buffer, keyBytes.Length);

        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(keyBytes.Length, 8), data.Window.StartMs);
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(keyBytes.Length + 8, 4), 0);
        TryPublishDiag("serialize", keyBytes?.Length ?? 0, buffer.Length, data.Window.StartMs, data.Window.EndMs, context.Topic);
        return buffer;
    }

    private GenericRecord DeserializeRecord(byte[] payload, SerializationContext context)
    {
        return _inner.Deserialize(payload, context);
    }

    private static bool TrySliceTopicFormat(byte[] data, out byte[] keyBytes, out long windowStart)
    {
        keyBytes = Array.Empty<byte>();
        windowStart = 0;

        if (data.Length <= 8)
            return false;

        var keyLength = data.Length - 8;
        keyBytes = data.AsSpan(0, keyLength).ToArray();
        windowStart = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(keyLength, 8));
        return true;
    }

    private static bool TrySliceStoreFormat(byte[] data, out byte[] keyBytes, out long windowStart)
    {
        keyBytes = Array.Empty<byte>();
        windowStart = 0;

        if (data.Length <= StoreSuffixBytes)
            return false;

        var keyLength = data.Length - StoreSuffixBytes;
        keyBytes = data.AsSpan(0, keyLength).ToArray();
        windowStart = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(data.Length - 8, 8));
        return true;
    }

    private static void TryPublishDiag(string phase, int keyBytesLen, int rawLen, long windowStart, long windowEnd, string? topic, Exception? error = null, string? note = null)
    {
        if (!_diagEnabled) return;
        var n = Interlocked.Increment(ref _diagCount);
        if (n > _diagMax) return;
        try
        {
            _ = RuntimeEventBus.PublishAsync(new RuntimeEvent
            {
                Name = "serdes.windowed",
                Phase = phase,
                Topic = topic ?? string.Empty,
                Message = $"len={rawLen} keyLen={keyBytesLen} win=[{windowStart},{windowEnd}]" + (string.IsNullOrEmpty(note) ? string.Empty : $" {note}"),
                Success = error is null,
                Exception = error
            });
        }
        catch { }
    }
}