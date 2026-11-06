using Avro.Generic;
using Confluent.Kafka;
using Ksql.Linq.Events;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Buffers.Binary;
using System.Threading;

namespace Ksql.Linq.SerDes;

internal enum WindowedKeyFormat
{
    Topic,
    Store
}

internal sealed class FixedTimeWindowedAvroSerDes : AbstractSerDes<Windowed<GenericRecord>>
{
    private const int StoreSuffixBytes = 12; // window start (8) + sequence (4)
    private const int TopicSuffixBytes = 8;

    private readonly ISerDes<GenericRecord> _inner;
    private readonly long _windowSizeMs;
    private readonly WindowedKeyFormat _format;
    private static readonly bool _diagEnabled =
        string.Equals(Environment.GetEnvironmentVariable("PHYS_SERDES_DIAG"), "1", StringComparison.Ordinal);
    private static int _diagCount = 0;
    private const int _diagMax = 50;

    public FixedTimeWindowedAvroSerDes()
        : this(new SchemaAvroSerDes<GenericRecord>(), -1)
    {
    }

    public FixedTimeWindowedAvroSerDes(ISerDes<GenericRecord> inner, long windowSizeMs)
        : this(inner, windowSizeMs, WindowedKeyFormat.Topic)
    {
    }

    public FixedTimeWindowedAvroSerDes(ISerDes<GenericRecord> inner, long windowSizeMs, WindowedKeyFormat format)
    {
        if (format != WindowedKeyFormat.Topic && format != WindowedKeyFormat.Store)
            throw new ArgumentOutOfRangeException(nameof(format));
        _format = format;
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

        byte[] key;
        long windowStart;
        var phase = _format == WindowedKeyFormat.Topic ? "topic" : "store";
        var sliced = _format == WindowedKeyFormat.Topic
            ? TrySliceTopicFormat(dd, out key, out windowStart)
            : TrySliceStoreFormat(dd, out key, out windowStart);
        if (sliced)
        {
            try
            {
                var record = DeserializeRecord(key, context);
                var windowEnd = _windowSizeMs > 0 ? windowStart + _windowSizeMs : windowStart;
                var result = new Windowed<GenericRecord>(record, new TimeWindow(windowStart, windowEnd));
                TryPublishDiag(phase, key.Length, dd.Length, windowStart, windowEnd, context.Topic);
                return result;
            }
            catch (Exception ex)
            {
                TryPublishDiag(phase + "_fail", key.Length, dd.Length, 0, 0, context.Topic, error: ex);
            }
        }

        TryPublishDiag("fail", 0, dd.Length, 0, 0, context.Topic, note: "invalid_" + phase + "_format");
        throw new Avro.AvroException($"Unable to deserialize {_format} windowed Avro key (length={dd.Length}).");
    }

    public override byte[] Serialize(Windowed<GenericRecord> data, SerializationContext context)
    {
        if (data == null)
            return null!;

        var keyBytes = _inner.Serialize(data.Key, context) ?? Array.Empty<byte>();
        // The default source-topic contract writes 8 suffix bytes. Store mode
        // explicitly retains the former 12-byte writer layout.
        var suffixBytes = _format == WindowedKeyFormat.Topic ? TopicSuffixBytes : StoreSuffixBytes;
        var buffer = new byte[keyBytes.Length + suffixBytes];
        if (keyBytes.Length > 0)
            Array.Copy(keyBytes, buffer, keyBytes.Length);

        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(keyBytes.Length, 8), data.Window.StartMs);
        if (_format == WindowedKeyFormat.Store)
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
        windowStart = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(keyLength, 8));
        return true;
    }

    private static void TryPublishDiag(string phase, int keyBytesLen, int rawLen, long windowStart, long windowEnd, string? topic, Exception? error = null, string? note = null)
    {
        if (!_diagEnabled) return;
        var n = Interlocked.Increment(ref _diagCount);
        if (n > _diagMax) return;
        try
        {
            Ksql.Linq.Events.RuntimeEvents.TryPublishFireAndForget(new RuntimeEvent
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
