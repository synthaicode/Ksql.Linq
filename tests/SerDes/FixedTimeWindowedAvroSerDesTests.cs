using System;
using System.Buffers.Binary;
using System.Text;
using System.Reflection;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Ksql.Linq.SerDes;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.State;
using Xunit;

namespace Ksql.Linq.Tests.SerDes;

[Trait("Level", "L3")]
public class FixedTimeWindowedAvroSerDesTests
{
    private static readonly RecordSchema TestSchema = (RecordSchema)Schema.Parse(@"{""type"":""record"",""name"":""TestRecord"",""fields"": [{""name"":""value"",""type"":""string""}]}");

    [Fact]
    public void DeserializeTopicKeyUsesWindowStart()
    {
        var inner = new StubGenericRecordSerDes(TestSchema);
        var serdes = new FixedTimeWindowedAvroSerDes(inner, windowSizeMs: 60_000);
        serdes.Initialize(null!);

        var record = CreateRecord("topic");
        var keyBytes = inner.Serialize(record, new SerializationContext(MessageComponentType.Key, "topic"));
        var windowStart = 1_000_000L;

        var payload = new byte[keyBytes.Length + 8];
        Array.Copy(keyBytes, payload, keyBytes.Length);
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(keyBytes.Length, 8), windowStart);

        var result = serdes.Deserialize(payload, new SerializationContext(MessageComponentType.Key, "topic"));

        Assert.NotNull(result);
        Assert.Equal(windowStart, result.Window.StartMs);
        Assert.Equal(windowStart + 60_000, result.Window.EndMs);
        Assert.Equal("topic", result.Key["value"].ToString());
    }

    private static GenericRecord CreateRecord(string value)
    {
        var record = new GenericRecord(TestSchema);
        record.Add("value", value);
        return record;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DefaultSerializerRoundTripsExactKeyAndTimestamp(bool strict)
    {
        var inner = new FramedSerDes(strict);
        var serde = new FixedTimeWindowedAvroSerDes(inner, 60_000);
        var context = new SerializationContext(MessageComponentType.Key, "roundtrip");
        const long start = 0x010203040506L;
        var original = new Windowed<GenericRecord>(CreateRecord("key"), new TimeWindow(start, start + 60_000));

        var result = serde.Deserialize(serde.Serialize(original, context), context);

        Assert.Equal("key", result.Key["value"]);
        Assert.Equal(start, result.Window.StartMs);
        Assert.Equal(start + 60_000, result.Window.EndMs);
        Assert.Equal(inner.Serialize(original.Key, context), inner.LastRead);
    }

    [Fact]
    public void StoreSlicerReadsTimestampBeforeSequenceAndPreservesKey()
    {
        byte[] key = { 0, 0, 0, 0, 17, 3, 107, 101, 121 };
        const long start = 0x010203040506L;
        var bytes = new byte[key.Length + 12];
        key.CopyTo(bytes, 0);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(key.Length, 8), start);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(key.Length + 8, 4), 0x11223344);
        var method = typeof(FixedTimeWindowedAvroSerDes).GetMethod("TrySliceStoreFormat", BindingFlags.NonPublic | BindingFlags.Static)!;
        object[] args = { bytes, null!, 0L };

        Assert.True((bool)method.Invoke(null, args)!);
        Assert.Equal(key, (byte[])args[1]);
        Assert.Equal(start, (long)args[2]);
    }

    // Supplemental tests added after the original regression tests demonstrated red.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ExplicitStoreMatchesInstalledStreamizInBothDirections(bool strict)
    {
        var inner = new FramedSerDes(strict);
        var fixedSerde = new FixedTimeWindowedAvroSerDes(inner, 60_000, WindowedKeyFormat.Store);
        var oracle = new TimeWindowedSerDes<GenericRecord>(inner, 60_000);
        var context = new SerializationContext(MessageComponentType.Key, "store");
        const long start = 0x010203040506L;
        var original = new Windowed<GenericRecord>(CreateRecord("key"), new TimeWindow(start, start + 60_000));
        var expected = new byte[] { 0, 0, 0, 0, 17, 3, 107, 101, 121, 0, 0, 1, 2, 3, 4, 5, 6, 0, 0, 0, 0 };

        var fixedBytes = fixedSerde.Serialize(original, context);
        var oracleBytes = oracle.Serialize(original, context);
        Assert.Equal(expected, fixedBytes);
        Assert.Equal(expected, oracleBytes);
        AssertWindow(oracle.Deserialize(fixedBytes, context), start);
        AssertWindow(fixedSerde.Deserialize(oracleBytes, context), start);
        BinaryPrimitives.WriteInt32BigEndian(oracleBytes.AsSpan(oracleBytes.Length - 4), 0x11223344);
        AssertWindow(fixedSerde.Deserialize(oracleBytes, context), start);
    }

    [Fact]
    public void DefaultTopicWriterUsesExactEightByteTimestampSuffix()
    {
        var serde = new FixedTimeWindowedAvroSerDes(new FramedSerDes(true), 60_000);
        var original = new Windowed<GenericRecord>(CreateRecord("key"), new TimeWindow(0x010203040506L, 0x010203040506L + 60_000));
        Assert.Equal(new byte[] { 0, 0, 0, 0, 17, 3, 107, 101, 121, 0, 0, 1, 2, 3, 4, 5, 6 },
            serde.Serialize(original, new SerializationContext(MessageComponentType.Key, "topic")));
    }

    [Theory]
    [InlineData(false, 8)]
    [InlineData(true, 12)]
    public void DeclaredFormatPreservesTombstonesAndRejectsMissingKey(bool store, int suffixSize)
    {
        var serde = new FixedTimeWindowedAvroSerDes(new FramedSerDes(true), 60_000, store ? WindowedKeyFormat.Store : WindowedKeyFormat.Topic);
        var context = new SerializationContext(MessageComponentType.Key, "bounds");
        Assert.Null(serde.Serialize(null!, context));
        Assert.Null(serde.Deserialize(null!, context));
        Assert.Null(serde.Deserialize(Array.Empty<byte>(), context));
        Assert.Throws<AvroException>(() => serde.Deserialize(new byte[suffixSize], context));
        Assert.Throws<AvroException>(() => serde.Deserialize(new byte[suffixSize + 1], context));
    }

    [Fact]
    public void TopicModeDoesNotFallBackToStoreWhenInnerRejectsExtraBytes()
    {
        var inner = new FramedSerDes(true);
        var context = new SerializationContext(MessageComponentType.Key, "wrong-format");
        var store = new FixedTimeWindowedAvroSerDes(inner, 60_000, WindowedKeyFormat.Store);
        var topic = new FixedTimeWindowedAvroSerDes(inner, 60_000);
        var data = new Windowed<GenericRecord>(CreateRecord("key"), new TimeWindow(1_000, 61_000));
        Assert.Throws<AvroException>(() => topic.Deserialize(store.Serialize(data, context), context));
    }

    private static void AssertWindow(Windowed<GenericRecord> result, long start)
    {
        Assert.Equal("key", result.Key["value"]);
        Assert.Equal(start, result.Window.StartMs);
        Assert.Equal(start + 60_000, result.Window.EndMs);
    }

    // A Confluent-style header and a length-prefixed test body let strict and
    // permissive decoders share identical bytes while exposing boundary guessing.
    private sealed class FramedSerDes : AbstractSerDes<GenericRecord>
    {
        private readonly bool _strict;
        public byte[]? LastRead { get; private set; }
        public FramedSerDes(bool strict) => _strict = strict;
        public override byte[] Serialize(GenericRecord data, SerializationContext context)
        {
            var value = Encoding.UTF8.GetBytes((string)data["value"]);
            var bytes = new byte[value.Length + 6];
            bytes[4] = 17;
            bytes[5] = checked((byte)value.Length);
            value.CopyTo(bytes, 6);
            return bytes;
        }
        public override GenericRecord Deserialize(byte[] data, SerializationContext context)
        {
            LastRead = data;
            if (data.Length < 6 || data[0] != 0 || data[4] != 17 || data.Length < data[5] + 6 || (_strict && data.Length != data[5] + 6))
                throw new AvroException("Invalid framed test key.");
            return CreateRecord(Encoding.UTF8.GetString(data, 6, data[5]));
        }
        public override void Initialize(SerDesContext context) { }
    }

    private sealed class StubGenericRecordSerDes : AbstractSerDes<GenericRecord>
    {
        private readonly RecordSchema _schema;

        public StubGenericRecordSerDes(RecordSchema schema)
        {
            _schema = schema;
        }

        public override GenericRecord Deserialize(byte[] data, SerializationContext context)
        {
            var text = data == null ? string.Empty : Encoding.UTF8.GetString(data);
            var record = new GenericRecord(_schema);
            record.Add("value", text);
            return record;
        }

        public override byte[] Serialize(GenericRecord data, SerializationContext context)
        {
            if (data == null)
                return Array.Empty<byte>();

            return Encoding.UTF8.GetBytes(data["value"]?.ToString() ?? string.Empty);
        }

        public override void Initialize(SerDesContext context)
        {
            // no-op
        }
    }
}
