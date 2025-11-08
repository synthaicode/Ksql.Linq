using System;
using System.Buffers.Binary;
using System.Text;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Ksql.Linq.SerDes;
using Streamiz.Kafka.Net.SerDes;
using Xunit;

namespace Ksql.Linq.Tests.SerDes;

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