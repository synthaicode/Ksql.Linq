using Avro;
using Avro.Generic;
using Ksql.Linq.Messaging.Producers;
using System.Reflection;
using Xunit;

namespace Ksql.Linq.Tests.Messaging;

[Trait("Level", "L3")]
public class NullableStringPreservationRegressionTests
{
    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("value")]
    public void Normalize_NullableString_PreservesPresentValue(string? value)
    {
        var schema = CreateSchema(nullable: true);
        var record = new GenericRecord(schema);
        record.Add("Text", value);

        Normalize(record, schema);

        Assert.Equal(value, record.GetValue(schema.Fields[0].Pos));
    }

    [Theory]
    [InlineData("")]
    [InlineData("value")]
    public void Normalize_RequiredString_PreservesPresentValue(string value)
    {
        var schema = CreateSchema(nullable: false);
        var record = new GenericRecord(schema);
        record.Add("Text", value);

        Normalize(record, schema);

        Assert.Equal(value, record.GetValue(schema.Fields[0].Pos));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Normalize_MissingField_PreservesExistingNullHandling(bool nullable)
    {
        var schema = CreateSchema(nullable);
        var record = new GenericRecord(schema);

        Normalize(record, schema);

        Assert.Null(record.GetValue(schema.Fields[0].Pos));
    }

    private static RecordSchema CreateSchema(bool nullable)
    {
        var fieldType = nullable ? "[\"null\",\"string\"]" : "\"string\"";
        return (RecordSchema)Schema.Parse(
            "{\"type\":\"record\",\"name\":\"StringPreservation\",\"fields\":[" +
            "{\"name\":\"Text\",\"type\":" + fieldType + "}]}");
    }

    private static void Normalize(GenericRecord record, RecordSchema schema)
    {
        // Both ordinary sends and schema-alignment retries use this boundary.
        var method = typeof(KafkaProducerManager).GetMethod(
            "EnsureAllFieldsAssigned", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);
        method.Invoke(null, new object[] { record, schema });
    }
}
