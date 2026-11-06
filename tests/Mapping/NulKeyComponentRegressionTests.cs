using Avro;
using Avro.Generic;
using Avro.Specific;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Xunit;

namespace Ksql.Linq.Tests.Mapping;

[Trait("Level", "L3")]
public class NulKeyComponentRegressionTests
{
    public class CompositeEntity
    {
        public string? First { get; set; }
        public string? Second { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    public class TypedEntity
    {
        public int Number { get; set; }
        public Guid Id { get; set; }
        public DateTime BucketStart { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    [Theory]
    [InlineData("generic", "a\0b", "c")]
    [InlineData("generic", "a", "b\0c")]
    [InlineData("specific", "a\0b", "c")]
    [InlineData("specific", "a", "b\0c")]
    [InlineData("fallback", "a\0b", "c")]
    [InlineData("fallback", "a", "b\0c")]
    [InlineData("generic", "\0", "")]
    [InlineData("specific", "", "\0")]
    [InlineData("fallback", "", "\0")]
    public void PreviouslyConflictingTuples_RejectNulBeforeJoining(string path, string first, string second)
    {
        var mapping = CompositeMapping(path);
        var entity = new CompositeEntity { First = first, Second = second };
        var key = CreateKey(mapping, entity, path);

        Assert.Throws<ArgumentException>(() => mapping.FormatKeyForPrefix(key));
    }

    [Theory]
    [InlineData("generic")]
    [InlineData("specific")]
    [InlineData("fallback")]
    public void NormalTuple_PreservesExactUtf8BytesAndRoundTrip(string path)
    {
        var mapping = CompositeMapping(path);
        var entity = new CompositeEntity { First = "東京", Second = "a|b", Payload = "preserved" };
        var key = mapping.FormatKeyForPrefix(CreateKey(mapping, entity, path));

        Assert.Equal(Encoding.UTF8.GetBytes("東京\0a|b"), Encoding.UTF8.GetBytes(key));
        var restored = (CompositeEntity)mapping.CombineFromStringKeyAndAvroValue(
            key, mapping.ExtractAvroValue(entity), typeof(CompositeEntity));
        Assert.Equal(entity.First, restored.First);
        Assert.Equal(entity.Second, restored.Second);
        Assert.Equal(entity.Payload, restored.Payload);
    }

    [Theory]
    [InlineData("generic", null)]
    [InlineData("generic", "")]
    [InlineData("specific", null)]
    [InlineData("specific", "")]
    [InlineData("fallback", null)]
    [InlineData("fallback", "")]
    public void EmptyAndNullComponents_PreserveDelimiterAndValueFallback(string path, string? missing)
    {
        var mapping = CompositeMapping(path);
        var entity = new CompositeEntity { First = missing, Second = "normal" };
        var key = mapping.FormatKeyForPrefix(CreateKey(mapping, entity, path));
        Assert.Equal("\0normal", key);
        var value = mapping.ExtractAvroValue(new CompositeEntity { First = "value fallback", Payload = "kept" });
        var restored = (CompositeEntity)mapping.CombineFromStringKeyAndAvroValue(key, value, typeof(CompositeEntity));
        Assert.Equal("value fallback", restored.First);
        Assert.Equal("normal", restored.Second);
        Assert.Equal("kept", restored.Payload);
    }

    [Fact]
    public void WindowKeyWithoutTimestamp_PreservesEmptySlotAndValueTimestamp()
    {
        var properties = typeof(TypedEntity).GetProperties();
        var mapping = new MappingRegistry().Register(typeof(TypedEntity),
            properties.Take(3).Select(p => PropertyMeta.FromProperty(p)).ToArray(),
            properties.Select(p => PropertyMeta.FromProperty(p)).ToArray(), genericKey: true, genericValue: true);
        var schema = (RecordSchema)Schema.Parse("{\"type\":\"record\",\"name\":\"window_key\",\"fields\":[{\"name\":\"Number\",\"type\":\"int\"},{\"name\":\"Id\",\"type\":\"string\"}]}");
        var record = new GenericRecord(schema);
        record.Add("Number", 7);
        record.Add("Id", Guid.Empty.ToString("D"));
        var timestamp = new DateTime(2026, 11, 6, 0, 0, 0, DateTimeKind.Utc);
        var key = mapping.FormatKeyForPrefix(record);
        Assert.Equal("7\000000000-0000-0000-0000-000000000000\0", key);
        var restored = (TypedEntity)mapping.CombineFromStringKeyAndAvroValue(key,
            mapping.ExtractAvroValue(new TypedEntity { BucketStart = timestamp }), typeof(TypedEntity));
        Assert.Equal(timestamp, restored.BucketStart);
        Assert.Equal(7, restored.Number);
    }

    [Fact]
    public void TypedKeys_PreserveInvariantFormatAndSupportedWindowTail()
    {
        var properties = typeof(TypedEntity).GetProperties();
        var mapping = new MappingRegistry().Register(typeof(TypedEntity),
            properties.Take(3).Select(p => PropertyMeta.FromProperty(p)).ToArray(),
            properties.Select(p => PropertyMeta.FromProperty(p)).ToArray(), genericValue: true);
        var entity = new TypedEntity { Number = -42, Id = Guid.Parse("d85b1407-351d-4694-9392-03acc5870eb1"),
            BucketStart = new DateTime(2026, 11, 6, 1, 2, 3, 456, DateTimeKind.Utc), Payload = "kept" };
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            var key = mapping.FormatKeyForPrefix(mapping.ExtractAvroKey(entity));
            Assert.Equal("-42\0d85b1407-351d-4694-9392-03acc5870eb1\020261106T010203456Z", key);
            var restored = (TypedEntity)mapping.CombineFromStringKeyAndAvroValue(
                key + "\01792600000000", mapping.ExtractAvroValue(entity), typeof(TypedEntity));
            Assert.Equal(entity.Number, restored.Number);
            Assert.Equal(entity.Id, restored.Id);
            Assert.Equal(entity.BucketStart, restored.BucketStart);
            Assert.Equal(entity.Payload, restored.Payload);
        }
        finally { CultureInfo.CurrentCulture = previous; }
    }

    private static KeyValueTypeMapping CompositeMapping(string path)
    {
        var properties = typeof(CompositeEntity).GetProperties();
        return new MappingRegistry().Register(typeof(CompositeEntity),
            properties.Take(2).Select(p => PropertyMeta.FromProperty(p)).ToArray(),
            properties.Select(p => PropertyMeta.FromProperty(p)).ToArray(), genericKey: path == "generic", genericValue: true);
    }

    private static object CreateKey(KeyValueTypeMapping mapping, CompositeEntity entity, string path)
    {
        if (path == "generic")
        {
            var record = new GenericRecord((RecordSchema)Schema.Parse(mapping.AvroKeySchema!));
            record.Add(nameof(entity.First), entity.First);
            record.Add(nameof(entity.Second), entity.Second);
            return record;
        }
        if (path == "fallback") return mapping.ExtractKey(entity);
        var specific = mapping.ExtractAvroKey(entity);
        Assert.IsAssignableFrom<ISpecificRecord>(specific);
        return specific;
    }
}
