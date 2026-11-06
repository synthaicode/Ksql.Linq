using Avro.Generic;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using System;
using System.Linq;
using Xunit;

namespace Ksql.Linq.Tests.Mapping;

[Trait("Level", "L3")]
public class GuidStringKeyValidationRegressionTests
{
    private static readonly Guid ExpectedId = new("d85b1407-351d-4694-9392-03acc5870eb1");
    private static readonly Guid ValueId = new("4b82473b-67bd-4c59-810f-9b78e37ef324");

    public class GuidEntity
    {
        public Guid Id { get; set; }
        public string Payload { get; set; } = string.Empty;
        public int Count { get; set; }
    }

    public class NullableGuidEntity
    {
        public Guid? Id { get; set; }
        public string Payload { get; set; } = string.Empty;
        public int Count { get; set; }
    }

    [Theory]
    [InlineData(false, "not-a-guid")]
    [InlineData(true, "not-a-guid")]
    [InlineData(false, "d85b1407-351d-4694-9392-03acc5870eb")]
    [InlineData(true, "d85b1407-351d-4694-9392-03acc5870eb")]
    [InlineData(false, " \t\r\n")]
    [InlineData(true, " \t\r\n")]
    public void NonemptyMalformedKey_ThrowsFormatException(bool nullable, string key)
    {
        var (mapping, value, entityType) = CreateMapping(nullable);

        Assert.Throws<FormatException>(() =>
            mapping.CombineFromStringKeyAndAvroValue(key, value, entityType));
    }

    [Theory]
    [InlineData(false, "D")]
    [InlineData(true, "D")]
    [InlineData(false, "N")]
    [InlineData(true, "N")]
    [InlineData(false, "B")]
    [InlineData(true, "B")]
    [InlineData(false, "P")]
    [InlineData(true, "P")]
    [InlineData(false, "X")]
    [InlineData(true, "X")]
    public void ValidKeyFormats_OverrideValueIdAndPreservePayload(bool nullable, string format)
    {
        var (mapping, value, entityType) = CreateMapping(nullable);
        var key = " \t" + ExpectedId.ToString(format).ToUpperInvariant() + "\r\n ";

        var result = mapping.CombineFromStringKeyAndAvroValue(key, value, entityType);

        AssertEntity(result, ExpectedId);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GenuineZeroKey_OverridesNonzeroValueId(bool nullable)
    {
        var (mapping, value, entityType) = CreateMapping(nullable);

        var result = mapping.CombineFromStringKeyAndAvroValue(Guid.Empty.ToString("D"), value, entityType);

        AssertEntity(result, Guid.Empty);
    }

    [Theory]
    [InlineData(false, null)]
    [InlineData(true, null)]
    [InlineData(false, "")]
    [InlineData(true, "")]
    public void MissingKey_FallsBackToValueId(bool nullable, string? key)
    {
        var (mapping, value, entityType) = CreateMapping(nullable);

        var result = mapping.CombineFromStringKeyAndAvroValue(key!, value, entityType);

        AssertEntity(result, ValueId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void MissingNullableKey_WithNullValueId_RemainsNull(string? key)
    {
        var (mapping, value, entityType) = CreateMapping(nullable: true);
        value.Add(nameof(NullableGuidEntity.Id), null);

        var result = mapping.CombineFromStringKeyAndAvroValue(key!, value, entityType);

        AssertEntity(result, null);
    }

    [Theory]
    [InlineData(false, "not-a-guid")]
    [InlineData(true, "not-a-guid")]
    [InlineData(false, null)]
    [InlineData(true, null)]
    [InlineData(false, "")]
    [InlineData(true, "")]
    public void Tombstone_ReturnsDefaultEntityWithoutParsingKey(bool nullable, string? key)
    {
        var (mapping, _, entityType) = CreateMapping(nullable);

        var result = mapping.CombineFromStringKeyAndAvroValue(key!, null!, entityType);

        Assert.Equal(nullable ? (Guid?)null : Guid.Empty, entityType.GetProperty("Id")!.GetValue(result));
        Assert.Equal(string.Empty, entityType.GetProperty("Payload")!.GetValue(result));
        Assert.Equal(0, entityType.GetProperty("Count")!.GetValue(result));
    }

    private static (KeyValueTypeMapping Mapping, GenericRecord Value, Type EntityType) CreateMapping(bool nullable)
    {
        var entityType = nullable ? typeof(NullableGuidEntity) : typeof(GuidEntity);
        var keys = new[] { PropertyMeta.FromProperty(entityType.GetProperty("Id")!) };
        var values = entityType.GetProperties().Select(p => PropertyMeta.FromProperty(p)).ToArray();
        var mapping = new MappingRegistry().Register(entityType, keys, values, genericValue: true);
        var value = new GenericRecord(mapping.AvroValueRecordSchema!);
        value.Add("Id", ValueId.ToString("D"));
        value.Add("Payload", "preserved payload");
        value.Add("Count", 37);
        return (mapping, value, entityType);
    }

    private static void AssertEntity(object result, Guid? expectedId)
    {
        var entityType = result.GetType();
        Assert.Equal(expectedId, entityType.GetProperty("Id")!.GetValue(result));
        Assert.Equal("preserved payload", entityType.GetProperty("Payload")!.GetValue(result));
        Assert.Equal(37, entityType.GetProperty("Count")!.GetValue(result));
    }
}
