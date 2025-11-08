using Avro.Generic;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using Ksql.Linq.Tests.Utils;
using System;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Ksql.Linq.Tests.Query.Mapping;

[Trait("Level", TestLevel.L2)]
public class DateTimeTimestampMillisTests
{
    private class RowRecord
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    [Fact]
    public void GenericRecord_Uses_TimestampMillis_For_DateTime()
    {
        var poco = new RowRecord
        {
            Broker = "brk",
            Symbol = "sym",
            Timestamp = new DateTime(2025, 1, 1, 12, 0, 5, DateTimeKind.Utc),
            Open = 1.20,
            High = 1.30,
            Low = 1.10,
            Close = 1.23
        };

        var keyMeta = new[]
        {
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.Broker))!),
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.Symbol))!)
        };
        var valueMeta = new[]
        {
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.Timestamp))!),
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.Open))!),
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.High))!),
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.Low))!),
            PropertyMeta.FromProperty(typeof(RowRecord).GetProperty(nameof(RowRecord.Close))!)
        };

        var reg = new MappingRegistry();
        var kv = reg.Register(typeof(RowRecord), keyMeta, valueMeta, topicName: "mapping_test_rows", genericKey: true, genericValue: true, overrideNamespace: "runtime_bar_ksql");

        Assert.NotNull(kv.AvroValueRecordSchema);
        var valueSchema = kv.AvroValueRecordSchema!;
        var keySchema = kv.AvroKeyRecordSchema!;

        var vrec = new GenericRecord(valueSchema);
        var krec = new GenericRecord(keySchema);

        var schemaJson = kv.AvroValueSchema!;
        Assert.Contains("\"logicalType\":\"timestamp-millis\"", schemaJson, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"name\":\"Open\"", schemaJson, StringComparison.OrdinalIgnoreCase);

        // Act
        kv.PopulateAvroKeyValue(poco, krec, vrec);

        // Assert value fields
        var tsField = kv.AvroValueRecordSchema!.Fields.First(f => string.Equals(f.Name, "TIMESTAMP", StringComparison.OrdinalIgnoreCase)).Name;
        var tsObj = vrec[tsField];
        var ts = Assert.IsType<DateTime>(tsObj);
        Assert.Equal(poco.Timestamp, ts.ToUniversalTime());

        var closeField = kv.AvroValueRecordSchema!.Fields.First(f => string.Equals(f.Name, "CLOSE", StringComparison.OrdinalIgnoreCase)).Name;
        var closeObj = vrec[closeField];
        Assert.Equal(1.23, (double)closeObj!, 3);

        // Assert key fields exist
        var brokerField = kv.AvroKeyRecordSchema!.Fields.First(f => string.Equals(f.Name, "BROKER", StringComparison.OrdinalIgnoreCase)).Name;
        var symbolField = kv.AvroKeyRecordSchema!.Fields.First(f => string.Equals(f.Name, "SYMBOL", StringComparison.OrdinalIgnoreCase)).Name;
        Assert.Equal("brk", krec[brokerField]);
        Assert.Equal("sym", krec[symbolField]);
    }
}

