using System.Collections.Generic;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Query.Metadata;
using Xunit;

namespace Ksql.Linq.Tests.Application;

public class KsqlContextMetadataTests
{
    [Fact]
    public void QueryMetadataFactory_Reads_ConfigOverrides_FromAdditionalSettings()
    {
        var settings = new Dictionary<string, object>
        {
            ["id"] = "entity_1m_live",
            ["StoreName"] = "custom_store",
            ["BaseDirectory"] = "/var/cache/custom",
            ["timeframe"] = "1m",
            ["retention.ms"] = "86400000",
            ["role"] = "Live",
            ["keys"] = new[] { "Id" },
            ["keys/types"] = new[] { typeof(int) },
            ["keys/nulls"] = new[] { false },
            ["projection"] = new[] { "Value" },
            ["projection/types"] = new[] { typeof(string) },
            ["projection/nulls"] = new[] { true },
        };

        var metadata = QueryMetadataFactory.FromAdditionalSettings(settings);

        Assert.Equal("custom_store", metadata.StoreName);
        Assert.Equal("/var/cache/custom", metadata.BaseDirectory);
        Assert.Equal("1m", metadata.TimeframeRaw);
        Assert.Equal("Live", metadata.Role);
        Assert.Equal(86400000L, metadata.RetentionMs);
        Assert.Equal(new[] { "Id" }, metadata.Keys.Names);
        Assert.Equal(new[] { typeof(int) }, metadata.Keys.Types);
        Assert.Equal(new[] { false }, metadata.Keys.NullableFlags);
        Assert.Equal(new[] { "Value" }, metadata.Projection.Names);
        Assert.Equal(new[] { typeof(string) }, metadata.Projection.Types);
        Assert.Equal(new[] { true }, metadata.Projection.NullableFlags);
    }

    [Fact]
    public void QueryMetadataWriter_Apply_UpdatesMetadataAndDictionary()
    {
        var model = new EntityModel();
        var metadata = new QueryMetadata
        {
            Identifier = "entity_1m_live",
            Namespace = "runtime_entity_1m_live_ksql",
            Role = "Live",
            TimeframeRaw = "1m",
            GraceSeconds = 15,
            Keys = new QueryKeyShape(
                Names: new[] { "Broker", "Symbol" },
                Types: new[] { typeof(string), typeof(string) },
                NullableFlags: new[] { false, false }),
            Projection = new QueryProjectionShape(
                Names: new[] { "Open", "Close" },
                Types: new[] { typeof(double), typeof(double) },
                NullableFlags: new[] { false, false }),
            RetentionMs = 172800000
        };

        QueryMetadataWriter.Apply(model, metadata);

        Assert.Same(metadata, model.QueryMetadata);
        Assert.Equal("entity_1m_live", model.AdditionalSettings["id"]);
        Assert.Equal("runtime_entity_1m_live_ksql", model.AdditionalSettings["namespace"]);
        Assert.Equal("Live", model.AdditionalSettings["role"]);
        Assert.Equal("1m", model.AdditionalSettings["timeframe"]);
        Assert.Equal(15, model.AdditionalSettings["graceSeconds"]);
        Assert.Equal(172800000L, (long)model.AdditionalSettings["retentionMs"]);
        Assert.Equal(172800000L, (long)model.AdditionalSettings["retention.ms"]);

        Assert.Equal(new[] { "Broker", "Symbol" }, model.AdditionalSettings["keys"]);
        Assert.Equal(new[] { typeof(string), typeof(string) }, model.AdditionalSettings["keys/types"]);
        Assert.Equal(new[] { false, false }, model.AdditionalSettings["keys/nulls"]);
        Assert.Equal(new[] { "Open", "Close" }, model.AdditionalSettings["projection"]);
        Assert.Equal(new[] { typeof(double), typeof(double) }, model.AdditionalSettings["projection/types"]);
        Assert.Equal(new[] { false, false }, model.AdditionalSettings["projection/nulls"]);
    }
}
