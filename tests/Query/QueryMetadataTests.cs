using Ksql.Linq.Query.Pipeline;
using System;
using Xunit;

namespace Ksql.Linq.Tests.Query;

public class QueryPipelineMetadataTests
{
    [Fact]
    public void WithProperty_AddsProperty()
    {
        var meta = new QueryPipelineMetadata(DateTime.UtcNow, "DML");
        var updated = meta.WithProperty("key", 1);
        Assert.Equal(1, updated.Properties!["key"]);
        Assert.Null(meta.Properties);
    }

    [Fact]
    public void GetProperty_ReturnsTypedValue()
    {
        var meta = new QueryPipelineMetadata(DateTime.UtcNow, "DML", null, new System.Collections.Generic.Dictionary<string, object> { { "val", 5 } });
        int? val = meta.GetProperty<int>("val");
        Assert.Equal(5, val);
        Assert.Null(meta.GetProperty<string>("missing"));
    }
}