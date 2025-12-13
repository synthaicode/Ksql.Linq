using System;
using System.Reflection;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Runtime.Schema;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Runtime;

[Trait("Level", TestLevel.L3)]
public class HoppingMetadataTests
{
    private class Dummy : IWindowedRecord
    {
        public string Id { get; set; } = string.Empty;
        public DateTime Ts { get; set; }
        public DateTime WindowStart => Ts;
        public DateTime WindowEnd => Ts.AddMinutes(5);
    }

    [Fact]
    public void ApplyHoppingMetadata_SetsRoleTimeframeGraceAndTimeKey()
    {
        var model = new EntityModel
        {
            EntityType = typeof(Dummy),
            QueryModel = new KsqlQueryModel
            {
                TimeKey = nameof(Dummy.Ts),
                Hopping = new HoppingWindowSpec
                {
                    Size = TimeSpan.FromMinutes(5),
                    Advance = TimeSpan.FromMinutes(1),
                    Grace = TimeSpan.FromSeconds(30)
                }
            }
        };

        var method = typeof(SchemaRegistrar).GetMethod("ApplyHoppingMetadata", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        method!.Invoke(null, new object[] { model });

        var metadata = model.QueryMetadata!;
        Assert.Equal("Live", metadata.Role);
        Assert.Equal("5m", metadata.TimeframeRaw);
        Assert.Equal(30, metadata.GraceSeconds);
        Assert.Equal(nameof(Dummy.Ts), metadata.TimeKey);
    }
}
