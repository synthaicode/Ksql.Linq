using System;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Metadata;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class DerivedEntityDdlPlannerTests
{
    private sealed class SourceEntity
    {
        [KsqlKey(1)]
        public string Broker { get; set; } = string.Empty;

        public double Open { get; set; }
    }

    private sealed class DerivedEntity { }

    [Fact]
    public void Build_LiveRole_RespectsMetadataNamespaceAndGrace()
    {
        var model = new EntityModel
        {
            EntityType = typeof(DerivedEntity),
            Partitions = 1,
            ReplicationFactor = 1
        };
        model.SetStreamTableType(StreamTableType.Table);

        var metadata = new QueryMetadata
        {
            Identifier = "bars_1m_live",
            Namespace = "runtime_bars_1m_live_ksql",
            Role = "Live",
            TimeframeRaw = "1m",
            GraceSeconds = 30,
            Keys = new QueryKeyShape(
                Names: new[] { "Broker" },
                Types: new[] { typeof(string) },
                NullableFlags: new[] { false }),
            Projection = new QueryProjectionShape(
                Names: new[] { "Open" },
                Types: new[] { typeof(double) },
                NullableFlags: new[] { false })
        };
        QueryMetadataWriter.Apply(model, metadata);

        var queryModel = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(SourceEntity) },
            SelectProjection = (Expression<Func<SourceEntity, object>>)(s => new { s.Open }),
            GroupByExpression = (Expression<Func<SourceEntity, object>>)(s => new { s.Broker })
        };
        queryModel.Windows.Add("1m");

        var (ddl, entityType, ns, inputOverride, shouldExecute) = DerivedEntityDdlPlanner.Build(
            baseName: "bars",
            queryModel: queryModel,
            model: model,
            role: Role.Live,
            resolveType: name => typeof(DerivedEntity),
            defaultRowsStreamRetentionMs: 7L * 24 * 60 * 60 * 1000);

        Assert.True(shouldExecute);
        Assert.Null(inputOverride);
        Assert.Equal(typeof(DerivedEntity), entityType);
        Assert.Equal("runtime_bars_1m_live_ksql", ns);
        Assert.Contains("GRACE PERIOD 30 SECONDS", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("runtime_bars_1m_live_ksql", model.QueryMetadata!.Namespace);
        Assert.Equal("bars_1m_live", model.QueryMetadata.Identifier);
        Assert.Equal("1m", model.QueryMetadata.TimeframeRaw);
    }
}
