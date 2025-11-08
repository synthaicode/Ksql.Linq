using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Ddl;
using Ksql.Linq.Query.Pipeline;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Ddl;

[Trait("Level", TestLevel.L3)]
public class DdlTimestampAndOneSecondDependencyTests
{
    private class RateTs
    {
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("bar")]
    private class BaseEntity { public int Id { get; set; } public long BucketStart { get; set; } }

    [Fact]
    public void GenerateCreateStream_Includes_Timestamp_In_WithClause()
    {
        var model = new EntityModel { EntityType = typeof(RateTs) };
        model.SetStreamTableType(StreamTableType.Stream);
        var adapter = new EntityModelDdlAdapter(model);
        var gen = new DDLQueryGenerator();
        string ddl;
        using (Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            ddl = gen.GenerateCreateStream(adapter);
        }
        Assert.Contains("CREATE STREAM IF NOT EXISTS", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("KAFKA_TOPIC='ratets'", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE_FORMAT='AVRO'", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OneSecond_TableFromSource_Then_StreamDefinition()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id" }, string.Empty, "BucketStart", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(BaseEntity) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(BaseEntity) }, Windows = { "1m" } };
        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql) { ddls.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new System.Reflection.AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        Assert.DoesNotContain(ddls, s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1s_final", StringComparison.OrdinalIgnoreCase));

        var str = ddls.Single(s => s.StartsWith("CREATE STREAM IF NOT EXISTS bar_1s_rows", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain("AS SELECT", str, StringComparison.OrdinalIgnoreCase); // definition, not CSAS
        Assert.Contains("KEY_FORMAT='AVRO'", str, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("WINDOW TUMBLING", str, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("VALUE_AVRO_SCHEMA_FULL_NAME", str, StringComparison.OrdinalIgnoreCase);
        SqlAssert.ContainsNormalized(str, "(ID INT KEY, BUCKETSTART BIGINT KEY, TIMESTAMP TIMESTAMP)");
        SqlAssert.ContainsNormalized(str, "TIMESTAMP='TIMESTAMP'");
    }

}