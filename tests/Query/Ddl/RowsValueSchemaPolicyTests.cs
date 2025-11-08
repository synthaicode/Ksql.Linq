using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;
using EntityModel = Ksql.Linq.Core.Abstractions.EntityModel;

namespace Ksql.Linq.Tests.Query.Ddl;

[Trait("Level", TestLevel.L3)]
public class RowsValueSchemaPolicyTests
{
    [KsqlTopic("bar")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [Fact]
    public async Task RowsStream_Omits_ValueSchemaFullName_ByDefault()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            PocoShape = new[]
            {
                new ColumnShape("Broker", typeof(string), false),
                new ColumnShape("Symbol", typeof(string), false),
                new ColumnShape("Timestamp", typeof(DateTime), false),
                new ColumnShape("Bid", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Broker", "Symbol" }, "Bid", "Bid", string.Empty)
        };
        var baseModel = new EntityModel { EntityType = typeof(Rate) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(Rate) } };
        model.Windows.Add("1m");

        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql)
        {
            ddls.Add(sql);
            return Task.FromResult(new KsqlDbResponse(true, string.Empty));
        }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn_rows_value"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;

        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));
        var ddl = ddls.First(s => s.StartsWith("CREATE STREAM IF NOT EXISTS bar_1s_rows", StringComparison.OrdinalIgnoreCase));

        // rows縺ｯVALUE_AVRO_SCHEMA_FULL_NAME繧貞ｼｷ蛻ｶ縺励↑縺・ｼ・enericRecord謨ｴ蜷茨ｼ・
        Assert.DoesNotContain("VALUE_AVRO_SCHEMA_FULL_NAME", ddl, StringComparison.OrdinalIgnoreCase);
        SqlAssert.ContainsNormalized(ddl, "(BROKER VARCHAR KEY, SYMBOL VARCHAR KEY, TIMESTAMP TIMESTAMP, BID DOUBLE)");
        SqlAssert.ContainsNormalized(ddl, "WITH (KAFKA_TOPIC='BAR_1S_ROWS'");
        SqlAssert.ContainsNormalized(ddl, "VALUE_FORMAT='AVRO'");
        SqlAssert.ContainsNormalized(ddl, "RETENTION_MS=604800000");
        SqlAssert.ContainsNormalized(ddl, "TIMESTAMP='TIMESTAMP'");
    }
}