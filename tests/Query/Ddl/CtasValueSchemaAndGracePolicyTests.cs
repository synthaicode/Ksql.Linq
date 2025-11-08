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
public class CtasValueSchemaAndGracePolicyTests
{
    [KsqlTopic("bar")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static async Task<(ConcurrentBag<string> ddls, KsqlQueryModel model)> RunAsync(bool addGrace, int graceSec = 1)
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
        if (addGrace) model.Extras["graceSeconds"] = graceSec;

        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql)
        { ddls.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn_ctas_val"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));
        return (ddls, model);
    }

    [Fact]
    public async Task LiveCtas_Omits_ValueSchemaFullName_When_NotProvided()
    {
        var (ddls, _) = await RunAsync(addGrace: false);
        var live = ddls.First(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain("VALUE_AVRO_SCHEMA_FULL_NAME", live, StringComparison.OrdinalIgnoreCase);
        SqlAssert.ContainsNormalized(live, "WINDOW TUMBLING (SIZE 1 MINUTES, GRACE PERIOD 1 SECONDS)");
    }

    [Fact]
    public async Task LiveCtas_Includes_Grace_When_Provided()
    {
        var (ddls, _) = await RunAsync(addGrace: true, graceSec: 1);
        var live = ddls.First(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));
        SqlAssert.ContainsNormalized(live, "GRACE PERIOD 1 SECONDS");
    }

    [Fact]
    public async Task LiveCtas_Defaults_Grace_When_NotProvided()
    {
        var (ddls, _) = await RunAsync(addGrace: false);
        var live = ddls.First(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));
        SqlAssert.ContainsNormalized(live, "GRACE PERIOD 1 SECONDS");
    }
}
