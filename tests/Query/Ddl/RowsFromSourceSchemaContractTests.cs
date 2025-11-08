using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Query.Ddl;

[Trait("Level", "L3")]
public class RowsFromSourceSchemaContractTests
{
    [KsqlTopic("rate")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [Fact]
    public async Task OneSecondRows_DefinesColumns_From_SourcePoco()
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
        var baseModel = new Ksql.Linq.Core.Abstractions.EntityModel { EntityType = typeof(Rate) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(Rate) } };
        model.Windows.Add("1m");

        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(Ksql.Linq.Core.Abstractions.EntityModel _, string sql)
        { ddls.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, Ksql.Linq.Core.Abstractions.EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn_rows_src"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        var rows = ddls.First(s => s.StartsWith("CREATE STREAM IF NOT EXISTS rate_1s_rows", StringComparison.OrdinalIgnoreCase));
        var upper = rows.ToUpperInvariant();
        Assert.Contains("(BROKER VARCHAR KEY", upper);
        Assert.Contains("SYMBOL VARCHAR KEY", upper);
        Assert.Contains("TIMESTAMP TIMESTAMP", upper);
        Assert.Contains("BID DOUBLE", upper);
        Assert.Contains("WITH (", upper);
        Assert.Contains("TIMESTAMP='TIMESTAMP'", upper);
    }
}