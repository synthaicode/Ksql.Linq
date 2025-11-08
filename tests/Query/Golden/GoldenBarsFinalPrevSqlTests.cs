using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

public class GoldenBarsFinalPrevSqlTests
{
    [KsqlTopic("bar")]
    private class BaseEntity
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static async Task<string[]> BuildAllDdlsAsync()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Broker", "Symbol", "BucketStart" },
            Projection = new[] { "Broker", "Symbol", "BucketStart", "Open", "High", "Low", "Close" },
            PocoShape = new[]
            {
                new ColumnShape("Broker", typeof(string), false),
                new ColumnShape("Symbol", typeof(string), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("Close", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Broker", "Symbol" }, "Open", "Close", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(BaseEntity) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(BaseEntity) }, Windows = { "1m" } };
        // Legacy prev/fill removed; live only

        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql)
        {
            ddls.Add(sql);
            return Task.FromResult(new KsqlDbResponse(true, string.Empty));
        }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));
        return ddls.ToArray();
    }

    [Fact(Skip = "WhenEmpty policy no longer creates final/prev; covered by WhenEmpty live golden tests.")]
    public async Task Bars1mFinal_Equals_Golden()
    {
        var ddls = await BuildAllDdlsAsync();
        var final = ddls.First(s => s.IndexOf("bar_1m_final", StringComparison.OrdinalIgnoreCase) >= 0);
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/bars_1m_final.sql", final);
    }

    [Fact(Skip = "WhenEmpty policy no longer creates final/prev; covered by WhenEmpty live golden tests.")]
    public async Task BarsPrev1m_Equals_Golden()
    {
        var ddls = await BuildAllDdlsAsync();
        var prev = ddls.First(s => s.IndexOf("bar_prev_1m", StringComparison.OrdinalIgnoreCase) >= 0);
        GoldenSqlHelpers.AssertEqualsOrUpdate("tests/Query/Golden/bars_prev_1m.sql", prev);
    }
}