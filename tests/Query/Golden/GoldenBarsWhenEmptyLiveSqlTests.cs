using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Abstractions;
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
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

[Trait("Level", TestLevel.L4)]
public class GoldenBarsWhenEmptyLiveSqlTests
{
    [KsqlTopic("bar")]
    private class BaseEntity
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static async Task<string[]> BuildDdlsAsync()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Broker", "Symbol", "BucketStart" },
            Projection = new[] { "Broker", "Symbol", "BucketStart", "Open", "High", "Low", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Broker", typeof(string), false),
                new ColumnShape("Symbol", typeof(string), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Broker", "Symbol" }, "Open", "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(BaseEntity) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(BaseEntity) }, Windows = { "1m" } };
        // Continuation is implicit in runtime; live only, EMIT CHANGES

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

    [Fact]
    public async Task Bars1mLive_WhenEmpty_Matches_ExpectedShape()
    {
        var ddls = await BuildDdlsAsync();
        var live = ddls.First(s => s.IndexOf("bar_1m_live", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.Contains("CREATE TABLE IF NOT EXISTS", live, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("bar_1m_live", live, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FROM bar_1s_rows", live, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WINDOW TUMBLING", live, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SIZE 1 MINUTES", live, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", live, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(ddls, s => s.Contains("bar_1m_final", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(ddls, s => s.Contains("bar_prev_1m", StringComparison.OrdinalIgnoreCase));
    }
}