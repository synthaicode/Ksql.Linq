using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

[KsqlTopic("bar")]
class HubAggSource { public int Id { get; set; } }

public class DerivedTumblingPipelineHubAggregationTests
{
    private static async Task<(ConcurrentBag<string> ddls, MappingRegistry map)> RunAsync(TumblingQao qao)
    {
        var baseModel = new EntityModel { EntityType = typeof(HubAggSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(HubAggSource) }, Windows = { "1m", "5m" } };
        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql) { ddls.Add(sql); return Task.FromResult(new KsqlDbResponse(true, string.Empty)); }
        var mapping = new MappingRegistry();
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new System.Reflection.AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));
        return (ddls, mapping);
    }

    [Fact]
    public async Task Hub_input_Reaggregates_From_Ohlc_Columns()
    {
        // seconds hub will produce OPEN/HIGH/LOW/KSQLTIMEFRAMECLOSE
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m"), new Timeframe(5, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "Open", "High", "Low", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };

        var (ddls, _) = await RunAsync(qao);
        var ddl1m = ddls.First(s => s.Contains("bar_1m_live"));
        Assert.Contains("FROM bar_1s_rows", ddl1m, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", ddl1m, StringComparison.OrdinalIgnoreCase);
        // 繝ｦ繝ｼ繧ｶ繝ｼ謚募ｽｱ萓晏ｭ倥・縺溘ａ縲∝・菴鍋噪縺ｪ蛻・髮・ｴ・↓縺ｯ萓晏ｭ倥＠縺ｪ縺・・
        // 繝上ヶSTREAM繧貞・蜉帙→縺励※縺・ｋ縺薙→縺ｮ縺ｿ繧呈､懆ｨｼ縺吶ｋ縲・
    }

    [Fact]
    public async Task Final_Ddl_Includes_Grace_Period()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var (ddls, _) = await RunAsync(qao);
        var ddl1m = ddls.First(s => s.Contains("bar_1m_live"));
        // Grace 縺ｯ 1s 繝上ヶ蜑肴署縺ｧ蛟､繧偵◎縺ｮ縺ｾ縺ｾ謗｡逕ｨ縺吶ｋ・郁・蜍募刈邂励＠縺ｪ縺・ｼ峨・
        // 譌｢螳壹・ 1 遘偵→縺吶ｋ縲・
        Assert.Contains("GRACE PERIOD 1 SECONDS", ddl1m, StringComparison.OrdinalIgnoreCase);
    }
}

