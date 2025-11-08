using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class DerivedTumblingBarsPipelineOrderTests
{
    [KsqlTopic("bar")]
    private class TestSource { public int Id { get; set; } }

    private static async Task<List<string>> RunAsync(params string[] windows)
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = windows.Select(w => new Timeframe(int.Parse(w.TrimEnd('m')), "m")).ToArray(),
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
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(TestSource) } };
        foreach (var w in windows) model.Windows.Add(w);

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
        return ddls.ToList();
    }

    [Fact]
    public async Task Downstream_Ddls_Use_Rows_As_Input_And_Names_Are_Stable()
    {
        var ddls = await RunAsync("1m", "5m");
        var ddl1m = ddls.FirstOrDefault(s => s.IndexOf("bar_1m_live", StringComparison.OrdinalIgnoreCase) >= 0);
        var ddl5m = ddls.FirstOrDefault(s => s.IndexOf("bar_5m_live", StringComparison.OrdinalIgnoreCase) >= 0);
        Assert.False(string.IsNullOrEmpty(ddl1m));
        Assert.False(string.IsNullOrEmpty(ddl5m));
        Assert.Contains("FROM bar_1s_rows", ddl1m!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FROM bar_1s_rows", ddl5m!, StringComparison.OrdinalIgnoreCase);
    }
}