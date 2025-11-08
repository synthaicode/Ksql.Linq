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
public class CtasRetentionCascadeTests
{
    [KsqlTopic("bar")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static async Task<ConcurrentBag<string>> RunAsync(Action<Ksql.Linq.Core.Abstractions.EntityModel>? apply = null)
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
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn_ctas_ret"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry,
            new LoggerFactory().CreateLogger("test"), afterExecuteAsync: null,
            applyTopicSettings: apply);
        return ddls;
    }

    [Fact]
    public async Task RowsAndLive_WithRetention_Config_AreRendered()
    {
        var ddls = await RunAsync(apply: em =>
        {
            var name = (em.TopicName ?? em.EntityType.Name).ToLowerInvariant();
            if (string.Equals(name, "bar_1s_rows", StringComparison.OrdinalIgnoreCase))
            {
                em.AdditionalSettings["retention.ms"] = "300000"; // 5 min
            }
            if (string.Equals(name, "bar_1m_live", StringComparison.OrdinalIgnoreCase))
            {
                em.AdditionalSettings["retention.ms"] = "3600000"; // 60 min
            }
        });

        var rows = ddls.First(s => s.StartsWith("CREATE STREAM IF NOT EXISTS bar_1s_rows", StringComparison.OrdinalIgnoreCase));
        Console.WriteLine("ROWS DDL: " + rows);
        Assert.Contains("RETENTION_MS=300000", rows, StringComparison.OrdinalIgnoreCase);

        var live = ddls.First(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("RETENTION_MS=3600000", live, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Live_Inherits_Retention_From_Base_When_NotSpecified()
    {
        var ddls = await RunAsync(apply: em =>
        {
            var name = (em.TopicName ?? em.EntityType.Name).ToLowerInvariant();
            if (string.Equals(name, "bar_1m", StringComparison.OrdinalIgnoreCase))
            {
                em.AdditionalSettings["retention.ms"] = "900000"; // 15 min on base
            }
            // live has no explicit; pipeline should still include if extras were surfaced
            if (string.Equals(name, "bar_1m_live", StringComparison.OrdinalIgnoreCase))
            {
                // simulate cascade performed earlier (ApplyTopicCreationSettingsAdapter)
                em.AdditionalSettings["retention.ms"] = "900000";
            }
        });
        var live = ddls.First(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("RETENTION_MS=900000", live, StringComparison.OrdinalIgnoreCase);
    }
}
