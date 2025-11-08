using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

[KsqlTopic("bar")]
class TestSource
{
    public int Id { get; set; }
}

[Trait("Level", TestLevel.L3)]
public class DerivedTumblingPipelineTests
{
    private static (TumblingQao qao, EntityModel baseModel, KsqlQueryModel queryModel) CreateBasicModel(params string[] windows)
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = windows.Select(w => new Timeframe(int.Parse(w.TrimEnd('m')), "m")).ToArray(),
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(TestSource) }, Windows = { } };
        foreach (var w in windows)
            model.Windows.Add(w);
        return (qao, baseModel, model);
    }

    private static async Task<List<string>> RunAsync(TumblingQao qao, EntityModel baseModel, KsqlQueryModel model)
    {
        var ddls = new ConcurrentBag<string>();
        Task<KsqlDbResponse> Exec(EntityModel _, string sql)
        {
            ddls.Add(sql);
            return Task.FromResult(new KsqlDbResponse(true, string.Empty));
        }
        var registry = new ConcurrentDictionary<Type, EntityModel>();
        var mapping = new MappingRegistry();
        var asm = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("dyn"), AssemblyBuilderAccess.Run);
        var mod = asm.DefineDynamicModule("m");
        Type Resolver(string _) => mod.DefineType("T" + Guid.NewGuid().ToString("N")).CreateType()!;
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));
        return ddls.ToList();
    }

    [Fact]
    public async Task Final1sStream_BindsToFinalTopicWithoutCtas()
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
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(TestSource) }, Windows = { "1m" } };

        var ddls = await RunAsync(qao, baseModel, model);
        var streamDdl = ddls.Single(s => s.StartsWith("CREATE STREAM IF NOT EXISTS bar_1s_rows", StringComparison.OrdinalIgnoreCase));

        Assert.DoesNotContain(ddls, s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1s_final", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain("AS SELECT", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("WINDOW TUMBLING", streamDdl, StringComparison.OrdinalIgnoreCase);

        Assert.StartsWith("CREATE STREAM IF NOT EXISTS BAR_1S_ROWS (", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("KAFKA_TOPIC='BAR_1S_ROWS'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("KEY_FORMAT='AVRO'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUE_FORMAT='AVRO'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TIMESTAMP='TIMESTAMP'", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RETENTION_MS=604800000", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("AS SELECT", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("EMIT", streamDdl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("PARTITION BY", streamDdl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FinalPipeline_EmitsExpectedEntitiesWithoutMutatingModel()
    {
        var (qao, baseModel, model) = CreateBasicModel("1m");
        var originalWindows = model.Windows.ToArray();

        var ddls = await RunAsync(qao, baseModel, model);

        Assert.DoesNotContain(ddls, s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1s_final", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(ddls, s => s.StartsWith("CREATE STREAM IF NOT EXISTS bar_1s_rows", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(ddls, s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(ddls, s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_final", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(originalWindows, model.Windows.ToArray());
    }

    [Fact]
    public async Task Final_5m_uses_hub_stream_as_input()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m"), new Timeframe(5, "m") },
            Keys = new[] { "Id", "BucketStart" },
            Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
            PocoShape = new[]
            {
                new ColumnShape("Id", typeof(int), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(TestSource) }, Windows = { "1m", "5m" } };

        var ddls = await RunAsync(qao, baseModel, model);
        var ddl5 = ddls.Single(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_5m_live", StringComparison.OrdinalIgnoreCase));

        Assert.Contains("FROM bar_1s_rows", ddl5, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", ddl5, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Final_projection_reaggregates_columns()
    {
        var qao = new TumblingQao
        {
            TimeKey = "Timestamp",
            Windows = new[] { new Timeframe(1, "m") },
            Keys = new[] { "Broker", "Symbol", "BucketStart" },
            Projection = new[] { "Broker", "Symbol", "BucketStart", "Open", "High", "Low", "KsqlTimeFrameClose", "Volume" },
            PocoShape = new[]
            {
                new ColumnShape("Broker", typeof(string), false),
                new ColumnShape("Symbol", typeof(string), false),
                new ColumnShape("BucketStart", typeof(long), false),
                new ColumnShape("Open", typeof(double), false),
                new ColumnShape("High", typeof(double), false),
                new ColumnShape("Low", typeof(double), false),
                new ColumnShape("KsqlTimeFrameClose", typeof(double), false),
                new ColumnShape("Volume", typeof(double), false)
            },
            BasedOn = new BasedOnSpec(new[] { "Broker", "Symbol" }, "Open", "KsqlTimeFrameClose", string.Empty),
            WeekAnchor = DayOfWeek.Monday
        };
        var baseModel = new EntityModel { EntityType = typeof(TestSource) };
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(TestSource) }, Windows = { "1m" } };
        Expression<Func<IGrouping<object, TestSource>, object>> sel = g => new
        {
            Open = g.EarliestByOffset(x => 0.0),
            High = g.Max(x => 0.0),
            Low = g.Min(x => 0.0),
            KsqlTimeFrameClose = g.LatestByOffset(x => 0.0),
            Volume = g.Sum(x => 0.0)
        };
        model.SelectProjection = sel;

        var ddls = await RunAsync(qao, baseModel, model);
        var ddl = ddls.Single(s => s.StartsWith("CREATE TABLE IF NOT EXISTS bar_1m_live", StringComparison.OrdinalIgnoreCase));

        Assert.Contains("FROM bar_1s_rows", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EMIT CHANGES", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("BID", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Final_ddl_has_no_window_clause_or_agg_final_reference()
    {
        var (qao, baseModel, model) = CreateBasicModel("1m", "5m");

        var ddls = await RunAsync(qao, baseModel, model);
        foreach (var sql in ddls.Where(s => s.Contains("_live", StringComparison.OrdinalIgnoreCase)))
        {
            Assert.DoesNotContain("_agg_final", sql, StringComparison.OrdinalIgnoreCase);
        }
    }
}