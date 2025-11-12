using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;
[KsqlTopic("test-topic")]
class ConcurrencySource
{
    public int Id { get; set; }
}

[Trait("Level", TestLevel.L3)]
public class DerivedTumblingPipelineConcurrencyTests
{
    [Fact]
    public async Task RunAsync_registers_all_models_without_conflict()
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
        var baseModel = new EntityModel { EntityType = typeof(ConcurrencySource) };
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(ConcurrencySource) },
            Windows = { "1m", "5m" }
        };
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

        // Continuation is handled at read/distribution layers; no WhenEmpty configuration required
        _ = await DerivedTumblingPipeline.RunAsync(qao, baseModel, model, Exec, Resolver, mapping, registry, new LoggerFactory().CreateLogger("test"));

        // WhenEmpty ポリシー更新:
        // - hub(_1s_rows) は作らない
        // - hb は作らない
        // - prev と fill は作らない
        // - rows_last は KsqlContext.EnsureRowsLastTableForAsync で個別作成（RunAsync の計上対象外）
        // 件数: 1s(hub)=1 + 1m(live)=1 + 5m(live)=1 → 合計 3
        var expected = 3;
        Assert.Equal(expected, registry.Count);
        Assert.Equal(expected, ddls.Count);
        var finals = ddls
            .Select(sql => new { sql, match = Regex.Match(sql, @"^CREATE TABLE\s+([^\s]+)", RegexOptions.IgnoreCase) })
            .Where(x => x.match.Success && x.match.Groups[1].Value.EndsWith("_final", StringComparison.OrdinalIgnoreCase))
            .Select(x => x.sql)
            .ToList();
        // final TABLE は作らない（0 本）
        Assert.Empty(finals);
        // live DDL は EMIT CHANGES であることを確認
        var lives = ddls.Where(s => s.IndexOf("_live", StringComparison.OrdinalIgnoreCase) >= 0).ToList();
        Assert.Equal(2, lives.Count);
        Assert.All(lives, d => Assert.Contains("EMIT CHANGES", d, StringComparison.OrdinalIgnoreCase));

    }
}

