using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Microsoft.Extensions.Logging;
using Ksql.Linq.Query.Dsl;
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
public class GoldenRowsStreamSqlTests
{
    [KsqlTopic("bar")]
    private class BaseEntity
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private static async Task<string> BuildRowsStreamDdlAsync()
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
        var model = new KsqlQueryModel { SourceTypes = new[] { typeof(BaseEntity) } };
        model.Windows.Add("1m");

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
        return ddls.First(s => s.StartsWith("CREATE STREAM IF NOT EXISTS bar_1s_rows", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Rows1sStream_Equals_Golden()
    {
        var sql = await BuildRowsStreamDdlAsync();
        GoldenSqlHelpers.AssertEqualsOrUpdate(
            goldenPath: "tests/Query/Golden/rows_1s_stream.sql",
            actualSql: sql);
    }
}