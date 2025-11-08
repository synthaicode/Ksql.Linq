using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Configuration;
using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration;


[Collection("DDL")]
public class KsqlSyntaxTests
{
    public KsqlSyntaxTests()
    {
        EnvKsqlSyntaxTests.ResetAsync().GetAwaiter().GetResult();
        EnvKsqlSyntaxTests.SetupAsync().GetAwaiter().GetResult();

        using var ctx = EnvKsqlSyntaxTests.CreateContext();
        var r1 = PhysicalTestEnv.KsqlHelpers.ExecuteStatementWithRetryAsync(ctx,
            "CREATE STREAM IF NOT EXISTS source (id INT) WITH (KAFKA_TOPIC='source', VALUE_FORMAT='AVRO', PARTITIONS=1);").Result;
        Console.WriteLine($"CREATE STREAM result: {r1.IsSuccess}, msg: {r1.Message}");

        foreach (var ddl in TestSchema.GenerateTableDdls())
        {
            var r = PhysicalTestEnv.KsqlHelpers.ExecuteStatementWithRetryAsync(ctx, ddl).Result;
            Console.WriteLine($"DDL result: {r.IsSuccess}, msg: {r.Message}");
        }
    }

    // 逕滓・縺輔ｌ縺溘け繧ｨ繝ｪ縺渓sqlDB縺ｧ隗｣驥亥庄閭ｽ縺狗｢ｺ隱・
    [Theory]
    [Trait("Category", "Integration")]
    [InlineData("CREATE STREAM test_stream AS SELECT * FROM source EMIT CHANGES;")]
    [InlineData("SELECT CustomerId, COUNT(*) FROM orders GROUP BY CustomerId EMIT CHANGES;")]
    // Translation functions (smoke)
    // String
    [InlineData("SELECT SUBSTRING('broker',1,1) FROM source EMIT CHANGES;")]
    [InlineData("SELECT REPLACE('a-b-a','-','_') FROM source EMIT CHANGES;")]
    [InlineData("SELECT CONCAT('a','b') FROM source EMIT CHANGES;")]
    [InlineData("SELECT INSTR('abc','b') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TRIM(' x ') FROM source EMIT CHANGES;")]
    [InlineData("SELECT SPLIT('a,b',',') FROM source EMIT CHANGES;")]
    [InlineData("SELECT LPAD('x',3,'_') FROM source EMIT CHANGES;")]
    [InlineData("SELECT RPAD('x',3,'_') FROM source EMIT CHANGES;")]
    // Math
    [InlineData("SELECT ROUND(1.24, 1) FROM source EMIT CHANGES;")]
    [InlineData("SELECT FLOOR(1.24) FROM source EMIT CHANGES;")]
    [InlineData("SELECT CEIL(1.01) FROM source EMIT CHANGES;")]
    [InlineData("SELECT ABS(-2.0) FROM source EMIT CHANGES;")]
    [InlineData("SELECT LOG(2.0) FROM source EMIT CHANGES;")]
    [InlineData("SELECT LOG(100.0) FROM source EMIT CHANGES;")]
    [InlineData("SELECT EXP(1.0) FROM source EMIT CHANGES;")]
    [InlineData("SELECT POWER(2.0,3.0) FROM source EMIT CHANGES;")]
    [InlineData("SELECT SQRT(4.0) FROM source EMIT CHANGES;")]
    // Date (use ROWTIME meta)
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'MM', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'dd', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'HH', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'mm', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'ss', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'e', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'D', 'UTC') FROM source EMIT CHANGES;")]
    [InlineData("SELECT TIMESTAMPTOSTRING(ROWTIME, 'w', 'UTC') FROM source EMIT CHANGES;")]
    public async Task GeneratedQuery_IsValidInKsqlDb(string ksql)
    {


        var response = await ExecuteExplainDirectAsync(ksql);
        Assert.True(response.IsSuccess, $"{ksql} failed: {response.Message}");
    }

    private static async Task<KsqlDbResponse> ExecuteExplainDirectAsync(string ksql)
    {
        using var client = new HttpClient { BaseAddress = new Uri(EnvKsqlSyntaxTests.KsqlDbUrl) };
        var payload = new { ksql = $"EXPLAIN {ksql}", streamsProperties = new { } };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/ksql", content);
        var body = await response.Content.ReadAsStringAsync();
        var success = response.IsSuccessStatusCode && !body.Contains("\"error_code\"");
        return new KsqlDbResponse(success, body);
    }

}

// local environment helpers
public class EnvKsqlSyntaxTests
{
    internal const string SchemaRegistryUrl = "http://127.0.0.1:18081";
    internal const string KsqlDbUrl = "http://127.0.0.1:18088";
    internal const string KafkaBootstrapServers = "127.0.0.1:39092";
    internal const string SkipReason = "Skipped in CI due to missing ksqlDB instance or schema setup failure";

    internal static bool IsKsqlDbAvailable()
    {
        try
        {
            using var ctx = CreateContext();
            var r = ctx.ExecuteStatementAsync("SHOW TOPICS;").GetAwaiter().GetResult();
            return r.IsSuccess;
        }
        catch
        {
            return false;
        }
    }

    internal static KsqlContext CreateContext()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = SchemaRegistryUrl },
            KsqlDbUrl = KsqlDbUrl
        };
        return new BasicContext(options);
    }

    internal static Task ResetAsync() => Task.CompletedTask;
    internal static Task SetupAsync() => Task.CompletedTask;

    private class BasicContext : KsqlContext
    {
        public BasicContext(KsqlDslOptions options) : base(options) { }
        protected override bool SkipSchemaRegistration => true;
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) => throw new NotImplementedException();
        protected override void OnModelCreating(IModelBuilder modelBuilder) { }
    }
}