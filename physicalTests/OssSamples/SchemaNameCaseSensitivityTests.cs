using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Configuration;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration;


[Collection("DataRoundTrip")]
public class SchemaNameCaseSensitivityTests
{

    [KsqlTopic("orders")]
    public class OrderCorrectCase
    {
        public int CustomerId { get; set; }
        public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
        public bool IsHighPriority { get; set; } = false;
        public int Count { get; set; }
    }

    public class OrderContext : KsqlContext
    {
        public EventSet<OrderCorrectCase> OrderCorrectCases { get; set; }
        public OrderContext() : base(new KsqlDslOptions()) { }
        public OrderContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            //    modelBuilder.Entity<OrderCorrectCase>();
        }
    }

    // Schema Registry 縺ｯ繝輔ぅ繝ｼ繝ｫ繝牙錐繧ょ性繧√せ繧ｭ繝ｼ繝槭→縺ｮ螳悟・荳閾ｴ繧定ｦ∵ｱゅ☆繧九・
    // 譌｢蟄倥せ繧ｭ繝ｼ繝槭→荳閾ｴ縺吶ｋ繝輔ぅ繝ｼ繝ｫ繝牙錐繝ｻ謨ｰ繧貞ｮ夂ｾｩ縺吶ｋ縺薙→縺ｧ逋ｻ骭ｲ繧ｨ繝ｩ繝ｼ繧帝亟縺舌・
    [Fact]
    [Trait("Category", "Integration")]
    public async Task LowercaseField_ShouldSucceed()
    {


        //try
        //{
        //    await Env.ResetAsync();
        //}
        //catch (Exception ex)
        //{
        //    Console.WriteLine($"[Warning] ResetAsync failed: {ex}");
        //    throw new SkipException($"Test setup failed in ResetAsync: {ex.Message}");
        //}

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvSchemaNameCaseSensitivityTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvSchemaNameCaseSensitivityTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvSchemaNameCaseSensitivityTests.KsqlDbUrl
        };

        // Map entity to dedicated topic to avoid SR conflicts
        options.Entities.Add(new Ksql.Linq.Configuration.EntityConfiguration { Entity = nameof(OrderCorrectCase), SourceTopic = "orders_casename" });
        await using var ctx = new OrderContext(options);
        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvSchemaNameCaseSensitivityTests.KafkaBootstrapServers }).Build())
        {
            try { await admin.DeleteTopicsAsync(new[] { "orders_casename" }); } catch { }
            try { await admin.CreateTopicsAsync(new[] { new Confluent.Kafka.Admin.TopicSpecification { Name = "orders_casename", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "orders_casename", 1, 1, TimeSpan.FromSeconds(10));
        }

        await ctx.OrderCorrectCases.AddAsync(new OrderCorrectCase
        {
            CustomerId = 1,
            Id = 1,
            Region = "east",
            Amount = 10d,
            IsHighPriority = false,
            Count = 1
        });


        var timeout = TimeSpan.FromSeconds(5);
        await ctx.WaitForEntityReadyAsync<OrderCorrectCase>(timeout);
    }
}

// local environment helpers
public class EnvSchemaNameCaseSensitivityTests
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
