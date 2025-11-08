using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using PhysicalTestEnv;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Configuration;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

[Collection("DDL")]
public class JoinIntegrationTests
{
    [KsqlTopic("orders_join")]
    public class OrderValue
    {
        [Ksql.Linq.Core.Attributes.KsqlKey]
        public int CustomerId { get; set; }
        public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
    }

    [KsqlTopic("customers_join")]
    public class Customer
    {
        [Ksql.Linq.Core.Attributes.KsqlKey]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [KsqlTopic("orders_customers_join")]
    public class OrderCustomerJoined
    {
        [Ksql.Linq.Core.Attributes.KsqlKey]
        public int CustomerId { get; set; }
        public string Name { get; set; } = string.Empty;
        public double Amount { get; set; }
    }


    public class JoinContext : KsqlContext
    {
        // EventSet 繝励Ο繝代ユ繧｣縺ｧ閾ｪ蜍慕匳骭ｲ縺輔○繧・
        public EventSet<Customer> Customers { get; set; }
        public EventSet<OrderValue> OrderValues { get; set; }

        public JoinContext() : base(new KsqlDslOptions()) { }
        public JoinContext(KsqlDslOptions options) : base(options) { }
        public JoinContext(KsqlDslOptions options, ILoggerFactory loggerFactory) : base(options, loggerFactory) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            // 繧ｽ繝ｼ繧ｹ
            modelBuilder.Entity<OrderValue>();
            modelBuilder.Entity<Customer>();

            // JOIN螳夂ｾｩ繧・QueryModel 縺ｨ縺励※逋ｻ骭ｲ
            var qm = new KsqlQueryRoot()
                .From<OrderValue>()
                .Join<Customer>((o, c) => o.CustomerId == c.Id)
                .Within(TimeSpan.FromSeconds(300))
                .Select((o, c) => new { o.CustomerId, c.Name, o.Amount })
                .Build();

            var builder = modelBuilder.Entity<OrderCustomerJoined>();
            if (builder is Ksql.Linq.Core.Modeling.EntityModelBuilder<OrderCustomerJoined> eb)
            {
                eb.GetModel().QueryModel = qm;
            }
        }
        protected override bool SkipSchemaRegistration => false;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TwoTableJoin_Query_ShouldBeValid()
    {
        await PhysicalTestEnv.Cleanup.DeleteTopicsAsync(EnvJoinIntegrationTests.KafkaBootstrapServers, new[]
        {
            "orders_join",
            "customers_join",
            "orders_customers_join"
        });
        await PhysicalTestEnv.Cleanup.DeleteSubjectsAsync(EnvJoinIntegrationTests.SchemaRegistryUrl, new[] { "orders_join", "customers_join", "orders_customers_join" });

        try
        {
            await EnvJoinIntegrationTests.ResetAsync();
        }
        catch (Exception)
        {
        }

        // ksqlDB 縺悟・襍ｷ蜍慕峩蠕後〒繧ょｮ牙ｮ壹☆繧九∪縺ｧ蠕・ｩ滂ｼ・info逶ｸ蠖・+ 迪ｶ莠茨ｼ・
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync(EnvJoinIntegrationTests.KsqlDbUrl, TimeSpan.FromSeconds(180), graceMs: 2000);

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvJoinIntegrationTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvJoinIntegrationTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvJoinIntegrationTests.KsqlDbUrl
        };

        using var lf = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.SetMinimumLevel(LogLevel.Information);
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        var diagLogger = lf.CreateLogger("JoinDiagnostics");

        await using var ctx = new JoinContext(options, lf);
        await Diagnostics.Ksql.LogStreamSnapshotAsync(
            EnvJoinIntegrationTests.KsqlDbUrl,
            EnvJoinIntegrationTests.SchemaRegistryUrl,
            EnvJoinIntegrationTests.KafkaBootstrapServers,
            "ORDERS_CUSTOMERS_JOIN",
            diagLogger);

        // JOIN 縺ｮ險育判縺檎函謌舌〒縺阪ｋ縺薙→繧堤｢ｺ隱搾ｼ医た繝ｼ繧ｹ縺ｨJOIN縺ｯ OnModelCreating 縺ｧ險ｭ螳壽ｸ医∩・・
        var ksql = "SELECT CustomerId, Name, Amount FROM ORDERS_JOIN JOIN CUSTOMERS_JOIN ON (CustomerId = Id);";
        var response = await ctx.ExecuteExplainAsync(ksql);
        Assert.True(response.IsSuccess, $"{ksql} failed: {response.Message}");
    }

}

// local environment helpers
public class EnvJoinIntegrationTests
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










