using Microsoft.Extensions.Logging;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using PhysicalTestEnv;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Configuration;
using Ksql.Linq.Core.Dlq;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Ksql.Linq.Tests.Integration;

[Collection("DataRoundTrip")]
public class DlqIntegrationTests
{
    [KsqlTopic("orders_dlq_int")] // use a unique topic to avoid Schema Registry subject conflicts
    public class Order
    {
        public int Id { get; set; }
        [KsqlDecimal(18, 2)]
        public decimal Amount { get; set; }
    }

    public class OrderContext : KsqlContext
    {
        public EventSet<Order> Orders { get; set; } = default!;
        public OrderContext() : base(new KsqlDslOptions()) { }
        public OrderContext(KsqlDslOptions options) : base(options) { }
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            //  modelBuilder.Entity<Order>();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ForEachAsync_OnErrorDlq_WritesToDlq()
    {
        await PhysicalTestEnv.Cleanup.DeleteTopicsAsync(EnvDlqIntegrationTests.KafkaBootstrapServers, new[] { "orders_dlq_int", "dead_letter_queue" });
        await PhysicalTestEnv.Cleanup.DeleteSubjectsAsync(EnvDlqIntegrationTests.SchemaRegistryUrl, new[] { "orders_dlq_int", "dead_letter_queue" });

        var ordersGroupId = Guid.NewGuid().ToString();
        var dlqGroupId = Guid.NewGuid().ToString();

        var options = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = EnvDlqIntegrationTests.KafkaBootstrapServers },
            SchemaRegistry = new SchemaRegistrySection { Url = EnvDlqIntegrationTests.SchemaRegistryUrl },
            KsqlDbUrl = EnvDlqIntegrationTests.KsqlDbUrl
        };
        options.Topics.Add("orders_dlq_int", new Configuration.Messaging.TopicSection
        {
            Consumer = new Configuration.Messaging.ConsumerSection
            {
                AutoOffsetReset = "Earliest",
                GroupId = ordersGroupId
            }
        });
        options.Topics.Add("dead_letter_queue", new Configuration.Messaging.TopicSection
        {
            Consumer = new Configuration.Messaging.ConsumerSection
            {
                AutoOffsetReset = "Earliest",
                GroupId = dlqGroupId
            }
        });

        using var diagFactory = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.SetMinimumLevel(LogLevel.Information);
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        var diagLogger = diagFactory.CreateLogger("DlqDiagnostics");

        await using var ctx = new OrderContext(options);

        var baseline = await Diagnostics.Ksql.LogStreamSnapshotAsync(
            EnvDlqIntegrationTests.KsqlDbUrl,
            EnvDlqIntegrationTests.SchemaRegistryUrl,
            EnvDlqIntegrationTests.KafkaBootstrapServers,
            "DEAD_LETTER_QUEUE",
            diagLogger,
            dlqGroupId,
            new[] { "dead_letter_queue" });

        using (var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = EnvDlqIntegrationTests.KafkaBootstrapServers }).Build())
        {
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "orders_dlq_int", 1, 1, TimeSpan.FromSeconds(10));
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "dead_letter_queue", 1, 1, TimeSpan.FromSeconds(10));
        }

        // スキーマ確定用ダミーデータ送信
        await ctx.Orders.AddAsync(new Order { Id = 1, Amount = 0.01m });
        await Task.Delay(5000);
        // DLQ送信テスト本体
        await ctx.Orders
            .OnError(ErrorAction.DLQ)
            .ForEachAsync(_ => throw new Exception("Simulated failure"), TimeSpan.FromSeconds(5));
        await Task.Delay(3000);
        // DLQ検証: 新APIで読み取り
        DlqRecord? found = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await foreach (var record in ctx.Dlq.ReadAsync(new DlqReadOptions { FromBeginning = true }, cts.Token))
        {
            // raw payload for diagnosis (sample snippet parity)
            Console.WriteLine(record.RawText);
            if (record.ErrorMessage == "Simulated failure")
            {
                found = record;
                break;
            }
        }

        var diagnostics = await Diagnostics.Ksql.LogStreamSnapshotAsync(
            EnvDlqIntegrationTests.KsqlDbUrl,
            EnvDlqIntegrationTests.SchemaRegistryUrl,
            EnvDlqIntegrationTests.KafkaBootstrapServers,
            "DEAD_LETTER_QUEUE",
            diagLogger,
            dlqGroupId,
            new[] { "dead_letter_queue" });

        Assert.NotNull(diagnostics);
        Assert.NotNull(found);
        Assert.Equal("Simulated failure", found!.ErrorMessage);
        Assert.Equal("Exception", found.ErrorType);
        Assert.False(string.IsNullOrWhiteSpace(found.RawText));

        Assert.True(diagnostics!.Kafka.ProducedCount >= 1, "G1: ProducedCount >= 1 expected");
        Assert.True(diagnostics.Kafka.SubscribedTopics.Length >= 1, "G1: SubscribedTopics should not be empty");
        Assert.True(diagnostics.Kafka.AssignedPartitions.Length > 0, "G2: AssignedPartitions > 0 expected");
        Assert.Equal("Stable", diagnostics.Kafka.GroupState, ignoreCase: true);

        var consumerLag = diagnostics.Kafka.ConsumerLag ?? 0;
        var processedOrCommitted = diagnostics.Snapshot.Processed > 0 || diagnostics.Snapshot.Committed > 0;
        Assert.True(processedOrCommitted || consumerLag > 0, "G3: progress metrics must indicate activity");

        if (baseline?.Kafka.ConsumerLag is long beforeLag && beforeLag > 0 && diagnostics.Kafka.ConsumerLag is long afterLag)
        {
            Assert.True(afterLag <= beforeLag, "G3: ConsumerLag should not increase after processing");
        }

        Assert.Equal(0, diagnostics.Snapshot.SkippedByDeserializer);
        Assert.True(!string.IsNullOrWhiteSpace(diagnostics.Schema?.KeyDigest) && diagnostics.Schema!.KeyDigest != "n/a", "G5: Key schema digest must be available");
        Assert.True(!string.IsNullOrWhiteSpace(diagnostics.Schema.ValueDigest) && diagnostics.Schema.ValueDigest != "n/a", "G5: Value schema digest must be available");
    }
}

public class EnvDlqIntegrationTests
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


