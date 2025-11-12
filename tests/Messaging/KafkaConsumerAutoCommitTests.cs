using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Core.Configuration;
using Ksql.Linq.Mapping;
using Ksql.Linq.Messaging.Consumers;
using Ksql.Linq.Messaging.Producers;
// runtime heartbeat types removed
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Ksql.Linq.Tests.Messaging;

public class KafkaConsumerAutoCommitTests
{
    private static EntityModel CreateModel()
    {
        var b = new ModelBuilder();
        b.Entity<TestEntity>();
        return b.GetEntityModel<TestEntity>()!;
    }

    private static (KafkaConsumerManager mgr, KsqlDslOptions opts, ConcurrentDictionary<Type, EntityModel> models, MappingRegistry map, Mock<ICommitManager> cmMock) BuildManager(bool enableAutoCommit)
    {
        var opts = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "localhost:9092", ClientId = "ut" },
            SchemaRegistry = new SchemaRegistrySection { Url = "http://localhost:8081" }
        };
        opts.Topics["test-topic"] = new Ksql.Linq.Configuration.Messaging.TopicSection
        {
            Consumer = new Ksql.Linq.Configuration.Messaging.ConsumerSection
            {
                GroupId = "ut-group",
                EnableAutoCommit = enableAutoCommit,
                AutoOffsetReset = "Earliest"
            }
        };
        var model = CreateModel();
        var models = new ConcurrentDictionary<Type, EntityModel>();
        models[typeof(TestEntity)] = model;
        var mapping = new MappingRegistry();
        mapping.RegisterEntityModel(model, genericValue: true);

        var dlqProd = new DlqProducer(new Ksql.Linq.Messaging.Producers.KafkaProducerManager(mapping, Options.Create(opts), NullLoggerFactory.Instance), "dlq");
        var cmMock = new Mock<ICommitManager>(MockBehavior.Strict);
        var mgr = new KafkaConsumerManager(mapping, Options.Create(opts), models, dlqProd, cmMock.Object, NullLoggerFactory.Instance);
        return (mgr, opts, models, mapping, cmMock);
    }

    [Fact]
    public async Task ConsumeAsync_DoesNotBind_WhenEnableAutoCommitTrue_FromTopic()
    {
        var (mgr, _, _, _, mock) = BuildManager(enableAutoCommit: true);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));
        try
        {
            await foreach (var _ in mgr.ConsumeAsync<TestEntity>(autoCommit: true, cancellationToken: cts.Token))
            {
                break;
            }
        }
        catch { /* ignore */ }

        mock.Verify(x => x.Bind(typeof(TestEntity), It.IsAny<string>(), It.IsAny<object>()), Times.Never);
    }

    [Fact]
    public void BuildConsumerConfig_Respects_EnableAutoCommit_FromTopic()
    {
        var (mgr, _, _, _, _) = BuildManager(enableAutoCommit: true);
        var cfg = PrivateAccessor.InvokePrivate<Confluent.Kafka.ConsumerConfig>(
            mgr,
            name: "BuildConsumerConfig",
            parameterTypes: new[] { typeof(string), typeof(Ksql.Linq.Configuration.Abstractions.KafkaSubscriptionOptions), typeof(string), typeof(bool) },
            args: new object?[] { "test-topic", new Ksql.Linq.Configuration.Abstractions.KafkaSubscriptionOptions(), null, true }
        );
        Assert.True(cfg.EnableAutoCommit);
        Assert.Equal("ut-group", cfg.GroupId);
    }
}
