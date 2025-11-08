using System;
using System.Collections.Concurrent;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Mapping;
using Ksql.Linq.Messaging.Consumers;
using Ksql.Linq.Messaging.Producers;
// runtime heartbeat types removed
using Ksql.Linq.Tests.Utils;
using Microsoft.Extensions.Options;
using Xunit;

namespace Ksql.Linq.Tests.Messaging;

public class ConsumerConfigLoggingTests
{
    private static (KafkaConsumerManager mgr, TestLoggerFactory lf) Build(bool enableAutoCommit)
    {
        var opts = new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "localhost:9092", ClientId = "ut" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://localhost:8081" }
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
        var mapping = new MappingRegistry();
        var models = new ConcurrentDictionary<Type, EntityModel>();
        var model = new EntityModel { EntityType = typeof(object), TopicName = "test-topic" };
        models[typeof(object)] = model;
        var dlqProd = new DlqProducer(new Ksql.Linq.Messaging.Producers.KafkaProducerManager(mapping, Options.Create(opts)), "dlq");
        var lf = new TestLoggerFactory();
        var mgr = new KafkaConsumerManager(mapping, Options.Create(opts), models, dlqProd, new ManualCommitManager(), lf);
        return (mgr, lf);
    }

    [Fact]
    public void BuildConsumerConfig_LogsInformation()
    {
        var (mgr, lf) = Build(enableAutoCommit: true);
        // invoke private BuildConsumerConfig via reflection
        var cfg = PrivateAccessor.InvokePrivate<Confluent.Kafka.ConsumerConfig>(
            mgr,
            name: "BuildConsumerConfig",
            parameterTypes: new[] { typeof(string), typeof(Ksql.Linq.Configuration.Abstractions.KafkaSubscriptionOptions), typeof(string), typeof(bool) },
            args: new object?[] { "test-topic", new Ksql.Linq.Configuration.Abstractions.KafkaSubscriptionOptions(), null, true }
        );
        Assert.NotNull(cfg);
        // assert that Information log for consumer:test-topic config exists
        var found = false;
        foreach (var kv in lf.Loggers)
        {
            foreach (var e in kv.Value.Entries)
            {
                if (e.Level == Microsoft.Extensions.Logging.LogLevel.Information && e.Message.Contains("consumer:test-topic config:"))
                {
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
        Assert.True(found, "Expected Information log for consumer:test-topic config");
    }
}
