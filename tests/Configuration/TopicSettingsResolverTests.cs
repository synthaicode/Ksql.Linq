using Ksql.Linq.Configuration;
using Ksql.Linq.Configuration.Messaging;
using Ksql.Linq.Core.Abstractions;
using Xunit;

namespace Ksql.Linq.Tests.Configuration;

[Trait("Level", "L2")]
public class TopicSettingsResolverTests
{
    private static KsqlDslOptions NewOptions()
    {
        return new KsqlDslOptions();
    }

    private static EntityModel NewModel(string topic)
    {
        return new EntityModel { EntityType = typeof(TopicSettingsResolverTests), TopicName = topic };
    }

    [Fact]
    public void Apply_ExactMatch_AppliesAllSettings()
    {
        var opts = NewOptions();
        opts.Topics["bar_1m_live"] = new TopicSection
        {
            Creation = new TopicCreationSection
            {
                NumPartitions = 2,
                ReplicationFactor = 1,
                Configs = new System.Collections.Generic.Dictionary<string, string>
                {
                    ["retention.ms"] = "60000"
                }
            }
        };

        var model = NewModel("bar_1m_live");
        TopicSettingsResolver.Apply(opts, model);

        Assert.Equal(2, model.Partitions);
        Assert.Equal(1, model.ReplicationFactor);
        Assert.True(model.AdditionalSettings.TryGetValue("retention.ms", out var v) && (v as string) == "60000");
    }

    [Fact]
    public void Apply_Cascade_FromBaseToLive()
    {
        var opts = NewOptions();
        opts.Topics["bar_1m"] = new TopicSection
        {
            Creation = new TopicCreationSection
            {
                NumPartitions = 3,
                ReplicationFactor = 1,
                Configs = new System.Collections.Generic.Dictionary<string, string>
                {
                    ["retention.ms"] = "900000"
                }
            }
        };

        var model = NewModel("bar_1m_live");
        TopicSettingsResolver.Apply(opts, model);

        Assert.Equal(3, model.Partitions);
        Assert.Equal(1, model.ReplicationFactor);
        Assert.True(model.AdditionalSettings.TryGetValue("retention.ms", out var v) && (v as string) == "900000");
    }

    [Fact]
    public void Apply_NoConfig_KeepsDefaults_NoRetention()
    {
        var opts = NewOptions();
        var model = NewModel("unknown_topic");
        model.Partitions = 1; model.ReplicationFactor = 1;

        TopicSettingsResolver.Apply(opts, model);

        Assert.Equal(1, model.Partitions);
        Assert.Equal(1, model.ReplicationFactor);
        Assert.False(model.AdditionalSettings.ContainsKey("retention.ms"));
        Assert.False(model.AdditionalSettings.ContainsKey("retentionMs"));
    }

    [Fact]
    public void Apply_AlternateRetentionKey_retentionMs()
    {
        var opts = NewOptions();
        opts.Topics["bar_5m_live"] = new TopicSection
        {
            Creation = new TopicCreationSection
            {
                NumPartitions = 4,
                ReplicationFactor = 1,
                Configs = new System.Collections.Generic.Dictionary<string, string>
                {
                    ["retentionMs"] = "120000"
                }
            }
        };

        var model = NewModel("bar_5m_live");
        TopicSettingsResolver.Apply(opts, model);

        Assert.Equal(4, model.Partitions);
        Assert.Equal(1, model.ReplicationFactor);
        Assert.True(model.AdditionalSettings.TryGetValue("retentionMs", out var v) && (v as string) == "120000");
    }
}

