using System;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Configuration.Messaging;
using Ksql.Linq.Core.Extensions;

namespace Ksql.Linq.Configuration;

/// <summary>
/// Resolves per-topic settings (partitions, replicas, retention) from KsqlDslOptions.Topics
/// and applies cascade rules (e.g., &lt;base&gt;_&lt;tf&gt; → &lt;base&gt;_&lt;tf&gt;_live).
/// </summary>
internal static class TopicSettingsResolver
{
    public static void Apply(KsqlDslOptions options, EntityModel model)
    {
        if (options == null || model == null) return;

        var topic = model.GetTopicName();

        TopicSection? section = null;
        if (options.Topics.TryGetValue(topic, out var sec) && sec?.Creation != null)
        {
            section = sec;
        }
        // Cascade from base: <base>_<tf> to <base>_<tf>_live when explicit config missing
        if (section == null && topic.EndsWith("_live", StringComparison.OrdinalIgnoreCase))
        {
            var baseName = topic[..^("_live".Length)];
            if (options.Topics.TryGetValue(baseName, out var baseSec) && baseSec?.Creation != null)
                section = baseSec;
        }

        if (section?.Creation != null)
        {
            model.Partitions = section.Creation.NumPartitions;
            model.ReplicationFactor = section.Creation.ReplicationFactor;

            if (section.Creation.Configs != null)
            {
                if (section.Creation.Configs.TryGetValue("retention.ms", out var rstr) && !string.IsNullOrWhiteSpace(rstr))
                {
                    model.AdditionalSettings["retention.ms"] = rstr;
                }
                else if (section.Creation.Configs.TryGetValue("retentionMs", out var rstr2) && !string.IsNullOrWhiteSpace(rstr2))
                {
                    model.AdditionalSettings["retentionMs"] = rstr2;
                }
            }
        }

        if (model.Partitions <= 0) model.Partitions = 1;
        if (model.ReplicationFactor <= 0) model.ReplicationFactor = 1;
    }
}
