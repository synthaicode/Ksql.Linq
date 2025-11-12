using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Ksql.Linq.Infrastructure.Monitoring;

internal static class LagMonitor
{
    public sealed class LagSnapshot
    {
        public long SumLag { get; init; }
        public long MaxLag { get; init; }
    }

    public static Task<LagSnapshot> GetGroupLagAsync(string bootstrapServers, string groupId, string[] topics, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(bootstrapServers)) throw new ArgumentNullException(nameof(bootstrapServers));
        if (string.IsNullOrWhiteSpace(groupId)) throw new ArgumentNullException(nameof(groupId));
        topics ??= Array.Empty<string>();

        var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"lagprobe-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        try
        {
            long sum = 0;
            long max = 0;
            // Build topic partition list from metadata
            var tps = topics.Distinct(StringComparer.OrdinalIgnoreCase)
                .Select(t => new TopicPartition(t, new Partition(0)))
                .ToList();
            if (tps.Count == 0)
                return Task.FromResult(new LagSnapshot { SumLag = 0, MaxLag = 0 });

            var committed = consumer.Committed(tps, TimeSpan.FromMilliseconds(500));
            foreach (var c in committed)
            {
                if (ct.IsCancellationRequested) break;
                try
                {
                    var w = consumer.QueryWatermarkOffsets(c.TopicPartition, TimeSpan.FromMilliseconds(500));
                    var high = w.High.Value;
                    var comm = c.Offset.Value < 0 ? 0 : c.Offset.Value;
                    var lag = Math.Max(0, high - comm);
                    sum += lag;
                    if (lag > max) max = lag;
                }
                catch { }
            }
            return Task.FromResult(new LagSnapshot { SumLag = sum, MaxLag = max });
        }
        finally
        {
            consumer.Close();
            consumer.Dispose();
        }
    }

    public static async Task<bool> WaitForZeroLagAsync(string bootstrapServers, string groupId, TimeSpan timeout, TimeSpan? pollInterval = null, string[] topics = null, int consecutiveZeroRequired = 1)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(500);
        var until = DateTime.UtcNow + timeout;
        if (consecutiveZeroRequired <= 0) consecutiveZeroRequired = 1;
        var streak = 0;
        while (DateTime.UtcNow < until)
        {
            var snap = await GetGroupLagAsync(bootstrapServers, groupId, topics ?? Array.Empty<string>(), CancellationToken.None);
            if (snap.SumLag == 0 && snap.MaxLag == 0)
            {
                streak++;
                if (streak >= consecutiveZeroRequired)
                    return true;
            }
            else
            {
                streak = 0;
            }
            await Task.Delay(interval);
        }
        return false;
    }
}
