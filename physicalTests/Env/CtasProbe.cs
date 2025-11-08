using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Ksql.Linq.Tests.Integration;

internal static class CtasProbe
{
    public sealed class Result
    {
        public string Test { get; set; } = string.Empty;
        public string KsqlUrl { get; set; } = string.Empty;
        public string KafkaBootstrap { get; set; } = string.Empty;
        public string SourceTopic { get; set; } = string.Empty;
        public string TargetName { get; set; } = string.Empty;
        public DateTime TimestampUtc { get; set; }
        public int? SourcePartitions { get; set; }
        public long? SourceHighWatermark { get; set; }
        public bool? TargetListedInShowQueries { get; set; }
        public bool? TargetListedInShowTables { get; set; }
        public int? TargetPullCount { get; set; }
        public string? Notes { get; set; }
    }

    public static async Task WriteProbeAsync(string testName, string ksqlUrl, string kafkaBootstrap, string sourceTopic, string targetName, CancellationToken ct)
    {
        var r = new Result
        {
            Test = testName,
            KsqlUrl = ksqlUrl,
            KafkaBootstrap = kafkaBootstrap,
            SourceTopic = sourceTopic,
            TargetName = targetName,
            TimestampUtc = DateTime.UtcNow
        };

        // Kafka topic metadata / watermarks
        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafkaBootstrap }).Build();
            var md = admin.GetMetadata(sourceTopic, TimeSpan.FromSeconds(3));
            r.SourcePartitions = md.Topics?[0].Partitions?.Count;

            // Use a lightweight consumer to query watermarks
            var cc = new ConsumerConfig { BootstrapServers = kafkaBootstrap, GroupId = Guid.NewGuid().ToString(), EnableAutoCommit = false, AllowAutoCreateTopics = true };
            using var consumer = new ConsumerBuilder<byte[], byte[]>(cc).Build();
            long high = 0;
            foreach (var p in md.Topics[0].Partitions)
            {
                var wm = consumer.QueryWatermarkOffsets(new TopicPartition(sourceTopic, p.PartitionId), TimeSpan.FromSeconds(2));
                if (wm.High.Value > high) high = wm.High.Value;
            }
            r.SourceHighWatermark = high;
            consumer.Close();
        }
        catch (Exception ex)
        {
            r.Notes = $"kafka-meta:{ex.GetType().Name}";
        }

        // ksql SHOW QUERIES / LIST TABLES and pull count
        try
        {
            using var http = new HttpClient { BaseAddress = new Uri(ksqlUrl) };
            var showQueries = new { ksql = "SHOW QUERIES;", streamsProperties = new Dictionary<string, object>() };
            var bodyQ = new StringContent(JsonSerializer.Serialize(showQueries), Encoding.UTF8, "application/vnd.ksql+json");
            var respQ = await http.PostAsync("/ksql", bodyQ, ct);
            var sQ = await respQ.Content.ReadAsStringAsync(ct);
            r.TargetListedInShowQueries = sQ.IndexOf(targetName, StringComparison.OrdinalIgnoreCase) >= 0;

            var listTables = new { ksql = "LIST TABLES;", streamsProperties = new Dictionary<string, object>() };
            var bodyT = new StringContent(JsonSerializer.Serialize(listTables), Encoding.UTF8, "application/vnd.ksql+json");
            var respT = await http.PostAsync("/ksql", bodyT, ct);
            var sT = await respT.Content.ReadAsStringAsync(ct);
            r.TargetListedInShowTables = sT.IndexOf(targetName, StringComparison.OrdinalIgnoreCase) >= 0;

            // Pull count (tolerate failure)
            try
            {
                var pull = new { sql = $"SELECT 1 FROM {targetName} LIMIT 1;" };
                var bodyP = new StringContent(JsonSerializer.Serialize(pull), Encoding.UTF8, "application/json");
                var respP = await http.PostAsync("/query", bodyP, ct);
                var sP = await respP.Content.ReadAsStringAsync(ct);
                int cnt = 0;
                using var doc = JsonDocument.Parse(sP);
                foreach (var el in doc.RootElement.EnumerateArray())
                    if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out _)) cnt++;
                r.TargetPullCount = cnt;
            }
            catch { }
        }
        catch (Exception ex)
        {
            r.Notes = (r.Notes is null ? string.Empty : r.Notes + "|") + $"ksql:{ex.GetType().Name}";
        }

        try
        {
            var dir = Path.Combine(Environment.CurrentDirectory, "reports", "physical", "ctas_probes");
            Directory.CreateDirectory(dir);
            var path = Path.Combine(dir, $"{testName}_{targetName}_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json");
            var json = JsonSerializer.Serialize(r, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(path, json);
            Console.WriteLine($"[ctas-probe] {path}");
        }
        catch { }
    }
}
