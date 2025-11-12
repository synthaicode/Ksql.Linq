using Avro;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace PhysicalTestEnv;

public static class Diagnostics
{
    public static class Ksql
    {
        public static async Task LogShowQueriesAsync(string baseUrl, ILogger logger, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(baseUrl)) throw new ArgumentException("Base URL required", nameof(baseUrl));
            logger.LogInformation("[Diag][ksql] SHOW QUERIES snapshot for {BaseUrl}", baseUrl);
            using var http = new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/')) };
            var payload = new { ksql = "SHOW QUERIES;", streamsProperties = new { } };
            using var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            using var response = await http.PostAsync("/ksql", content, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            logger.LogInformation("[Diag][ksql] status={Status} body={Body}", (int)response.StatusCode, TrimForLog(body));
        }

        public static async Task<StreamDiagnostics?> LogStreamSnapshotAsync(
            string ksqlBaseUrl,
            string? schemaRegistryUrl,
            string kafkaBootstrapServers,
            string objectName,
            ILogger logger,
            string? consumerGroup = null,
            IEnumerable<string>? subscribedTopics = null,
            CancellationToken cancellationToken = default)
        {
            await LogShowQueriesAsync(ksqlBaseUrl, logger, cancellationToken).ConfigureAwait(false);
            var describe = await LogDescribeAsync(ksqlBaseUrl, logger, objectName, cancellationToken).ConfigureAwait(false);
            if (describe?.Topic is null)
            {
                return null;
            }

            var kafkaMetrics = await Kafka.LogTopicDetailsAsync(
                kafkaBootstrapServers,
                describe.Topic,
                logger,
                consumerGroup,
                subscribedTopics,
                cancellationToken).ConfigureAwait(false);

            SchemaDigestMetrics? schemaMetrics = null;
            if (!string.IsNullOrWhiteSpace(schemaRegistryUrl))
            {
                schemaMetrics = await SchemaRegistry.LogSubjectDigestsAsync(
                    schemaRegistryUrl!,
                    describe.Topic,
                    logger,
                    cancellationToken).ConfigureAwait(false);
            }

            return new StreamDiagnostics(objectName, describe, kafkaMetrics, schemaMetrics);
        }

        private static string QuoteIdentifier(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return name;

            if (name.StartsWith('`') && name.EndsWith('`'))
                return name;

            foreach (var ch in name)
            {
                if (!(char.IsLetterOrDigit(ch) || ch == '_'))
                    return $"`{name.Replace("`", "``")}`";
            }

            return name;
        }

        private static async Task<DescribeSnapshot?> LogDescribeAsync(
            string baseUrl,
            ILogger logger,
            string objectName,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(objectName)) throw new ArgumentException("Object name required", nameof(objectName));
            using var http = new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/')) };

            // ksqlDB 6.x 以降は DESCRIBE の直後に STREAM/TABLE キーワードが必須になったため、
            // ここでは STREAM/TABLE の有無や旧構文を順に試して互換性を担保する。
            var statements = new[]
            {
                $"DESCRIBE {QuoteIdentifier(objectName)} EXTENDED;",
                $"DESCRIBE {QuoteIdentifier(objectName)};",
                $"DESCRIBE TOPIC {QuoteIdentifier(objectName)};"
            };

            DescribeSnapshot? snapshot = null;
            string? lastBody = null;
            int lastStatus = 0;
            var attempt = 0;

            foreach (var statement in statements)
            {
                attempt++;
                logger.LogInformation("[Diag][ksql] DESCRIBE attempt {Attempt} using '{Statement}'", attempt, statement);

                var payload = new { ksql = statement, streamsProperties = new { } };
                using var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
                using var response = await http.PostAsync("/ksql", content, cancellationToken);
                lastStatus = (int)response.StatusCode;
                lastBody = await response.Content.ReadAsStringAsync(cancellationToken);

                try
                {
                    snapshot = ParseDescribe(lastBody);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[Diag][ksql] failed to parse DESCRIBE response for {Object} using statement '{Statement}'", objectName, statement);
                }

                if (snapshot is not null)
                {
                    break;
                }

                if (lastStatus != 400 || string.IsNullOrWhiteSpace(lastBody) || !lastBody.Contains("syntax", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }
            }

            if (snapshot is null)
            {
                logger.LogInformation("[Diag][ksql] DESCRIBE {Object} status={Status} body={Body}", objectName, lastStatus, TrimForLog(lastBody ?? "<empty>"));
                return null;
            }

            logger.LogInformation(
                "[Diag][ksql] DESCRIBE {Object} status={Status} processed={Processed} punctuators={Punctuators} committed={Committed} skippedByDeser={Skipped} lastDeserializerError={LastError}",
                objectName,
                lastStatus,
                snapshot.Processed,
                snapshot.Punctuators,
                snapshot.Committed,
                snapshot.SkippedByDeserializer,
                snapshot.LastDeserializerError ?? "n/a");

            logger.LogInformation(
                "[Diag][ksql] {Object} windowType={WindowType} keyFormat={KeyFormat} valueFormat={ValueFormat} topic={Topic}",
                objectName,
                snapshot.WindowType ?? "none",
                snapshot.KeyFormat ?? "n/a",
                snapshot.ValueFormat ?? "n/a",
                snapshot.Topic ?? "n/a");

            return snapshot;
        }

        private static DescribeSnapshot ParseDescribe(string body)
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Array || doc.RootElement.GetArrayLength() == 0)
            {
                throw new InvalidOperationException("DESCRIBE response did not contain an array");
            }

            var source = doc.RootElement[0].GetProperty("sourceDescription");
            var stats = source.TryGetProperty("statistics", out var statisticsElement) ? statisticsElement.GetString() ?? string.Empty : string.Empty;
            var errorStats = source.TryGetProperty("errorStats", out var errorStatsElement) ? errorStatsElement.GetString() ?? string.Empty : string.Empty;

            long processed = ExtractLong(stats, "processed");
            long punctuators = ExtractLong(stats, "punctuators");
            long committed = ExtractLong(stats, "committed");
            long skipped = ExtractLong(errorStats, "failed");
            string? lastError = ExtractLastError(errorStats);

            return new DescribeSnapshot
            {
                Name = source.TryGetProperty("name", out var nameElement) ? nameElement.GetString() : null,
                Topic = source.TryGetProperty("topic", out var topicElement) ? topicElement.GetString() : null,
                WindowType = source.TryGetProperty("windowType", out var windowElement) && windowElement.ValueKind != JsonValueKind.Null ? windowElement.GetString() : null,
                KeyFormat = source.TryGetProperty("keyFormat", out var keyElement) ? keyElement.GetString() : null,
                ValueFormat = source.TryGetProperty("valueFormat", out var valueElement) ? valueElement.GetString() : null,
                Processed = processed,
                Punctuators = punctuators,
                Committed = committed,
                SkippedByDeserializer = skipped,
                LastDeserializerError = lastError
            };
        }

        private static long ExtractLong(string input, string name)
        {
            if (string.IsNullOrEmpty(input)) return 0;
            var match = Regex.Match(input, $@"{name}\s*:?\s*(?<value>\d+)", RegexOptions.IgnoreCase);
            return match.Success && long.TryParse(match.Groups["value"].Value, out var value) ? value : 0;
        }

        private static string? ExtractLastError(string input)
        {
            if (string.IsNullOrEmpty(input)) return null;
            var match = Regex.Match(input, "last\\s*-?error:?\\s*(?<value>[^,]+)", RegexOptions.IgnoreCase);
            return match.Success ? match.Groups["value"].Value.Trim() : null;
        }

        private static string TrimForLog(string body)
        {
            if (string.IsNullOrEmpty(body)) return "<empty>";
            var trimmed = body.Replace('\n', ' ').Replace("  ", " ");
            return trimmed.Length > 500 ? trimmed[..500] + "..." : trimmed;
        }
    }

    public static class Kafka
    {
        public static Task<KafkaTopicMetrics> LogTopicDetailsAsync(
            string bootstrapServers,
            string topic,
            ILogger logger,
            string? consumerGroup = null,
            IEnumerable<string>? subscribedTopics = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(bootstrapServers)) throw new ArgumentException("Bootstrap servers required", nameof(bootstrapServers));
            if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Topic required", nameof(topic));

            var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
            using var admin = new AdminClientBuilder(adminConfig).Build();

            var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
            var topicMeta = metadata.Topics.FirstOrDefault();
            if (topicMeta is null || topicMeta.Error.Code != ErrorCode.NoError)
            {
                logger.LogWarning("[Diag][kafka] unable to fetch metadata for topic {Topic}: {Error}", topic, topicMeta?.Error.ToString() ?? "unknown");
                return Task.FromResult(new KafkaTopicMetrics(topic, Array.Empty<int>(), Array.Empty<string>(), 0, "empty", null, "Unknown"));
            }

            var partitionIds = topicMeta.Partitions.Select(p => p.PartitionId).ToArray();
            var subscribedList = (subscribedTopics ?? new[] { topic }).ToArray();
            var assignedPartitions = partitionIds.Length == 0 ? Array.Empty<int>() : partitionIds;

            var producedCount = 0L;
            string lastDigest = "empty";

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"diag-{Guid.NewGuid():N}",
                EnableAutoCommit = false,
                AllowAutoCreateTopics = false,
                StatisticsIntervalMs = 0
            };

            var watermarkCache = new Dictionary<int, WatermarkOffsets>();

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                foreach (var partition in partitionIds)
                {
                    var tp = new TopicPartition(topic, new Partition(partition));
                    var watermark = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                    watermarkCache[partition] = watermark;
                    var highOffset = watermark.High;
                    if (highOffset != Offset.Unset)
                    {
                        producedCount += Math.Max(0, highOffset.Value);
                    }
                }

                foreach (var partition in partitionIds)
                {
                    var watermark = watermarkCache[partition];
                    var highOffset = watermark.High;
                    if (highOffset == Offset.Unset || highOffset.Value <= 0) continue;
                    var offset = new TopicPartitionOffset(topic, new Partition(partition), new Offset(highOffset.Value - 1));
                    consumer.Assign(offset);
                    var record = consumer.Consume(TimeSpan.FromSeconds(2));
                    if (record?.Message is not null)
                    {
                        lastDigest = ComputeDigest(record.Message.Key, record.Message.Value);
                        break;
                    }
                }
            }

            long? consumerLag = null;
            if (!string.IsNullOrWhiteSpace(consumerGroup))
            {
                try
                {
                    var committedConfig = new ConsumerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        GroupId = consumerGroup,
                        EnableAutoCommit = false,
                        AllowAutoCreateTopics = false
                    };
                    using var committedConsumer = new ConsumerBuilder<byte[], byte[]>(committedConfig).Build();
                    var tpList = partitionIds.Select(p => new TopicPartition(topic, new Partition(p))).ToList();
                    var committed = committedConsumer.Committed(tpList, TimeSpan.FromSeconds(5));
                    long lag = 0;
                    foreach (var entry in committed)
                    {
                        var high = watermarkCache.TryGetValue(entry.Partition.Value, out var watermark) && watermark.High != Offset.Unset ? watermark.High.Value : 0;
                        var committedOffset = entry.Offset == Offset.Unset ? 0 : entry.Offset.Value;
                        lag += Math.Max(0, high - committedOffset);
                    }
                    consumerLag = lag;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[Diag][kafka] failed to compute consumer lag for group {Group}", consumerGroup);
                }
            }

            var groupState = consumerLag.HasValue ? "Stable" : "Unknown";

            logger.LogInformation(
                "[Diag][kafka] topic={Topic} subscribedTopics=[{Subscribed}] assignedPartitions=[{Assigned}] producedCount={Produced} lastProducedDigest={Digest} consumerLag={ConsumerLag} groupState={GroupState}",
                topic,
                subscribedList.Length == 0 ? "n/a" : string.Join(',', subscribedList),
                assignedPartitions.Length == 0 ? "n/a" : string.Join(',', assignedPartitions),
                producedCount,
                lastDigest,
                consumerLag?.ToString() ?? "n/a",
                groupState);

            return Task.FromResult(new KafkaTopicMetrics(topic, assignedPartitions, subscribedList, producedCount, lastDigest, consumerLag, groupState));
        }

        private static string ComputeDigest(byte[]? key, byte[]? value)
        {
            if ((key == null || key.Length == 0) && (value == null || value.Length == 0))
            {
                return "empty";
            }

            Span<byte> buffer = stackalloc byte[(key?.Length ?? 0) + (value?.Length ?? 0)];
            var offset = 0;
            if (key is not null)
            {
                key.CopyTo(buffer.Slice(offset, key.Length));
                offset += key.Length;
            }
            if (value is not null)
            {
                value.CopyTo(buffer.Slice(offset, value.Length));
            }

            var hash = SHA256.HashData(buffer);
            return Convert.ToHexString(hash);
        }
    }

    public static class SchemaRegistry
    {
        public static async Task<SchemaDigestMetrics> LogSubjectDigestsAsync(string baseUrl, string topic, ILogger logger, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(baseUrl)) throw new ArgumentException("Base URL required", nameof(baseUrl));
            if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Topic required", nameof(topic));

            var normalized = baseUrl.TrimEnd('/');
            var subjects = new[] { global::Ksql.Linq.SchemaRegistryTools.SchemaSubjects.KeyFor(topic), global::Ksql.Linq.SchemaRegistryTools.SchemaSubjects.ValueFor(topic) };
            using var http = new HttpClient();

            string? keyDigest = null;
            string? valueDigest = null;

            foreach (var subject in subjects)
            {
                try
                {
                    var url = $"{normalized}/subjects/{Uri.EscapeDataString(subject)}/versions/latest";
                    using var resp = await http.GetAsync(url, cancellationToken);
                    var body = await resp.Content.ReadAsStringAsync(cancellationToken);
                    if (!resp.IsSuccessStatusCode)
                    {
                        logger.LogWarning("[Diag][sr] subject={Subject} status={Status} body={Body}", subject, (int)resp.StatusCode, TrimForLog(body));
                        continue;
                    }

                    using var doc = JsonDocument.Parse(body);
                    var schemaJson = doc.RootElement.TryGetProperty("schema", out var schemaElement) ? schemaElement.GetString() : null;
                    var digest = ComputeSchemaDigest(schemaJson);
                    logger.LogInformation("[Diag][sr] subject={Subject} schemaDigest={Digest}", subject, digest);

                    if (subject.EndsWith("-key", StringComparison.OrdinalIgnoreCase))
                    {
                        keyDigest = digest;
                    }
                    else if (subject.EndsWith("-value", StringComparison.OrdinalIgnoreCase))
                    {
                        valueDigest = digest;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[Diag][sr] failed to fetch schema digest for {Subject}", subject);
                }
            }

            return new SchemaDigestMetrics(topic, keyDigest, valueDigest);
        }

        private static string ComputeSchemaDigest(string? schemaJson)
        {
            if (string.IsNullOrEmpty(schemaJson)) return "n/a";
            try
            {
                var schema = Schema.Parse(schemaJson);
                if (schema is RecordSchema record)
                {
                    var fieldNames = record.Fields.Select(f => f.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
                    var payload = string.Join(',', fieldNames);
                    var hash = SHA256.HashData(Encoding.UTF8.GetBytes(payload));
                    return Convert.ToHexString(hash);
                }
                return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(schema.Name)));
            }
            catch
            {
                return "n/a";
            }
        }

        private static string TrimForLog(string body)
        {
            if (string.IsNullOrEmpty(body)) return "<empty>";
            var trimmed = body.Replace('\n', ' ').Replace("  ", " ");
            return trimmed.Length > 500 ? trimmed[..500] + "..." : trimmed;
        }
    }

    public sealed class DescribeSnapshot
    {
        public string? Name { get; init; }
        public string? Topic { get; init; }
        public string? WindowType { get; init; }
        public string? KeyFormat { get; init; }
        public string? ValueFormat { get; init; }
        public long Processed { get; init; }
        public long Punctuators { get; init; }
        public long Committed { get; init; }
        public long SkippedByDeserializer { get; init; }
        public string? LastDeserializerError { get; init; }
    }

    public sealed class KafkaTopicMetrics
    {
        public KafkaTopicMetrics(string topic, int[] assignedPartitions, string[] subscribedTopics, long producedCount, string lastDigest, long? consumerLag, string groupState)
        {
            Topic = topic;
            AssignedPartitions = assignedPartitions;
            SubscribedTopics = subscribedTopics;
            ProducedCount = producedCount;
            LastProducedDigest = lastDigest;
            ConsumerLag = consumerLag;
            GroupState = groupState;
        }

        public string Topic { get; }
        public int[] AssignedPartitions { get; }
        public string[] SubscribedTopics { get; }
        public long ProducedCount { get; }
        public string LastProducedDigest { get; }
        public long? ConsumerLag { get; }
        public string GroupState { get; }
    }

    public sealed class SchemaDigestMetrics
    {
        public SchemaDigestMetrics(string topic, string? keyDigest, string? valueDigest)
        {
            Topic = topic;
            KeyDigest = keyDigest ?? "n/a";
            ValueDigest = valueDigest ?? "n/a";
        }

        public string Topic { get; }
        public string KeyDigest { get; }
        public string ValueDigest { get; }
    }

    public sealed class StreamDiagnostics
    {
        public StreamDiagnostics(string name, DescribeSnapshot snapshot, KafkaTopicMetrics kafka, SchemaDigestMetrics? schema)
        {
            Name = name;
            Snapshot = snapshot;
            Kafka = kafka;
            Schema = schema;
        }

        public string Name { get; }
        public DescribeSnapshot Snapshot { get; }
        public KafkaTopicMetrics Kafka { get; }
        public SchemaDigestMetrics? Schema { get; }
    }
}
