using Confluent.Kafka;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Events;
using Ksql.Linq.Infrastructure.Admin;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Ksql;

/// <summary>
/// Encapsulates the ancillary stabilization checks that were previously embedded
/// inside <see cref="KsqlContext"/>. This keeps monitoring-focused logic together
/// and allows the orchestration surface to stay minimal.
/// </summary>
internal static class KsqlPersistentQueryMonitor
{
    private const string DefaultBootstrapServers = "127.0.0.1:39092";

    public static async Task EnsureRowsLastTableAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        Func<Task<HashSet<string>>> getTableTopics,
        Func<Task<HashSet<string>>> getStreamTopics,
        global::Ksql.Linq.Core.Abstractions.EntityModel rowsModel,
        int ddlRetryCount,
        int ddlRetryInitialDelayMs,
        Func<RuntimeEvent, Task>? publishEvent)
    {
        if (rowsModel == null)
            return;

        var response = await KsqlRowsLastOrchestrator.EnsureAsync(
            execute,
            getTableTopics,
            getStreamTopics,
            rowsModel,
            Math.Max(0, ddlRetryCount),
            Math.Max(0, ddlRetryInitialDelayMs),
            publishEvent).ConfigureAwait(false);

        if (!response.IsSuccess)
            throw new InvalidOperationException($"rows_last ensure failed: {response.Message}");
    }

    public static async Task WaitForDerivedQueriesRunningAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        IEnumerable<string> targetTopics,
        TimeSpan timeout)
    {
        foreach (var topic in targetTopics)
        {
            await KsqlWaitClient.WaitForQueryRunningAsync(
                execute,
                topic,
                null,
                timeout).ConfigureAwait(false);
        }
    }

    public static async Task TerminateQueriesAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        ILogger? logger,
        IReadOnlyList<PersistentQueryExecution> executions)
    {
        if (executions == null || executions.Count == 0)
            return;

        foreach (var execution in executions)
        {
            try
            {
                var terminate = $"TERMINATE {execution.QueryId};";
                var response = await execute(terminate).ConfigureAwait(false);
                if (!response.IsSuccess)
                {
                    logger?.LogWarning("Termination of query {QueryId} reported non-success: {Message}", execution.QueryId, response.Message);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed to terminate query {QueryId}", execution.QueryId);
            }
        }
    }

    public static async Task WaitForKsqlReadyAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            try
            {
                var res = await execute("SHOW TOPICS;").ConfigureAwait(false);
                if (res.IsSuccess)
                    return;
            }
            catch
            {
            }
            await Task.Delay(500).ConfigureAwait(false);
        }
    }

    public static async Task WarmupKsqlWithTopicsOrFailAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            try
            {
                var res = await execute("SHOW TOPICS;").ConfigureAwait(false);
                if (res.IsSuccess)
                {
                    try { await execute("SHOW TOPICS;").ConfigureAwait(false); } catch { }
                    return;
                }
            }
            catch
            {
            }
            await Task.Delay(500).ConfigureAwait(false);
        }
        throw new TimeoutException("ksqlDB warmup timed out while waiting for SHOW TOPICS to succeed.");
    }

    public static async Task AssertTopicPartitionsAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        global::Ksql.Linq.Core.Abstractions.EntityModel model)
    {
        var res = await execute("SHOW TOPICS;").ConfigureAwait(false);
        if (!res.IsSuccess || string.IsNullOrWhiteSpace(res.Message))
            throw new InvalidOperationException("SHOW TOPICS failed");

        if (TryReadTopicPartitionsFromJson(res.Message, model.GetTopicName(), out var partitionsFromJson))
        {
            if (partitionsFromJson == model.Partitions)
                return;
            throw new InvalidOperationException($"Topic {model.GetTopicName()} partition mismatch");
        }

        var lines = res.Message.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            if (!line.Contains('|')) continue;
            var parts = line.Split('|', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2) continue;
            if (parts[0].Trim().Equals(model.GetTopicName(), StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(parts[1].Trim(), out var partitions) && partitions == model.Partitions)
                    return;
                throw new InvalidOperationException($"Topic {model.GetTopicName()} partition mismatch");
            }
        }
        throw new InvalidOperationException($"Topic {model.GetTopicName()} not found");
    }

    public static bool TryReadTopicPartitionsFromJson(string message, string topicName, out int partitions)
    {
        partitions = 0;
        if (string.IsNullOrWhiteSpace(message) || string.IsNullOrWhiteSpace(topicName))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(message);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                return false;

            foreach (var entry in doc.RootElement.EnumerateArray())
            {
                if (!entry.TryGetProperty("topics", out var topics) || topics.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var topic in topics.EnumerateArray())
                {
                    if (topic.ValueKind != JsonValueKind.Object)
                        continue;

                    if (!topic.TryGetProperty("name", out var nameEl) || nameEl.ValueKind != JsonValueKind.String)
                        continue;

                    var name = nameEl.GetString();
                    if (string.IsNullOrWhiteSpace(name) || !name.Equals(topicName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (topic.TryGetProperty("partitions", out var partitionsEl)
                        && partitionsEl.ValueKind == JsonValueKind.Number
                        && partitionsEl.TryGetInt32(out var partitionCount)
                        && partitionCount > 0)
                    {
                        partitions = partitionCount;
                        return true;
                    }

                    if (topic.TryGetProperty("replicaInfo", out var replicaEl) && replicaEl.ValueKind == JsonValueKind.Array)
                    {
                        var count = replicaEl.GetArrayLength();
                        if (count > 0)
                        {
                            partitions = count;
                            return true;
                        }
                    }

                    return false;
                }
            }
        }
        catch
        {
        }

        return false;
    }

    public static async Task EnsureInternalTopicsReadyAsync(
        KafkaAdminService? adminService,
        ILogger? logger,
        string? queryId,
        int partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (adminService == null || string.IsNullOrWhiteSpace(queryId))
            return;

        if (partitions <= 0)
            partitions = 1;

        var (repartitionTopic, changelogTopic) = BuildInternalTopicNames(queryId);
        await EnsureInternalTopicReadyAsync(adminService, logger, repartitionTopic, partitions, timeout, cancellationToken).ConfigureAwait(false);
        await EnsureInternalTopicReadyAsync(adminService, logger, changelogTopic, partitions, timeout, cancellationToken).ConfigureAwait(false);
    }

    public static async Task EnsureSchemaSubjectsReadyAsync(
        Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>? schemaRegistryClient,
        ILogger? logger,
        global::Ksql.Linq.Core.Abstractions.EntityModel targetModel,
        string targetTopic,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (schemaRegistryClient == null)
            return;

        ConfluentSchemaRegistry.ISchemaRegistryClient client;
        try
        {
            client = schemaRegistryClient.Value;
        }
        catch
        {
            return;
        }

        var subjects = new[]
        {
            $"{targetTopic}-key",
            $"{targetTopic}-value"
        };

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var allReady = true;
            foreach (var subject in subjects)
            {
                if (!await SubjectExistsAsync(client, subject).ConfigureAwait(false))
                {
                    allReady = false;
                    break;
                }
            }

            if (allReady)
            {
                if (!string.IsNullOrWhiteSpace(targetModel?.ValueSchemaFullName))
                {
                    try
                    {
                        var latest = await client.GetLatestSchemaAsync(subjects[1]).ConfigureAwait(false);
                        if (latest?.SchemaString != null &&
                            latest.SchemaString.IndexOf(targetModel.ValueSchemaFullName, StringComparison.Ordinal) < 0)
                        {
                            logger?.LogWarning("Schema subject {Subject} does not include expected full name {Expected}", subjects[1], targetModel.ValueSchemaFullName);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger?.LogDebug(ex, "Failed to verify schema full name for subject {Subject}", subjects[1]);
                    }
                }
                return;
            }

            await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
        }

        logger?.LogWarning("Schema subjects for {Topic} were not ready within {Seconds:F0}s", targetTopic, timeout.TotalSeconds);
    }

    public static async Task VerifyOutputRecordsAsync(
        KafkaAdminService? adminService,
        string? bootstrapServers,
        ILogger? logger,
        string targetTopic,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero || adminService == null)
        {
            if (timeout <= TimeSpan.Zero)
                logger?.LogInformation("Skipping output verification for {Topic} because timeout <= 0", targetTopic);
            return;
        }

        var metadata = adminService.TryGetTopicMetadata(targetTopic);
        if (metadata?.Partitions == null || metadata.Partitions.Count == 0)
        {
            logger?.LogWarning("Unable to verify output records; topic metadata missing for {Topic}", targetTopic);
            return;
        }

        var bootstrap = string.IsNullOrWhiteSpace(bootstrapServers) ? DefaultBootstrapServers : bootstrapServers!;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"ksql-verify-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false
        };

        try
        {
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
            var assignments = metadata.Partitions
                .Select(p => new TopicPartitionOffset(new TopicPartition(targetTopic, p.PartitionId), Offset.Beginning))
                .ToList();
            consumer.Assign(assignments);

            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(250));
                if (consumeResult == null)
                {
                    await Task.Delay(250, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                logger?.LogInformation("Observed record on {Topic} partition {Partition} offset {Offset}",
                    targetTopic,
                    consumeResult.TopicPartition.Partition.Value,
                    consumeResult.Offset.Value);
                consumer.Close();
                return;
            }

            consumer.Close();
            logger?.LogWarning("No records observed on {Topic} within {Seconds:F0}s", targetTopic, timeout.TotalSeconds);
        }
        catch (Exception ex)
        {
            logger?.LogDebug(ex, "Failed to verify output records for {Topic}", targetTopic);
        }
    }

    private static async Task EnsureInternalTopicReadyAsync(
        KafkaAdminService adminService,
        ILogger? logger,
        string topicName,
        int partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        var pollDelay = TimeSpan.FromSeconds(2);
        var createDelay = TimeSpan.FromSeconds(6);
        var creationThreshold = DateTime.UtcNow + createDelay;
        var creationAttempted = false;

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var metadata = adminService.TryGetTopicMetadata(topicName)
                ?? adminService.TryGetTopicMetadata(topicName.ToLowerInvariant());
            if (metadata?.Partitions != null && metadata.Partitions.Count > 0)
            {
                if (metadata.Partitions.Count != partitions)
                {
                    logger?.LogWarning("Internal topic {Topic} has {Actual} partitions, expected {Expected}",
                        topicName,
                        metadata.Partitions.Count,
                        partitions);
                }
                return;
            }

            if (!creationAttempted && DateTime.UtcNow >= creationThreshold)
            {
                try
                {
                    logger?.LogInformation("Creating internal topic {Topic} with {Partitions} partitions (manual)", topicName, partitions);
                    await adminService.CreateDbTopicAsync(topicName, partitions, 1).ConfigureAwait(false);
                    logger?.LogInformation("Internal topic {Topic} created successfully", topicName);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Failed to create internal topic {Topic}", topicName);
                }
                creationAttempted = true;
            }

            await Task.Delay(pollDelay, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"Internal topic {topicName} was not ready within {timeout.TotalSeconds:F0}s");
    }

    private static (string RepartitionTopic, string ChangelogTopic) BuildInternalTopicNames(string queryId)
    {
        var id = queryId?.Trim();
        if (string.IsNullOrWhiteSpace(id))
            return (string.Empty, string.Empty);
        return ($"{id}-repartition", $"{id}-changelog");
    }

    private static async Task<bool> SubjectExistsAsync(ConfluentSchemaRegistry.ISchemaRegistryClient client, string subject)
    {
        try
        {
            var schema = await client.GetLatestSchemaAsync(subject).ConfigureAwait(false);
            return schema != null;
        }
        catch (Confluent.SchemaRegistry.SchemaRegistryException)
        {
            return false;
        }
        catch
        {
            return false;
        }
    }
}
