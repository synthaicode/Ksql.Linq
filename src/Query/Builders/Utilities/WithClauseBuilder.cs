using System;
using System.Collections.Generic;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Dsl;

namespace Ksql.Linq.Query.Builders.Utilities;

internal enum WithClauseRetentionPolicy
{
    Auto,
    Require,
    Disallow
}

internal static class WithClauseBuilder
{
    public static List<string> BuildWithParts(
        string kafkaTopic,
        bool hasKey,
        string? valueSchemaFullName,
        string? timestampColumn,
        int? partitions,
        short? replicas,
        long? retentionMs = null,
        bool allowRetentionMs = true,
        KsqlQueryModel? model = null,
        WithClauseRetentionPolicy retentionPolicy = WithClauseRetentionPolicy.Auto,
        string? cleanupPolicy = null,
        StreamTableType? objectType = null)
    {
        if (string.IsNullOrWhiteSpace(kafkaTopic))
            throw new ArgumentException("kafkaTopic is required", nameof(kafkaTopic));

        var parts = new List<string> { $"KAFKA_TOPIC='{kafkaTopic}'" };

        if (TryResolveCleanupPolicy(model, cleanupPolicy, out var resolvedCleanup))
            parts.Add($"CLEANUP_POLICY='{resolvedCleanup}'");

        if (hasKey)
            parts.Add("KEY_FORMAT='AVRO'");

        parts.Add("VALUE_FORMAT='AVRO'");

        if (!string.IsNullOrWhiteSpace(valueSchemaFullName))
            parts.Add($"VALUE_AVRO_SCHEMA_FULL_NAME='{valueSchemaFullName}'");

        if (!string.IsNullOrWhiteSpace(timestampColumn))
            parts.Add($"TIMESTAMP='{timestampColumn}'");

        if (partitions.HasValue && partitions.Value > 0)
            parts.Add($"PARTITIONS={partitions.Value}");
        if (replicas.HasValue && replicas.Value > 0)
            parts.Add($"REPLICAS={replicas.Value}");

        var effectivePolicy = allowRetentionMs ? retentionPolicy : WithClauseRetentionPolicy.Disallow;
        var isWindowed = model?.HasTumbling();
        if (objectType.HasValue && !isWindowed.HasValue)
            isWindowed = objectType.Value != StreamTableType.Table ? (bool?)true : null;
        if (TryResolveRetention(model, effectivePolicy, retentionMs, out var resolvedRetention, objectType, isWindowed))
            parts.Add($"RETENTION_MS={resolvedRetention}");

        return parts;
    }

    public static string BuildClause(
        string kafkaTopic,
        bool hasKey,
        string? valueSchemaFullName,
        string? timestampColumn,
        int? partitions,
        short? replicas,
        long? retentionMs = null,
        bool allowRetentionMs = true,
        KsqlQueryModel? model = null,
        WithClauseRetentionPolicy retentionPolicy = WithClauseRetentionPolicy.Auto,
        string? cleanupPolicy = null,
        StreamTableType? objectType = null)
    {
        var parts = BuildWithParts(
            kafkaTopic,
            hasKey,
            valueSchemaFullName,
            timestampColumn,
            partitions,
            replicas,
            retentionMs,
            allowRetentionMs,
            model,
            retentionPolicy,
            cleanupPolicy,
            objectType);
        return $"WITH ({string.Join(", ", parts)})";
    }

    public static void AddRetentionIfSupported(
        List<string> withParts,
        long? retentionMs,
        bool allowRetentionMs,
        KsqlQueryModel? model = null,
        WithClauseRetentionPolicy retentionPolicy = WithClauseRetentionPolicy.Auto,
        StreamTableType? objectType = null,
        bool? isWindowed = null)
    {
        if (withParts == null)
            throw new ArgumentNullException(nameof(withParts));

        var effectivePolicy = allowRetentionMs ? retentionPolicy : WithClauseRetentionPolicy.Disallow;
        if (TryResolveRetention(model, effectivePolicy, retentionMs, out var resolvedRetention, objectType, isWindowed))
            withParts.Add($"RETENTION_MS={resolvedRetention}");
    }

    private static bool TryResolveRetention(
        KsqlQueryModel? model,
        WithClauseRetentionPolicy policy,
        long? explicitRetention,
        out long result,
        StreamTableType? objectType = null,
        bool? isWindowed = null)
    {
        result = 0;

        if (!ShouldApplyRetention(policy, model, objectType, isWindowed))
            return false;

        if (explicitRetention.HasValue && explicitRetention.Value > 0)
        {
            result = explicitRetention.Value;
            return true;
        }

        if (model != null &&
            model.Extras.TryGetValue("sink/retentionMs", out var extraValue) &&
            TryConvertRetention(extraValue, out var extraRetention) &&
            extraRetention > 0)
        {
            result = extraRetention;
            return true;
        }

        if (policy == WithClauseRetentionPolicy.Require)
            throw new InvalidOperationException("RETENTION_MS is required but no value was provided.");

        return false;
    }

    private static bool ShouldApplyRetention(WithClauseRetentionPolicy policy, KsqlQueryModel? model, StreamTableType? objectType, bool? isWindowed)
    {
        if (policy == WithClauseRetentionPolicy.Disallow)
            return false;
        if (policy == WithClauseRetentionPolicy.Require)
            return true;

        var windowed = isWindowed ?? model?.HasTumbling() ?? false;
        if (objectType.HasValue)
        {
            if (objectType.Value == StreamTableType.Table && !windowed)
                return false;
        }
        else if (model != null)
        {
            var derivedType = model.DetermineType();
            if (derivedType == StreamTableType.Table && !windowed)
                return false;
        }

        return true;
    }

    public static bool TryConvertRetention(object? value, out long result)
    {
        switch (value)
        {
            case long l when l > 0:
                result = l;
                return true;
            case int i when i > 0:
                result = i;
                return true;
            case short s when s > 0:
                result = s;
                return true;
            case string s when long.TryParse(s, out var parsed) && parsed > 0:
                result = parsed;
                return true;
            default:
                result = 0;
                return false;
        }
    }

    public static long ResolveRetentionMs(IReadOnlyDictionary<string, object> settings, long defaultMs)
    {
        if (settings == null)
            throw new ArgumentNullException(nameof(settings));

        if (TryConvertRetention(settings, "retentionMs", out var camel))
            return camel;
        if (TryConvertRetention(settings, "retention.ms", out var dotted))
            return dotted;
        return defaultMs;
    }

    public static bool TryConvertRetention(
        IReadOnlyDictionary<string, object> settings,
        string key,
        out long result)
    {
        if (settings != null && settings.TryGetValue(key, out var value))
            return TryConvertRetention(value, out result);

        result = 0;
        return false;
    }

    private static bool TryResolveCleanupPolicy(
        KsqlQueryModel? model,
        string? explicitPolicy,
        out string result)
    {
        static bool TryNormalize(object? value, out string normalized)
        {
            if (value is null)
            {
                normalized = string.Empty;
                return false;
            }

            var text = value switch
            {
                string s => s,
                _ => value.ToString()
            };

            if (string.IsNullOrWhiteSpace(text))
            {
                normalized = string.Empty;
                return false;
            }

            normalized = text.Trim();
            return normalized.Length > 0;
        }

        if (TryNormalize(explicitPolicy, out var policy))
        {
            result = policy;
            return true;
        }

        if (model != null)
        {
            if (model.Extras.TryGetValue("sink/cleanupPolicy", out var extrasPolicy) && TryNormalize(extrasPolicy, out policy))
            {
                result = policy;
                return true;
            }

            if (model.Extras.TryGetValue("sink/cleanup.policy", out var dottedPolicy) && TryNormalize(dottedPolicy, out policy))
            {
                result = policy;
                return true;
            }
        }

        result = string.Empty;
        return false;
    }
}