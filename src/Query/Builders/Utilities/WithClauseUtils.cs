using System;
using System.Collections.Generic;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Dsl;

namespace Ksql.Linq.Query.Builders.Utilities;

internal static class WithClauseUtils
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
        return WithClauseBuilder.BuildWithParts(
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
        WithClauseBuilder.AddRetentionIfSupported(withParts, retentionMs, allowRetentionMs, model, retentionPolicy, objectType, isWindowed);
    }

    public static bool TryConvertRetention(
        IReadOnlyDictionary<string, object> settings,
        string key,
        out long result)
    {
        return WithClauseBuilder.TryConvertRetention(settings, key, out result);
    }
}