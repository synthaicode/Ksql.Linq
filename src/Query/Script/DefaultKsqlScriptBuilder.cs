using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Builders.Utilities;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Script;

namespace Ksql.Linq.Query.Script;

/// <summary>
/// Default implementation that inspects KsqlContext models and emits
/// CREATE STREAM/TABLE (and CSAS/CTAS for query-defined entities).
/// </summary>
public sealed class DefaultKsqlScriptBuilder : IKsqlScriptBuilder
{
    public KsqlScript Build(KsqlContext context)
    {
        if (context == null) throw new ArgumentNullException(nameof(context));

        var statements = new List<string>();

        var scriptHeader = BuildScriptHeader(context);
        if (!string.IsNullOrEmpty(scriptHeader))
        {
            statements.Add(scriptHeader);
        }

        var models = context.GetEntityModels();
        var configs = context.GetResolvedEntityConfigs();

        foreach (var (clrType, model) in models.OrderBy(kvp => kvp.Key.Name))
        {
            // Skip internal entities like DLQ
            if (clrType.Namespace != null && clrType.Namespace.StartsWith("Ksql.Linq.Messaging", StringComparison.Ordinal))
                continue;

            configs.TryGetValue(clrType, out var resolved);
            var topic = resolved?.SourceTopic ?? model.TopicName ?? clrType.GetKafkaTopicName();

            var hasKey = model.HasKeys();
            var partitions = model.Partitions > 0 ? model.Partitions : (int?)null;
            var replicas = model.ReplicationFactor > 0 ? model.ReplicationFactor : (short?)null;

            var retentionMs = WithClauseBuilder.ResolveRetentionMs(
                resolved?.AdditionalSettings ?? new Dictionary<string, object>(),
                defaultMs: 0);
            if (retentionMs <= 0) retentionMs = 0;

            var withParts = WithClauseUtils.BuildWithParts(
                kafkaTopic: topic,
                hasKey: hasKey,
                valueSchemaFullName: model.ValueSchemaFullName,
                timestampColumn: null,
                partitions: partitions,
                replicas: replicas,
                retentionMs: retentionMs > 0 ? retentionMs : (long?)null,
                allowRetentionMs: true,
                model: model.QueryModel,
                retentionPolicy: WithClauseRetentionPolicy.Auto,
                cleanupPolicy: null,
                objectType: model.GetExplicitStreamTableType());

            if (!string.IsNullOrWhiteSpace(resolved?.ValueSchemaSubject))
            {
                withParts.Add($"VALUE_SCHEMA_SUBJECT='{resolved.ValueSchemaSubject}'");
            }

            var headerLines = BuildHeaderLines(clrType, resolved);
            var ddl = BuildCreateStatement(clrType, model, withParts);

            var builder = new StringBuilder();
            foreach (var line in headerLines)
            {
                builder.AppendLine(line);
            }
            builder.AppendLine(ddl);

            statements.Add(builder.ToString().TrimEnd());
        }

        return new KsqlScript(statements);
    }

    private static IEnumerable<string> BuildHeaderLines(Type clrType, ResolvedEntityConfig? resolved)
    {
        if (resolved == null)
            yield break;

        if (!string.IsNullOrWhiteSpace(resolved.ValueSchemaSubject))
        {
            yield return $"-- Schema: Subject={resolved.ValueSchemaSubject}, Entity={resolved.Entity}";
        }

        if (!string.IsNullOrWhiteSpace(clrType.Namespace))
        {
            yield return $"-- Namespace: {clrType.Namespace}";
        }
    }

    private static string BuildScriptHeader(KsqlContext context)
    {
        var builder = new StringBuilder();

        var libAssembly = typeof(KsqlContext).Assembly.GetName();
        var targetAssembly = context.GetType().Assembly.GetName();
        var now = DateTimeOffset.Now;

        builder.AppendLine($"-- GeneratedBy: {libAssembly.Name} {libAssembly.Version}");
        builder.AppendLine($"-- TargetAssembly: {targetAssembly.Name} {targetAssembly.Version}");
        builder.AppendLine($"-- GeneratedAt: {now:O}");

        return builder.ToString().TrimEnd();
    }

    private static string BuildCreateStatement(Type clrType, Core.Abstractions.EntityModel model, List<string> withParts)
    {
        var streamName = model.TopicName ?? clrType.GetKafkaTopicName();
        var withClause = $"WITH ({string.Join(", ", withParts)})";

        // Query-defined entity (CSAS/CTAS)
        if (model.QueryModel != null)
        {
            var create = KsqlCreateStatementBuilder.Build(
                streamName,
                model.QueryModel,
                keySchemaFullName: model.KeySchemaFullName,
                valueSchemaFullName: model.ValueSchemaFullName,
                partitionBy: null);

            // Insert our WITH clause in place of the one generated inside the builder
            var idx = create.IndexOf("WITH (", StringComparison.OrdinalIgnoreCase);
            if (idx >= 0)
            {
                var before = create.Substring(0, idx);
                var afterIdx = create.IndexOf(")", idx, StringComparison.OrdinalIgnoreCase);
                if (afterIdx > idx)
                {
                    var after = create.Substring(afterIdx + 1);
                    return $"{before}{withClause}{after}";
                }
            }

            return create;
        }

        // Simple base entity: CREATE STREAM/TABLE ...
        var objectType = model.GetExplicitStreamTableType();
        var typeKeyword = objectType == StreamTableType.Table ? "TABLE" : "STREAM";
        return $"CREATE {typeKeyword} {streamName} {withClause};";
    }
}
