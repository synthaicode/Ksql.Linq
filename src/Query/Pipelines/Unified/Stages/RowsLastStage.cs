using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Metadata;
using System;
using System.Threading.Tasks;

namespace Ksql.Linq.Query.Pipelines.Unified.Stages;

/// <summary>
/// Ensures rows-last tables are materialized and metadata snapshots stay synchronized.
/// </summary>
internal sealed class RowsLastStage : IUnifiedPipelineStage
{
    private readonly KsqlQueryDdlMonitor.Dependencies _deps;

    public RowsLastStage(KsqlQueryDdlMonitor.Dependencies deps)
    {
        _deps = deps ?? throw new ArgumentNullException(nameof(deps));
    }

    public async Task ExecuteAsync(UnifiedPipelineContext context, UnifiedPipelineRequest request)
    {
        foreach (var execution in context.Executions)
        {
            if (execution?.Model == null)
                continue;

            var metadata = execution.Model.GetOrCreateMetadata();
            var role = PromoteRole(execution.Model, ref metadata);
            if (string.Equals(role, Role.Final1sStream.ToString(), StringComparison.OrdinalIgnoreCase))
            {
                var identifier = PromoteIdentifier(execution.Model, ref metadata);
                if (!string.IsNullOrWhiteSpace(identifier) && identifier.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase))
                {
                    QueryMetadataWriter.Apply(execution.Model, metadata);
                    await _deps.EnsureRowsLastTableAsync(
                        sql => _deps.ExecuteStatementAsync(sql),
                        () => _deps.KsqlDbClient.GetTableTopicsAsync(),
                        () => _deps.KsqlDbClient.GetStreamTopicsAsync(),
                        execution.Model).ConfigureAwait(false);
                    continue;
                }
            }

            QueryMetadataWriter.Apply(execution.Model, metadata);
        }
    }

    private static string PromoteRole(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.Role))
            return metadata.Role!;
        return metadata.Role ?? Role.Live.ToString();
    }

    private static string PromoteIdentifier(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.Identifier))
            return metadata.Identifier!;
        return metadata.Identifier ?? model.GetTopicName();
    }
}
