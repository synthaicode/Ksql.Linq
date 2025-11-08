using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Query.Analysis;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Query.Pipelines.Unified.Stages;

/// <summary>
/// Collects persistent query executions and stabilizes them before proceeding.
/// </summary>
internal sealed class PersistentQueryStage : IUnifiedPipelineStage
{
    private readonly KsqlQueryDdlMonitor.Dependencies _deps;

    public PersistentQueryStage(KsqlQueryDdlMonitor.Dependencies deps)
    {
        _deps = deps ?? throw new ArgumentNullException(nameof(deps));
    }

    public async Task ExecuteAsync(UnifiedPipelineContext context, UnifiedPipelineRequest request)
    {
        var persistent = await CollectPersistentExecutionsAsync(context.Executions).ConfigureAwait(false);
        context.ReplacePersistentExecutions(persistent);

        if (persistent.Count > 0)
        {
            await _deps.StabilizePersistentQueriesAsync(
                persistent,
                context.BaseModel,
                _deps.GetPersistentQueryTimeout(),
                CancellationToken.None).ConfigureAwait(false);
        }

        await _deps.WaitForDerivedQueriesRunningAsync(_deps.GetQueryRunningTimeout()).ConfigureAwait(false);
    }

    private async Task<List<PersistentQueryExecution>> CollectPersistentExecutionsAsync(IReadOnlyList<DerivedTumblingPipeline.ExecutionResult> executions)
    {
        var list = new List<PersistentQueryExecution>();
        if (executions == null || executions.Count == 0)
            return list;

        foreach (var execution in executions)
        {
            if (!execution.IsPersistentQuery)
                continue;

            if (!string.IsNullOrWhiteSpace(execution.InputTopic) && execution.InputTopic.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase))
                continue;

            var topicName = execution.Model.GetTopicName();
            var queryId = !string.IsNullOrWhiteSpace(execution.QueryId)
                ? execution.QueryId
                : await _deps.TryGetQueryIdFromShowQueriesAsync(topicName, execution.Statement).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(queryId))
            {
                list.Add(new PersistentQueryExecution(queryId!, execution.Model, topicName, execution.Statement, execution.InputTopic, true));
            }
            else
            {
                _deps.Logger?.LogWarning("Could not locate queryId via SHOW QUERIES for derived statement targeting {Topic}: {Statement}", topicName, execution.Statement);
            }
        }

        return list;
    }
}
