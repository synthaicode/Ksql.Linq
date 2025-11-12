using System;
using System.Linq;
using System.Threading.Tasks;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Query.Analysis;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Query.Pipelines.Unified.Stages;

/// <summary>
/// Starts the row monitor once derived queries are running.
/// </summary>
internal sealed class RowMonitorStage : IUnifiedPipelineStage
{
    private readonly KsqlQueryDdlMonitor.Dependencies _deps;

    public RowMonitorStage(KsqlQueryDdlMonitor.Dependencies deps)
    {
        _deps = deps ?? throw new ArgumentNullException(nameof(deps));
    }

    public async Task ExecuteAsync(UnifiedPipelineContext context, UnifiedPipelineRequest request)
    {
        var executions = context.Executions?.ToArray() ?? Array.Empty<DerivedTumblingPipeline.ExecutionResult>();
        var rowCandidates = executions.Count(r => _deps.IsRowsRole(r.Model));
        _deps.Logger?.LogInformation("Row monitor preparation entity={Entity} rowCandidates={RowCandidates} totalExecutions={Executions}",
            context.EntityType.Name,
            rowCandidates,
            executions.Length);
        _deps.Logger?.LogInformation("Row monitor invocation entity={Entity} results={ResultsCount}",
            context.EntityType.Name,
            executions.Length);

        await _deps.StartRowMonitorAsync(executions).ConfigureAwait(false);
    }
}
