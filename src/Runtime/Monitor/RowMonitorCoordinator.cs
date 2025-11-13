using Ksql.Linq.Query.Metadata;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Monitor;

/// <summary>
/// Default coordinator that bridges to existing KsqlContext behavior.
/// Keeps semantics identical while providing an orchestration surface.
/// </summary>
public sealed class RowMonitorCoordinator : IRowMonitorCoordinator
{
    private readonly KsqlContext _context;

    public RowMonitorCoordinator(KsqlContext context)
    {
        _context = context;
    }

    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public Task StopAsync() => Task.CompletedTask;

    public Task StartForResults(System.Collections.Generic.IReadOnlyList<object> results, CancellationToken ct)
    {
        var typed = new System.Collections.Generic.List<Query.Analysis.DerivedTumblingPipeline.ExecutionResult>();
        foreach (var r in results)
        {
            if (r is Query.Analysis.DerivedTumblingPipeline.ExecutionResult er)
                typed.Add(er);
        }
        if (typed.Count == 0)
        {
            _context.Logger?.LogDebug("Row monitor coordinator received no execution results");
            return Task.CompletedTask;
        }

        // Initialize market schedule if needed (once)
        _context.InitializeMarketScheduleIfNeededAdapter(typed);

        foreach (var execution in typed)
        {
            var targetTopicName = _context.GetTopicNameAdapter(execution.Model);
            var metadata = execution.Model.GetOrCreateMetadata();
            var identifier = metadata.Identifier ?? targetTopicName;
            var roleMetadata = metadata.Role ?? "(unset)";
            var timeframe = metadata.TimeframeRaw ?? "(unset)";
            var extrasCount = metadata.Extras?.Count ?? 0;
            var isRowsRole = _context.IsRowsRoleAdapter(execution.Model);
            var hasQueryModel = execution.Model.QueryModel != null;

            _context.Logger?.LogInformation(
                "Row monitor candidate target={Target} identifier={Identifier} executionRole={ExecutionRole} roleMetadata={RoleMetadata} timeframe={Timeframe} extras={Extras} isRowsRole={IsRowsRole} hasQueryModel={HasQueryModel} statementType={StatementType} queryId={QueryId} responseSuccess={ResponseSuccess}",
                targetTopicName,
                identifier,
                execution.Role,
                roleMetadata,
                timeframe,
                extrasCount,
                isRowsRole,
                hasQueryModel,
                execution.Statement?.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? "(unknown)",
                execution.QueryId ?? "(none)",
                execution.Response.IsSuccess);

            if (!isRowsRole)
            {
                _context.Logger?.LogDebug("Row monitor prerequisites not met: identifier '{Identifier}' is not tagged as rows stream", identifier);
                continue;
            }

            var targetModel = _context.EnsureEntityModelAdapter(execution.Model.EntityType, execution.Model);
            var queryModel = _context.PopulateQueryModelIfMissingAdapter(targetModel, execution.Model.QueryModel);
            if (queryModel == null)
                continue;
            if (queryModel.SourceTypes.Length != 1)
            {
                _context.Logger?.LogWarning("Row monitor evaluation skipped: multiple sources not supported for {Target}", _context.GetTopicNameAdapter(targetModel));
                continue;
            }
            var sourceType = queryModel.SourceTypes[0];
            var sourceModel = _context.EnsureEntityModelAdapter(sourceType);

            // Ensure companion rows_last TABLE exists for this hub stream
            _context.EnsureRowsLastTableForSafeAsync(targetModel).GetAwaiter().GetResult();

            var monitor = _context.CreateRowMonitorAdapter(sourceModel, targetModel, queryModel);
            if (monitor == null)
            {
                _context.Logger?.LogWarning("Row monitor evaluation skipped: monitor creation failed for {Target}", _context.GetTopicNameAdapter(targetModel));
                continue;
            }

            var targetTopic = _context.GetTopicNameAdapter(targetModel);
            var sourceTopic = _context.GetTopicNameAdapter(sourceModel);
            var graceSeconds = queryModel.GraceSeconds ?? 0;
            var windowSummary = queryModel.Windows.Count > 0 ? string.Join(",", queryModel.Windows) : "-";
            var hasGroupBy = queryModel.GroupByExpression != null;
            var timeKey = queryModel.TimeKey ?? "(auto)";
            _context.Logger?.LogInformation(
                "Row monitor prerequisites satisfied target={Target} source={Source} identifier={Identifier} roleMetadata={RoleMetadata} graceSeconds={GraceSeconds}s windows={Windows} timeKey={TimeKey} hasGroupBy={HasGroupBy} extras={ExtrasCount}",
                targetTopic,
                sourceTopic,
                identifier,
                roleMetadata,
                graceSeconds,
                windowSummary,
                timeKey,
                hasGroupBy,
                queryModel.Extras.Count);

            _context.StartHubControllerAdapter(monitor, sourceTopic, targetTopic);
        }

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
