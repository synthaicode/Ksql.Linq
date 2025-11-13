using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.Query.Pipelines.Unified;
using Ksql.Linq.Query.Pipelines.Unified.Stages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Ksql;

/// <summary>
/// New unified pipeline entry point used by KsqlContext orchestration.
/// </summary>
internal sealed class UnifiedPipelineRunner
{
    private readonly KsqlQueryDdlMonitor.Dependencies _deps;
    private readonly Func<Type, string, Task<KsqlDbResponse>> _executeWithRetryAsync;
    private readonly UnifiedPipelineOrchestrator _orchestrator;

    public UnifiedPipelineRunner(
        KsqlQueryDdlMonitor.Dependencies deps,
        Func<Type, string, Task<KsqlDbResponse>> executeWithRetryAsync)
    {
        _deps = deps ?? throw new ArgumentNullException(nameof(deps));
        _executeWithRetryAsync = executeWithRetryAsync ?? throw new ArgumentNullException(nameof(executeWithRetryAsync));
        _orchestrator = new UnifiedPipelineOrchestrator(new IUnifiedPipelineStage[]
        {
            new RowsLastStage(_deps),
            new PersistentQueryStage(_deps),
            new RowMonitorStage(_deps)
        });
    }

    public async Task ExecuteAsync(Type entityType, EntityModel baseModel)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        if (baseModel == null) throw new ArgumentNullException(nameof(baseModel));

        var attempts = Math.Max(1, _deps.GetPersistentQueryMaxAttempts());
        var delay = TimeSpan.FromSeconds(5);
        Exception? lastError = null;

        var pipelineLogger = _deps.Logger ?? NullLogger.Instance;

        for (var attempt = 0; attempt < attempts; attempt++)
        {
            _deps.Logger?.LogInformation("Derived pipeline execution start entity={Entity} attempt={Attempt}/{Max}", entityType.Name, attempt + 1, attempts);

            var request = new UnifiedPipelineRequest(
                EntityType: entityType,
                BaseModel: baseModel,
                QueryModel: baseModel.QueryModel!,
                MappingRegistry: _deps.MappingRegistry,
                EntityModels: _deps.EntityModels,
                PipelineLogger: pipelineLogger,
                ExecuteDerivedAsync: ExecuteDerivedAsync,
                AfterExecution: null);

            try
            {
                var context = await _orchestrator.ExecuteAsync(request).ConfigureAwait(false);

                _deps.Logger?.LogInformation(
                    "Derived pipeline execution complete entity={Entity} attempt={Attempt}/{Max} results={ResultCount} persistent={PersistentCount}",
                    entityType.Name,
                    attempt + 1,
                    attempts,
                    context.Executions.Count,
                    context.PersistentExecutions.Count);

                return;
            }
            catch (UnifiedPipelineException ex) when (attempt + 1 < attempts)
            {
                lastError = ex.InnerException ?? ex;
                _deps.Logger?.LogWarning(lastError, "Derived query stabilization failed for {Entity} (attempt {Attempt}/{Max})", entityType.Name, attempt + 1, attempts);
                await _deps.TerminateQueriesAsync(ex.Context.PersistentExecutions).ConfigureAwait(false);
                await _deps.DelayAsync(delay, CancellationToken.None).ConfigureAwait(false);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 8000));
            }
            catch (UnifiedPipelineException ex)
            {
                lastError = ex.InnerException ?? ex;
                break;
            }
            catch (Exception ex)
            {
                lastError = ex;
                break;
            }
        }

        throw lastError ?? new TimeoutException($"Derived query for {entityType.Name} did not stabilize.");

        async Task<KsqlDbResponse> ExecuteDerivedAsync(EntityModel model, string sql)
        {
            try
            {
                var target = model.GetTopicName().ToLowerInvariant();
                var tables = await _deps.KsqlDbClient.GetTableTopicsAsync().ConfigureAwait(false);
                if (tables.Contains(target))
                {
                    var needRecreate = false;
                    try
                    {
                        var metadata = model.GetOrCreateMetadata();
                        var expectedGrace = PromoteGraceSeconds(model, ref metadata);
                        if (expectedGrace > 0)
                        {
                            var describe = await _deps.ExecuteStatementAsync($"DESCRIBE {model.GetTopicName().ToUpperInvariant()} EXTENDED;").ConfigureAwait(false);
                            if (describe.IsSuccess && !string.IsNullOrWhiteSpace(describe.Message))
                            {
                                var actualGrace = KsqlWaitService.ReadGraceSecondsFromDescribe(describe.Message);
                                if (actualGrace.HasValue && actualGrace.Value != expectedGrace)
                                {
                                    _deps.Logger?.LogInformation("Detected GRACE mismatch for {Target}: actual={Actual}s expected={Expected}s -> will recreate", target, actualGrace.Value, expectedGrace);
                                    needRecreate = true;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _deps.Logger?.LogDebug(ex, "Failed to verify GRACE; keeping existing by default");
                    }

                    if (!needRecreate)
                    {
                        _deps.Logger?.LogInformation("Derived DDL skipped (exists): {Target}", target);
                        return new KsqlDbResponse(true, $"SKIPPED (exists): {target}");
                    }

                    try
                    {
                        var queryId = await _deps.TryGetQueryIdFromShowQueriesAsync(model.GetTopicName(), sql).ConfigureAwait(false);
                        if (!string.IsNullOrWhiteSpace(queryId))
                        {
                            var terminate = await _deps.ExecuteStatementAsync($"TERMINATE {queryId};").ConfigureAwait(false);
                            _deps.Logger?.LogInformation("Terminate {QueryId} -> {Success}", queryId, terminate.IsSuccess);
                        }
                    }
                    catch (Exception ex)
                    {
                        _deps.Logger?.LogWarning(ex, "Failed to terminate existing query for {Target}", target);
                    }

                    try
                    {
                        var drop = await _deps.ExecuteStatementAsync($"DROP TABLE {model.GetTopicName().ToUpperInvariant()} DELETE TOPIC;").ConfigureAwait(false);
                        _deps.Logger?.LogInformation("Drop table {Target} -> {Success}", target, drop.IsSuccess);
                    }
                    catch (Exception ex)
                    {
                        _deps.Logger?.LogWarning(ex, "Failed to drop existing table {Target}", target);
                    }
                }

                return await _executeWithRetryAsync(model.EntityType, sql).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _deps.Logger?.LogError(ex, "Derived pipeline execution failed for model {Model}", model?.EntityType?.Name ?? "(unknown)");
                throw;
            }
        }

    }

    private async Task<KsqlDbResponse> ExecuteWithRetryAsync(Type entityType, string sql)
        => await _executeWithRetryAsync(entityType, sql).ConfigureAwait(false);

    private static int PromoteGraceSeconds(EntityModel model, ref QueryMetadata metadata)
    {
        _ = model;
        return metadata.GraceSeconds ?? 0;
    }
}
