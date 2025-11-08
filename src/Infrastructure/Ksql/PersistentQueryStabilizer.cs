using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Query.Analysis;
using Microsoft.Extensions.Logging;

namespace Ksql.Linq.Infrastructure.Ksql;

internal sealed class PersistentQueryStabilizer
{
    private readonly KsqlQueryDdlMonitor.Dependencies _deps;
    private readonly Func<Type, string, Task<KsqlDbResponse>> _executeWithRetryAsync;

    public PersistentQueryStabilizer(
        KsqlQueryDdlMonitor.Dependencies deps,
        Func<Type, string, Task<KsqlDbResponse>> executeWithRetryAsync)
    {
        _deps = deps ?? throw new ArgumentNullException(nameof(deps));
        _executeWithRetryAsync = executeWithRetryAsync ?? throw new ArgumentNullException(nameof(executeWithRetryAsync));
    }

    public async Task StabilizeAsync(Type entityType, EntityModel tableModel, string ddl)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        if (tableModel == null) throw new ArgumentNullException(nameof(tableModel));
        if (string.IsNullOrWhiteSpace(ddl)) throw new ArgumentException("DDL must be provided.", nameof(ddl));

        var attempts = Math.Max(1, _deps.GetPersistentQueryMaxAttempts());
        var delay = TimeSpan.FromSeconds(5);
        Exception? lastError = null;

        for (var attempt = 0; attempt < attempts; attempt++)
        {
            _ = await _executeWithRetryAsync(entityType, ddl).ConfigureAwait(false);
            var persistent = await CollectPersistentExecutionsAsync(tableModel, ddl, entityType).ConfigureAwait(false);

            try
            {
                if (persistent.Count > 0)
                {
                    await _deps.StabilizePersistentQueriesAsync(
                        persistent,
                        tableModel,
                        _deps.GetPersistentQueryTimeout(),
                        CancellationToken.None).ConfigureAwait(false);
                }

                var queryHint = persistent.FirstOrDefault()?.QueryId;
                await _deps.WaitForQueryRunningAsync(
                    tableModel.GetTopicName(),
                    queryHint,
                    TimeSpan.FromSeconds(60)).ConfigureAwait(false);
                await _deps.AssertTopicPartitionsAsync(tableModel).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt + 1 < attempts)
            {
                lastError = ex;
                _deps.Logger?.LogWarning(
                    ex,
                    "Persistent query stabilization failed for {Entity} (attempt {Attempt}/{Max})",
                    entityType.Name,
                    attempt + 1,
                    attempts);
                await _deps.TerminateQueriesAsync(persistent).ConfigureAwait(false);
                await _deps.DelayAsync(delay, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                lastError = ex;
                break;
            }
        }

        throw lastError ?? new TimeoutException($"Persistent query for {entityType.Name} did not stabilize.");
    }

    private async Task<List<PersistentQueryExecution>> CollectPersistentExecutionsAsync(
        EntityModel tableModel,
        string ddl,
        Type entityType)
    {
        var list = new List<PersistentQueryExecution>();
        var topicName = tableModel.GetTopicName();
        var queryId = await _deps.TryGetQueryIdFromShowQueriesAsync(topicName, ddl).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(queryId))
        {
            list.Add(new PersistentQueryExecution(queryId, tableModel, topicName, ddl, null, false));
        }
        else if (ddl.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            _deps.Logger?.LogWarning(
                "Could not locate queryId via SHOW QUERIES for CTAS statement executed for {Entity}",
                entityType.Name);
        }

        return list;
    }
}