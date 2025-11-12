using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Events;
using Ksql.Linq.Infrastructure.KsqlDb;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Adapters;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Ddl;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Pipeline;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Ksql;

internal sealed class KsqlQueryDdlMonitor
{
    internal sealed record Dependencies
    {
        public required ILogger Logger { get; init; }
        public required KsqlDslOptions Options { get; init; }
        public required MappingRegistry MappingRegistry { get; init; }
        public required ConcurrentDictionary<Type, EntityModel> EntityModels { get; init; }
        public required IKsqlDbClient KsqlDbClient { get; init; }
        public required Func<string, Task<KsqlDbResponse>> ExecuteStatementAsync { get; init; }
        public required Action<EntityModel> RegisterQueryModelMapping { get; init; }
        public required Func<EntityModel, TimeSpan, Task> WaitForEntityDdlAsync { get; init; }
        public required Func<EntityModel, Task> AlignDerivedMappingWithSchemaAsync { get; init; }
        public required Func<IReadOnlyList<PersistentQueryExecution>, EntityModel?, TimeSpan, CancellationToken, Task> StabilizePersistentQueriesAsync { get; init; }
        public required Func<IReadOnlyList<DerivedTumblingPipeline.ExecutionResult>, Task> StartRowMonitorAsync { get; init; }
        public required Func<TimeSpan, Task> WaitForDerivedQueriesRunningAsync { get; init; }
        public required Func<TimeSpan, CancellationToken, Task> DelayAsync { get; init; }
        public required Func<Func<string, Task<KsqlDbResponse>>, Func<Task<HashSet<string>>>, Func<Task<HashSet<string>>>, EntityModel, Task> EnsureRowsLastTableAsync { get; init; }
        public required Func<string, string?, TimeSpan, Task> WaitForQueryRunningAsync { get; init; }
        public required Func<string, string?, Task<string?>> TryGetQueryIdFromShowQueriesAsync { get; init; }
        public required Func<IReadOnlyList<PersistentQueryExecution>, Task> TerminateQueriesAsync { get; init; }
        public required Func<EntityModel, Task> AssertTopicPartitionsAsync { get; init; }
        public required Func<RuntimeEvent, Task> PublishEventAsync { get; init; }
        public required Func<string, string?, TimeSpan, Task> WaitForPersistentQueryAsync { get; init; }
        public required Func<int> GetPersistentQueryMaxAttempts { get; init; }
        public required Func<TimeSpan> GetPersistentQueryTimeout { get; init; }
        public required Func<TimeSpan> GetQueryRunningTimeout { get; init; }
        public required Func<EntityModel, bool> IsRowsRole { get; init; }
    }

    private readonly Dependencies _deps;
    private readonly PersistentQueryStabilizer _persistentStabilizer;
    private readonly UnifiedPipelineRunner _pipelineRunner;

    public KsqlQueryDdlMonitor(Dependencies deps)
    {
        _deps = deps ?? throw new ArgumentNullException(nameof(deps));
        _persistentStabilizer = new PersistentQueryStabilizer(_deps, ExecuteWithRetryAsync);
        _pipelineRunner = new UnifiedPipelineRunner(_deps, ExecuteWithRetryAsync);
    }

    public async Task EnsureQueryEntityDdlAsync(Type entityType, EntityModel model)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        if (model == null) throw new ArgumentNullException(nameof(model));

        if (model.QueryModel != null)
            _deps.RegisterQueryModelMapping(model);

        if (model.QueryModel?.HasTumbling() == true)
        {
            await EnsureDerivedQueryEntityDdlAsync(entityType, model).ConfigureAwait(false);
            return;
        }

        var isTable = model.GetExplicitStreamTableType() == StreamTableType.Table ||
            model.QueryModel?.DetermineType() == StreamTableType.Table;
        if (model.QueryModel?.DetermineType() == StreamTableType.Table)
        {
            await EnsureTableQueryEntityDdlAsync(entityType, model).ConfigureAwait(false);
            return;
        }

        var generator = new DDLQueryGenerator();
        var adapter = new EntityModelDdlAdapter(model);
        var ddlSql = isTable
            ? generator.GenerateCreateTable(adapter)
            : generator.GenerateCreateStream(adapter);
        _deps.Logger?.LogInformation("KSQL DDL (query {Entity}): {Sql}", entityType.Name, ddlSql);
        _ = await ExecuteWithRetryAsync(entityType, ddlSql).ConfigureAwait(false);

        if (model.QueryModel != null)
        {
            Func<Type, string> resolver = type => ResolveSourceName(type);
            var insert = KsqlInsertStatementBuilder.Build(model.GetTopicName(), model.QueryModel, resolver);
            _deps.Logger?.LogInformation("KSQL DDL (query {Entity}): {Sql}", entityType.Name, insert);
            _ = await ExecuteWithRetryAsync(entityType, insert).ConfigureAwait(false);
        }
    }

    public Task EnsureDerivedQueryEntityDdlAsync(Type entityType, EntityModel baseModel)
    {
        return _pipelineRunner.ExecuteAsync(entityType, baseModel);
    }

    public Task EnsureTableQueryEntityDdlAsync(Type entityType, EntityModel tableModel)
    {
        return EnsureTableQueryEntityDdlCoreAsync(entityType, tableModel);
    }

    private async Task EnsureTableQueryEntityDdlCoreAsync(Type entityType, EntityModel tableModel)
    {
        Func<Type, string> resolver = type => ResolveSourceName(type);
        var ddl = KsqlCreateStatementBuilder.Build(
            tableModel.GetTopicName(),
            tableModel.QueryModel!,
            tableModel.KeySchemaFullName,
            tableModel.ValueSchemaFullName,
            resolver);

        _deps.Logger?.LogInformation("KSQL DDL (query {Entity}): {Sql}", entityType.Name, ddl);

        await _persistentStabilizer.StabilizeAsync(entityType, tableModel, ddl).ConfigureAwait(false);
    }

    private async Task<KsqlDbResponse> ExecuteWithRetryAsync(Type entityType, string sql)
    {
        var attempts = 0;
        var maxAttempts = Math.Max(0, _deps.Options.KsqlDdlRetryCount) + 1;
        var delayMs = Math.Max(0, _deps.Options.KsqlDdlRetryInitialDelayMs);

        while (true)
        {
            var result = await _deps.ExecuteStatementAsync(sql).ConfigureAwait(false);
            if (result.IsSuccess)
                return result;

            if (IsNonFatalCreateConflict(sql, result.Message))
                return new KsqlDbResponse(true, result.Message ?? string.Empty, result.ErrorCode, result.ErrorDetail);

            attempts++;
            var retryable = IsRetryableKsqlError(result.Message);
            if (!retryable || attempts >= maxAttempts)
            {
                var msg = $"DDL execution failed for {entityType.Name}: {result.Message}";
                _deps.Logger?.LogError(msg);
                throw new InvalidOperationException(msg);
            }

            _deps.Logger?.LogWarning("Retrying DDL for {Entity} (attempt {Attempt}/{Max}) due to: {Reason}", entityType.Name, attempts, maxAttempts - 1, result.Message);
            await _deps.DelayAsync(TimeSpan.FromMilliseconds(delayMs), CancellationToken.None).ConfigureAwait(false);
            delayMs = Math.Min(delayMs * 2, 8000);
        }
    }

    private string ResolveSourceName(Type? type)
    {
        var key = type?.Name ?? string.Empty;
        if (!string.IsNullOrEmpty(key)
            && _deps.Options.SourceNameOverrides is { Count: > 0 }
            && _deps.Options.SourceNameOverrides.TryGetValue(key, out var overrideName))
        {
            return overrideName;
        }

        if (type != null && _deps.EntityModels.TryGetValue(type, out var srcModel))
            return srcModel.GetTopicName().ToUpperInvariant();

        return key;
    }

    private static bool IsNonFatalCreateConflict(string sql, string? message)
    {
        if (string.IsNullOrWhiteSpace(sql) || string.IsNullOrWhiteSpace(message))
            return false;

        var trimmed = sql.TrimStart().ToUpperInvariant();
        if (!trimmed.StartsWith("CREATE ", StringComparison.Ordinal))
            return false;

        var msg = message.ToLowerInvariant();
        return msg.Contains("already exists");
    }

    private static bool IsRetryableKsqlError(string? message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return true;

        var m = message.ToLowerInvariant();
        return m.Contains("timeout while waiting for command topic")
            || m.Contains("could not write the statement")
            || m.Contains("failed to create new kafkaadminclient")
            || m.Contains("no resolvable bootstrap urls")
            || m.Contains("ksqldb server is not ready")
            || m.Contains("statement_error");
    }

}


