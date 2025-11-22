using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Events;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Ddl;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Runtime.Schema;

/// <summary>
/// Orchestrates schema registration and related readiness steps, leveraging context adapters.
/// </summary>
internal sealed class SchemaRegistrar : ISchemaRegistrar
{
    private readonly KsqlContext _context;

    public SchemaRegistrar(KsqlContext context)
    {
        _context = context;
    }

    public async Task RegisterAndMaterializeAsync(CancellationToken ct = default)
    {
        using (Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            // Phase 0/1: dynamic rows injection occurs in context; then register schemas
            await _context.RegisterSchemasPhase1Async().ConfigureAwait(false);

            // Phase 2: Ensure DDL for simple entities with warmup + retry handled here
            var all = _context.GetEntityModels().ToArray();
            var simple = all.Where(e => e.Value.QueryModel == null && e.Value.QueryExpression == null).ToArray();
            foreach (var (type, model) in simple)
            {
                ct.ThrowIfCancellationRequested();
                // Apply topic structure settings from options and ensure topic exists
                _context.ApplyTopicCreationSettingsAdapter(model);
                await _context.CreateDbTopicAdapterAsync(model.GetTopicName(), model.Partitions, model.ReplicationFactor).ConfigureAwait(false);
                // Warmup ksqlDB before issuing DDL (simple entities)
                await WarmupKsqlWithTopicsOrFail(_context.GetSimpleEntityWarmupAdapter(), ct).ConfigureAwait(false);

                // Generate DDL and execute with retry
                var generator = new Ksql.Linq.Query.Pipeline.DDLQueryGenerator();
                var schemaProvider = new EntityModelDdlAdapter(model);
                var ddl = model.StreamTableType == StreamTableType.Table
                    ? generator.GenerateCreateTable(schemaProvider)
                    : generator.GenerateCreateStream(schemaProvider);
                _context.Logger?.LogInformation("KSQL DDL (simple {Entity}): {Sql}", type.Name, ddl);
                var res = await ExecuteWithRetryAsync(ddl, ct).ConfigureAwait(false);
                if (!res.IsSuccess)
                    throw new InvalidOperationException($"DDL execution failed for {type.Name}: {res.Message}");
                // Ensure metadata visibility then align mapping
                await _context.WaitForEntityDdlAdapterAsync(model, _context.GetEntityDdlVisibilityTimeoutAdapter()).ConfigureAwait(false);
                await _context.AlignDerivedMappingWithSchemaAdapterAsync(model).ConfigureAwait(false);
            }

            // Phase 3: DDL for query-defined entities（Registrar 本体に集約）
            await WarmupKsqlWithTopicsOrFail(_context.GetQueryEntityWarmupAdapter(), ct).ConfigureAwait(false);
            var allQ = _context.GetEntityModels().ToArray();
            foreach (var (type, model) in allQ.Where(e => e.Value.QueryModel != null))
            {
                ct.ThrowIfCancellationRequested();
                // Hopping: generate専用CTASのみ（通常CTASは衝突するのでスキップ）
                if (model.QueryModel!.HasHopping())
                {
                    var timeframe = model.QueryModel.Windows.FirstOrDefault() ?? "5m";
                    var hopInterval = model.QueryModel.HopInterval ?? TimeSpan.FromMinutes(1);
                    var period = timeframe.EndsWith("h", StringComparison.OrdinalIgnoreCase)
                        ? Ksql.Linq.Runtime.Period.Hours(int.TryParse(timeframe[..^1], out var h) ? h : 1)
                        : timeframe.EndsWith("d", StringComparison.OrdinalIgnoreCase)
                            ? Ksql.Linq.Runtime.Period.Days(int.TryParse(timeframe[..^1], out var d) ? d : 1)
                            : Ksql.Linq.Runtime.Period.Minutes(int.TryParse(timeframe[..^1], out var m) ? m : 1);
                    // Align sink topic with TimeBucket hopping naming so cache lookup succeeds.
                    var sinkTopic = Ksql.Linq.Runtime.TimeBucketTypes.GetHoppingLiveTopicName(type, period, hopInterval);
                    model.TopicName = sinkTopic;

                    // Set metadata for streamiz cache integration (same as tumbling)
                    var md = model.GetOrCreateMetadata();
                    model.SetMetadata(md with
                    {
                        Role = "Live",
                        TimeframeRaw = timeframe
                    });

                    var ddl = KsqlCreateWindowedStatementBuilder.Build(
                        name: model.GetTopicName(),
                        model: model.QueryModel!,
                        timeframe: timeframe,
                        emitOverride: "EMIT CHANGES",
                        inputOverride: null,
                        options: null,
                        hopInterval: hopInterval,
                        // Hopping は windowed key を ksql に生成させるため keySchemaFullName は渡さない
                        keySchemaFullName: null,
                        valueSchemaFullName: model.ValueSchemaFullName);
                    _context.Logger?.LogInformation("KSQL DDL (hopping {Entity}): {Sql}", type.Name, ddl);
                    var _ = await ExecuteWithRetryAsync(ddl, ct).ConfigureAwait(false);
                    var queryId = await _context.TryGetQueryIdFromShowQueriesAdapterAsync(model.GetTopicName(), ddl).ConfigureAwait(false);
                    await _context.WaitForQueryRunningAdapterAsync(model.GetTopicName(), TimeSpan.FromSeconds(60), queryId).ConfigureAwait(false);
                    await _context.AssertTopicPartitionsAdapterAsync(model).ConfigureAwait(false);
                    continue;
                }
                // Derived (tumbling) はアダプタで一発安定化まで実行（A案）
                if (model.QueryModel!.HasTumbling())
                {
                    await _context.EnsureDerivedQueryEntityDdlAdapterAsync(type, model).ConfigureAwait(false);
                    continue;
                }

                var isTable = model.QueryModel.DetermineType() == StreamTableType.Table;
                if (isTable)
                {
                    // TABLE系: CTAS を生成・実行し、実行確認まで（最小安定化）
                    Func<Type, string> resolver = t =>
                    {
                        var key = t?.Name ?? string.Empty;
                        var models = _context.GetEntityModels();
                        if (t != null && models.TryGetValue(t, out var srcModel))
                            return srcModel.GetTopicName().ToUpperInvariant();
                        return key;
                    };
                    var ddl = KsqlCreateStatementBuilder.Build(
                            model.GetTopicName(),
                            model.QueryModel!,
                            model.KeySchemaFullName,
                            model.ValueSchemaFullName,
                            resolver);
                    _context.Logger?.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, ddl);
                    var _ = await ExecuteWithRetryAsync(ddl, ct).ConfigureAwait(false);
                    var queryId = await _context.TryGetQueryIdFromShowQueriesAdapterAsync(model.GetTopicName(), ddl).ConfigureAwait(false);
                    try
                    {
                        await RuntimeEvents.TryPublishAsync(new RuntimeEvent
                        {
                            Name = "query.run",
                            Phase = "start",
                            Entity = type.Name,
                            Topic = model.GetTopicName(),
                            QueryId = queryId,
                            SqlPreview = ddl
                        }, ct).ConfigureAwait(false);
                    }
                    catch { }
                    try
                    {
                        await _context.WaitForQueryRunningAdapterAsync(model.GetTopicName(), TimeSpan.FromSeconds(60), queryId).ConfigureAwait(false);
                        try
                        {
                            await RuntimeEvents.TryPublishAsync(new RuntimeEvent
                            {
                                Name = "query.run",
                                Phase = "done",
                                Entity = type.Name,
                                Topic = model.GetTopicName(),
                                QueryId = queryId,
                                Success = true
                            }, ct).ConfigureAwait(false);
                        }
                        catch { }
                    }
                    catch (Exception ex)
                    {
                        try
                        {
                            await RuntimeEvents.TryPublishAsync(new RuntimeEvent
                            {
                                Name = "query.run",
                                Phase = "timeout",
                                Entity = type.Name,
                                Topic = model.GetTopicName(),
                                QueryId = queryId,
                                Success = false,
                                Message = ex.Message,
                                Exception = ex
                            }, ct).ConfigureAwait(false);
                        }
                        catch { }
                        throw;
                    }
                    await _context.AssertTopicPartitionsAdapterAsync(model).ConfigureAwait(false);
                }
                else
                {
                    // STREAM系: DDL生成→実行（挿入は KsqlInsertStatementBuilder に委譲）
                    var generator = new Ksql.Linq.Query.Pipeline.DDLQueryGenerator();
                    var ddl = generator.GenerateCreateStream(new EntityModelDdlAdapter(model));
                    _context.Logger?.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, ddl);
                    var res = await ExecuteWithRetryAsync(ddl, ct).ConfigureAwait(false);
                    if (!res.IsSuccess)
                        throw new InvalidOperationException($"DDL execution failed for {type.Name}: {res.Message}");

                    Func<Type, string> resolver = t =>
                    {
                        var key = t?.Name ?? string.Empty;
                        var models = _context.GetEntityModels();
                        if (t != null && models.TryGetValue(t, out var srcModel))
                            return srcModel.GetTopicName().ToUpperInvariant();
                        return key;
                    };
                    var insert = KsqlInsertStatementBuilder.Build(model.GetTopicName(), model.QueryModel!, resolver);
                    _context.Logger?.LogInformation("KSQL DDL (query {Entity}): {Sql}", type.Name, insert);
                    await ExecuteWithRetryAsync(insert, ct).ConfigureAwait(false);
                }
            }
            // tumbling は上記ループ内で A案のアダプタ経由で完了済み（外側での追加待機は不要）
        }
        // Cache eligibility registration
        var tableTopics = await _context.GetTableTopicsAsyncAdapter().ConfigureAwait(false);
        _context.RegisterEligibleTablesForCache(tableTopics);
        // Connectivity + readiness
        _context.ValidateKafkaConnectivityAdapter();
        await _context.EnsureKafkaReadyAdapterAsync().ConfigureAwait(false);
    }

    private async Task<Ksql.Linq.KsqlDbResponse> ExecuteWithRetryAsync(string sql, CancellationToken ct)
    {
        var maxAttempts = Math.Max(0, _context.GetKsqlDdlRetryCountAdapter()) + 1;
        var initialDelay = TimeSpan.FromMilliseconds(Math.Max(0, _context.GetKsqlDdlRetryInitialDelayMsAdapter()));
        if (maxAttempts <= 0) maxAttempts = 1;
        if (initialDelay == TimeSpan.Zero) initialDelay = TimeSpan.FromMilliseconds(500);

        var policy = new Ksql.Linq.Core.Retry.RetryPolicy
        {
            MaxAttempts = maxAttempts,
            InitialDelay = initialDelay,
            Strategy = Ksql.Linq.Core.Retry.BackoffStrategy.Exponential,
            IsRetryable = ex => IsRetryableKsqlError(ex.Message)
        };

        try
        {
            return await policy.ExecuteAsync(async () =>
            {
                ct.ThrowIfCancellationRequested();
                var res = await _context.ExecuteStatementAsync(sql).ConfigureAwait(false);
                if (res.IsSuccess)
                    return res;
                // Non-success: throw with message so policy can decide
                throw new InvalidOperationException(res.Message ?? "DDL execution failed");
            }, ct, (attempt, ex) =>
            {
                _context.Logger?.LogWarning("Retrying DDL due to: {Reason} (attempt {Attempt})", ex.Message, attempt);
                RuntimeEvents.TryPublishFireAndForget(new RuntimeEvent
                {
                    Name = "ddl.simple.retry",
                    Phase = "retry",
                    SqlPreview = sql,
                    Success = false,
                    Message = ex.Message
                }, ct);
            }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Final failure after retries: return an error response-like result for consistency
            _context.Logger?.LogError(ex, "DDL execution failed after retries.");
            return new Ksql.Linq.KsqlDbResponse(false, ex.Message);
        }
    }

    private static bool IsRetryableKsqlError(string? message)
    {
        if (string.IsNullOrWhiteSpace(message)) return true;
        var m = message.ToLowerInvariant();
        return m.Contains("timeout while waiting for command topic")
            || m.Contains("could not write the statement")
            || m.Contains("failed to create new kafkaadminclient")
            || m.Contains("no resolvable bootstrap urls")
            || m.Contains("ksqldb server is not ready")
            || m.Contains("statement_error");
    }

    private async Task WarmupKsqlWithTopicsOrFail(TimeSpan timeout, CancellationToken ct)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            try
            {
                var res = await _context.ExecuteStatementAsync("SHOW TOPICS;").ConfigureAwait(false);
                if (res.IsSuccess)
                {
                    try { await _context.ExecuteStatementAsync("SHOW TOPICS;").ConfigureAwait(false); } catch { }
                    return;
                }
            }
            catch { }
            await Ksql.Linq.Core.Async.SafeDelay.Milliseconds(500, ct).ConfigureAwait(false);
        }
        throw new TimeoutException("ksqlDB warmup timed out (SHOW TOPICS did not succeed).");
    }
}
