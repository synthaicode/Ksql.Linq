using System.Threading;
using System.Threading.Tasks;
using System;
using System.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Ddl;
using Microsoft.Extensions.Logging;
using Ksql.Linq.Events;

namespace Ksql.Linq.Runtime.Schema;

/// <summary>
/// Orchestrates schema registration and related readiness steps, leveraging context adapters.
/// </summary>
    public sealed class SchemaRegistrar : ISchemaRegistrar
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
                // Warmup ksqlDB before issuing DDL
                await WarmupKsqlWithTopicsOrFail(TimeSpan.FromSeconds(15), ct).ConfigureAwait(false);

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
                await _context.WaitForEntityDdlAdapterAsync(model, TimeSpan.FromSeconds(12)).ConfigureAwait(false);
                await _context.AlignDerivedMappingWithSchemaAdapterAsync(model).ConfigureAwait(false);
            }

            // Phase 3: DDL for query-defined entities・・egistrar 譛ｬ菴薙↓髮・ｴ・ｼ・
            await WarmupKsqlWithTopicsOrFail(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
            var allQ = _context.GetEntityModels().ToArray();
            foreach (var (type, model) in allQ.Where(e => e.Value.QueryModel != null))
            {
                ct.ThrowIfCancellationRequested();
                // Derived (tumbling) 縺ｯ繧｢繝繝励ち縺ｧ荳逋ｺ螳牙ｮ壼喧縺ｾ縺ｧ螳溯｡鯉ｼ・譯茨ｼ・
                if (model.QueryModel!.HasTumbling())
                {
                    await _context.EnsureDerivedQueryEntityDdlAdapterAsync(type, model).ConfigureAwait(false);
                    continue;
                }

                var isTable = model.QueryModel.DetermineType() == StreamTableType.Table;
                if (isTable)
                {
                    // TABLE邉ｻ: CTAS 繧堤函謌舌・螳溯｡後＠縲∝ｮ溯｡檎｢ｺ隱阪∪縺ｧ・域怙蟆丞ｮ牙ｮ壼喧・・
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
                    await RuntimeEventBus.PublishAsync(new RuntimeEvent
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
                        await RuntimeEventBus.PublishAsync(new RuntimeEvent
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
                        await RuntimeEventBus.PublishAsync(new RuntimeEvent
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
                    // STREAM邉ｻ: DDL逕滓・竊貞ｮ溯｡鯉ｼ域諺蜈･縺ｯ KsqlInsertStatementBuilder 縺ｫ蟋碑ｭｲ・・
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
            // tumbling 縺ｯ荳願ｨ倥Ν繝ｼ繝怜・縺ｧ A譯医・繧｢繝繝励ち邨檎罰縺ｧ螳御ｺ・ｸ医∩・亥､門・縺ｧ縺ｮ霑ｽ蜉蠕・ｩ溘・荳崎ｦ・ｼ・
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
        var attempts = 0;
        var configured = _context.GetKsqlDdlRetryCountAdapter();
        var maxAttempts = Math.Max(0, configured) + 1; // include first try
        var delayMs = Math.Max(0, _context.GetKsqlDdlRetryInitialDelayMsAdapter());
        if (maxAttempts <= 0) maxAttempts = 1;
        if (delayMs <= 0) delayMs = 500;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var result = await _context.ExecuteStatementAsync(sql).ConfigureAwait(false);
            if (result.IsSuccess)
                return result;
            attempts++;
            if (!IsRetryableKsqlError(result.Message))
                return result;
            if (attempts >= maxAttempts)
                return result;
            _context.Logger?.LogWarning("Retrying DDL due to: {Reason} (attempt {Attempt})", result.Message, attempts);
            try
            {
                await RuntimeEventBus.PublishAsync(new RuntimeEvent
                {
                    Name = "ddl.simple.retry",
                    Phase = "retry",
                    SqlPreview = sql,
                    Success = false,
                    Message = result.Message
                }, ct).ConfigureAwait(false);
            }
            catch { }
            try { await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false); } catch { }
            delayMs = Math.Min(delayMs * 2, 8000);
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
            try { await Task.Delay(500, ct).ConfigureAwait(false); } catch { }
        }
        throw new TimeoutException("ksqlDB warmup timed out (SHOW TOPICS did not succeed).");
    }
}

