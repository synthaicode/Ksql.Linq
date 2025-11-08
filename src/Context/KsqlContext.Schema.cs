using Avro;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Mapping;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Query.Adapters;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.Query.Ddl;
using Ksql.Linq.SchemaRegistryTools;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading;
using System.Linq.Expressions;
using System.Text.Json;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;
using Ksql.Linq.Events;
namespace Ksql.Linq;
public abstract partial class KsqlContext
{
    // Internal adapters to enable SchemaRegistrar ownership without exposing internals broadly
    internal Task RegisterSchemasAndMaterializeCoreAsync()
        => RegisterSchemasAndMaterializeAsync();
    internal Task RegisterSchemasPhase1Async()
        => RegisterSchemasForAllEntitiesAsync();
    internal Task EnsureSimpleDdlPhase2Async()
        => EnsureDdlForSimpleEntitiesAsync();
    internal Task EnsureQueryDdlPhase3Async()
        => EnsureDdlForQueryEntitiesAsync();
    internal Task<HashSet<string>> GetTableTopicsAsyncAdapter()
        => _ksqlDbClient.GetTableTopicsAsync();
    internal void RegisterEligibleTablesForCache(HashSet<string> tableTopics)
        => _cacheRegistry?.RegisterEligibleTables(_entityModels.Values, tableTopics);
    internal void ValidateKafkaConnectivityAdapter()
        => ValidateKafkaConnectivity();
    internal Task EnsureKafkaReadyAdapterAsync()
        => EnsureKafkaReadyAsync();
    internal Task WaitForKsqlReadyAdapterAsync(TimeSpan timeout)
        => KsqlPersistentQueryMonitor.WaitForKsqlReadyAsync(sql => ExecuteStatementAsync(sql), timeout);
    internal Task WaitForEntityDdlAdapterAsync(EntityModel model, TimeSpan timeout)
        => WaitForEntityDdlAsync(model, timeout);
    internal Task AlignDerivedMappingWithSchemaAdapterAsync(EntityModel model)
        => AlignDerivedMappingWithSchemaAsync(model);
    internal Task CreateDbTopicAdapterAsync(string topic, int partitions, short replicationFactor)
        => _adminService.CreateDbTopicAsync(topic, partitions, replicationFactor);
    internal void ApplyTopicCreationSettingsAdapter(EntityModel model)
    {
        var topic = model.GetTopicName();
        if (_dslOptions.Topics.TryGetValue(topic, out var config) && config?.Creation != null)
        {
            model.Partitions = config.Creation.NumPartitions;
            model.ReplicationFactor = config.Creation.ReplicationFactor;
        }
        if (model.Partitions <= 0) model.Partitions = 1;
        if (model.ReplicationFactor <= 0) model.ReplicationFactor = 1;
    }
    internal Task<string?> TryGetQueryIdFromShowQueriesAdapterAsync(string targetTopic, string? statement, int attempts = 5, int delayMs = 1000)
        => KsqlWaitClient.TryGetQueryIdFromShowQueriesAsync(sql => ExecuteStatementAsync(sql), targetTopic, statement, attempts, delayMs);
    internal Task WaitForQueryRunningAdapterAsync(string targetEntityName, TimeSpan timeout, string? queryId = null)
        => KsqlWaitClient.WaitForQueryRunningAsync(sql => ExecuteStatementAsync(sql), targetEntityName, queryId, timeout);
    internal Task AssertTopicPartitionsAdapterAsync(EntityModel model)
        => AssertTopicPartitionsAsync(model);
    private Task AssertTopicPartitionsAsync(EntityModel model)
        => KsqlPersistentQueryMonitor.AssertTopicPartitionsAsync(
            sql => ExecuteStatementAsync(sql),
            model);
    internal Task WaitForDerivedQueriesRunningAdapterAsync(TimeSpan timeout)
        => WaitForDerivedQueriesRunningAsync(timeout);
    internal TimeSpan GetQueryRunningTimeoutAdapter()
        => GetQueryRunningTimeout();
    internal int GetKsqlDdlRetryCountAdapter()
        => Math.Max(0, _dslOptions.KsqlDdlRetryCount);
    internal int GetKsqlDdlRetryInitialDelayMsAdapter()
        => Math.Max(0, _dslOptions.KsqlDdlRetryInitialDelayMs);

    // Adapters for stabilization/termination using a public-friendly tuple shape
    internal Task StabilizePersistentQueriesAdapterAsync(
        System.Collections.Generic.IReadOnlyList<(string QueryId, EntityModel TargetModel, string TargetTopic, string Statement, string? InputTopic, bool IsDerived)> executions,
        EntityModel? baseModel,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var list = new System.Collections.Generic.List<PersistentQueryExecution>(executions?.Count ?? 0);
        if (executions != null)
        {
            foreach (var e in executions)
            {
                list.Add(new PersistentQueryExecution(e.QueryId, e.TargetModel, e.TargetTopic, e.Statement, e.InputTopic, e.IsDerived));
            }
        }
        return StabilizePersistentQueriesAsync(list, baseModel, timeout, cancellationToken);
    }

    internal Task TerminateQueriesAdapterAsync(
        System.Collections.Generic.IReadOnlyList<(string QueryId, EntityModel TargetModel, string TargetTopic, string Statement, string? InputTopic, bool IsDerived)> executions)
    {
        var list = new System.Collections.Generic.List<PersistentQueryExecution>(executions?.Count ?? 0);
        if (executions != null)
        {
            foreach (var e in executions)
            {
                list.Add(new PersistentQueryExecution(e.QueryId, e.TargetModel, e.TargetTopic, e.Statement, e.InputTopic, e.IsDerived));
            }
        }
        return KsqlPersistentQueryMonitor.TerminateQueriesAsync(
            sql => ExecuteStatementAsync(sql),
            Logger,
            list);
    }

    // A案: tumbling（Derived）の安定化を単発で実行するアダプタ
    internal Task EnsureDerivedQueryEntityDdlAdapterAsync(Type entityType, EntityModel baseModel)
    {
        if (_queryDdlMonitor == null)
            throw new InvalidOperationException("Query DDL monitor not initialized.");

        return _queryDdlMonitor.EnsureDerivedQueryEntityDdlAsync(entityType, baseModel);
    }
    /// <summary>
    /// Internal hook for refactor: expose schema registration as a single callable step.
    /// Non-breaking wrapper around existing initialization sequence.
    /// </summary>
    internal void RunSchemaRegistrationAdapter()
    {
        InitializeWithSchemaRegistration();
    }
    private void InitializeWithSchemaRegistration()
    {
        // Register schemas and materialize entities if new
        using (Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            RegisterSchemasAndMaterializeAsync().GetAwaiter().GetResult();
        }
        var tableTopics = _ksqlDbClient.GetTableTopicsAsync().GetAwaiter().GetResult();
        _cacheRegistry?.RegisterEligibleTables(_entityModels.Values, tableTopics);
        // Verify Kafka connectivity
        ValidateKafkaConnectivity();
        EnsureKafkaReadyAsync().GetAwaiter().GetResult();
    }
    private async Task EnsureKafkaReadyAsync()
    {
        try
        {
            if (_topicAdmin != null)
            {
                await _topicAdmin.EnsureDlqTopicExistsAsync();
                if (_dslOptions.Fill?.EnableAppSide == true && !string.IsNullOrWhiteSpace(_dslOptions.Fill?.StateTopicName))
                {
                    await _topicAdmin.EnsureCompactedTopicAsync(_dslOptions.Fill!.StateTopicName!);
                }
                _topicAdmin.ValidateKafkaConnectivity();
            }
            else
            {
                // Auto-create DLQ topic
                await _adminService.EnsureDlqTopicExistsAsync();
                // Ensure compacted fill-state topic if app-side fill is enabled
                if (_dslOptions.Fill?.EnableAppSide == true && !string.IsNullOrWhiteSpace(_dslOptions.Fill?.StateTopicName))
                {
                    await _adminService.EnsureCompactedTopicAsync(_dslOptions.Fill!.StateTopicName!);
                }
                // Additional connectivity check (performed by AdminService)
                _adminService.ValidateKafkaConnectivity();
            }
            // Log output: DLQ preparation complete
            Logger.LogInformation(
                "Kafka initialization completed; DLQ topic '{Topic}' ready with {Retention}ms retention",
                GetDlqTopicName(),
                _dslOptions.DlqOptions.RetentionMs);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "FATAL: Kafka readiness check failed. DLQ functionality may be unavailable.", ex);
        }
    }
    public string GetDlqTopicName()
    {
        return _dslOptions.DlqTopicName;
    }
    /// <summary>
    /// Kafka connectivity validation.
    /// </summary>
    private void ValidateKafkaConnectivity()
    {
        try
        {
            // Kafka connectivity is verified during Producer/Consumer initialization.
            // No additional checks needed beyond the existing startup sequence.
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "FATAL: Cannot connect to Kafka. Verify bootstrap servers and network connectivity.", ex);
        }
    }
    /// <summary>
    /// Register schemas for all entities and send dummy record if newly created
    /// </summary>
    private async Task RegisterSchemasAndMaterializeAsync()
    {
        // Inject simple 1s rows entities so they go through the standard simple-entity flow
        InjectRowsSimpleEntities();
        var client = _schemaRegistryClient.Value;
        // Pass 1: Register schemas for all entities
        await RegisterSchemasForAllEntitiesAsync();
        // Pass 2: Ensure DDL for simple entities first
        await EnsureDdlForSimpleEntitiesAsync();
        // Pass 3: Then ensure DDL for query-defined entities
        await EnsureDdlForQueryEntitiesAsync();
    }

    // Pass 1: Register schemas for all entities
    private async Task RegisterSchemasForAllEntitiesAsync()
    {
        var client = _schemaRegistryClient.Value;
        var entities = _entityModels.ToArray();
        var schemaResults = new Dictionary<Type, SchemaRegistrationResult>();
        foreach (var (type, model) in entities)
        {
            try
            {
                var mapping = _mappingRegistry.GetMapping(type);
                if (model.QueryModel == null && model.QueryExpression == null && model.HasKeys() && mapping.AvroKeySchema != null)
                {
                    var keySubject = $"{model.GetTopicName()}-key";
                    await client.RegisterSchemaIfNewAsync(keySubject, mapping.AvroKeySchema);
                    var keySchema = Avro.Schema.Parse(mapping.AvroKeySchema);
                    model.KeySchemaFullName = keySchema.Fullname;
                }
                var valueSubject = $"{model.GetTopicName()}-value";
                var valueResult = await client.RegisterSchemaIfNewAsync(valueSubject, mapping.AvroValueSchema!);
                var valueSchema = Avro.Schema.Parse(mapping.AvroValueSchema!);
                if (mapping.AvroValueType == typeof(Avro.Generic.GenericRecord))
                {
                    model.ValueSchemaFullName = null;
                }
                else
                {
                    model.ValueSchemaFullName = valueSchema.Fullname;
                }
                schemaResults[type] = valueResult;
                DecimalSchemaValidator.Validate(model, client, ValidationMode.Strict, Logger);
                try
                {
                    await Ksql.Linq.Events.RuntimeEventBus.PublishAsync(new Ksql.Linq.Events.RuntimeEvent
                    {
                        Name = "schema.register",
                        Phase = "done",
                        Entity = type.Name,
                        Topic = model.GetTopicName(),
                        Success = true,
                        Message = $"subjects {model.GetTopicName()}-key,{model.GetTopicName()}-value registered"
                    }).ConfigureAwait(false);
                }
                catch { }
            }
            catch (ConfluentSchemaRegistry.SchemaRegistryException ex)
            {
                Logger.LogError(ex, "Schema registration failed for {Entity}", type.Name);
                try
                {
                    await Ksql.Linq.Events.RuntimeEventBus.PublishAsync(new Ksql.Linq.Events.RuntimeEvent
                    {
                        Name = "schema.register",
                        Phase = "fail",
                        Entity = type.Name,
                        Topic = model.GetTopicName(),
                        Success = false,
                        Message = ex.Message,
                        Exception = ex
                    }).ConfigureAwait(false);
                }
                catch { }
                throw;
            }
        }
    }

    // Pass 2: Ensure DDL for simple entities
    private async Task EnsureDdlForSimpleEntitiesAsync()
    {
        var entities = _entityModels.ToArray();
        foreach (var (type, model) in entities.Where(e => e.Value.QueryModel == null && e.Value.QueryExpression == null))
        {
            await EnsureSimpleEntityDdlAsync(type, model);
        }
    }

    // Pass 3: Ensure DDL for query-defined entities
    private async Task EnsureDdlForQueryEntitiesAsync()
    {
        var entities = _entityModels.ToArray();
        foreach (var (type, model) in entities.Where(e => e.Value.QueryModel != null || e.Value.QueryExpression != null))
        {
            await EnsureQueryEntityDdlAsync(type, model);
        }
    }
    /// <summary>
    /// Create topics and ksqlDB objects for an entity defined without queries.
    /// </summary>
    private async Task EnsureSimpleEntityDdlAsync(Type type, EntityModel model)
    {
        var generator = new Ksql.Linq.Query.Pipeline.DDLQueryGenerator();
        var topic = model.GetTopicName();
        if (_dslOptions.Topics.TryGetValue(topic, out var config) && config.Creation != null)
        {
            model.Partitions = config.Creation.NumPartitions;
            model.ReplicationFactor = config.Creation.ReplicationFactor;
        }
        // Fallback defaults for simple entity without explicit topic settings
        if (model.Partitions <= 0)
            model.Partitions = 1;
        if (model.ReplicationFactor <= 0)
            model.ReplicationFactor = 1;
        await _adminService.CreateDbTopicAsync(topic, model.Partitions, model.ReplicationFactor);
        // Wait for ksqlDB readiness briefly before issuing DDL
        await KsqlPersistentQueryMonitor.WaitForKsqlReadyAsync(
            sql => ExecuteStatementAsync(sql),
            TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        string ddl;
        var schemaProvider = new Query.Ddl.EntityModelDdlAdapter(model);
        ddl = model.StreamTableType == StreamTableType.Table
            ? generator.GenerateCreateTable(schemaProvider)
            : generator.GenerateCreateStream(schemaProvider);
        Logger.LogInformation("KSQL DDL (simple {Entity}): {Sql}", type.Name, ddl);
        // Retry DDL with exponential backoff to tolerate ksqlDB command-topic warmup
        var attempts = 0;
        var maxAttempts = Math.Max(0, _dslOptions.KsqlDdlRetryCount) + 1; // include first try
        var delayMs = Math.Max(0, _dslOptions.KsqlDdlRetryInitialDelayMs);
        while (true)
        {
            var result = await ExecuteStatementAsync(ddl);
            if (result.IsSuccess)
                break;
            attempts++;
            var retryable = IsRetryableKsqlError(result.Message);
            if (!retryable || attempts >= maxAttempts)
            {
                var msg = $"DDL execution failed for {type.Name}: {result.Message}";
                Logger.LogError(msg);
                try
                {
                    await Ksql.Linq.Events.RuntimeEventBus.PublishAsync(new Ksql.Linq.Events.RuntimeEvent
                    {
                        Name = "ddl.simple.fail",
                        Phase = "fail",
                        Entity = type.Name,
                        Topic = model.GetTopicName(),
                        SqlPreview = Preview(ddl),
                        Success = false,
                        Message = result.Message
                    }).ConfigureAwait(false);
                }
                catch { }
                throw new InvalidOperationException(msg);
            }
            Logger.LogWarning("Retrying DDL for {Entity} (attempt {Attempt}/{Max}) due to: {Reason}", type.Name, attempts, maxAttempts - 1, result.Message);
            try
            {
                await Ksql.Linq.Events.RuntimeEventBus.PublishAsync(new Ksql.Linq.Events.RuntimeEvent
                {
                    Name = "ddl.simple.retry",
                    Phase = "retry",
                    Entity = type.Name,
                    Topic = model.GetTopicName(),
                    SqlPreview = Preview(ddl),
                    Success = false,
                    Message = result.Message
                }).ConfigureAwait(false);
            }
            catch { }
            await _delay(TimeSpan.FromMilliseconds(delayMs), default);
            delayMs = Math.Min(delayMs * 2, 8000);
        }
        // Ensure the entity is visible to ksqlDB metadata before proceeding
        await WaitForEntityDdlAsync(model, TimeSpan.FromSeconds(12));
        // Align simple entity mapping (especially key schema) with existing SR subjects
        await AlignDerivedMappingWithSchemaAsync(model).ConfigureAwait(false);
    }

    private void InjectRowsSimpleEntities()
    {
        try
        {
            var baseEntities = _entityModels.Values.Where(m => m.QueryModel != null).ToList();
            foreach (var baseModel in baseEntities)
            {
                var windows = baseModel.QueryModel?.Windows ?? new List<string>();
                if (!windows.Any(w => w.Equals("1s", StringComparison.OrdinalIgnoreCase)))
                    continue;

                var baseMetadata = baseModel.GetOrCreateMetadata();
                var baseName = GetTopicName(baseModel);
                var rowsTopic = $"{baseName}_1s_rows";
                if (_entityModels.Values.Any(m => string.Equals(m.GetTopicName(), rowsTopic, StringComparison.OrdinalIgnoreCase)))
                    continue; // already present (either derived or injected)

                // Build dynamic type with keys inferred from base model metadata
                var ns = $"runtime_{baseName}_ksql";
                string[] keyNames = Array.Empty<string>();
                try
                {
                    if (baseMetadata.Keys.Names is { Length: > 0 } metaKeys)
                        keyNames = metaKeys.Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
                    else if (baseModel.KeyProperties != null && baseModel.KeyProperties.Length > 0)
                        keyNames = baseModel.KeyProperties.Select(p => p.Name).ToArray();
                }
                catch { }
                // Ensure bucket column is included
                var bucket = baseModel.QueryModel?.BucketColumnName;
                if (string.IsNullOrWhiteSpace(bucket)) bucket = "BucketStart";
                if (!keyNames.Any(k => string.Equals(k, bucket, StringComparison.OrdinalIgnoreCase)))
                    keyNames = keyNames.Concat(new[] { bucket! }).ToArray();

                var rowsType = Query.Analysis.DerivedTypeFactory.GetDerivedType(rowsTopic, keyNames, Array.Empty<string>(), Array.Empty<Type>());
                var props = rowsType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                var keyProps = keyNames
                    .Select(n => props.FirstOrDefault(p => string.Equals(p.Name, n, StringComparison.OrdinalIgnoreCase)))
                    .Where(p => p != null)
                    .Cast<PropertyInfo>()
                    .ToArray();

                var em = new EntityModel
                {
                    EntityType = rowsType,
                    TopicName = rowsTopic,
                    AllProperties = props,
                    KeyProperties = keyProps,
                    ValidationResult = new ValidationResult { IsValid = true }
                };
                em.SetStreamTableType(StreamTableType.Stream);
                var keyTypes = keyNames.Select(name =>
                {
                    var prop = props.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
                    return prop?.PropertyType ?? typeof(object);
                }).ToArray();
                var keyNulls = keyTypes.Select(t => !t.IsValueType || Nullable.GetUnderlyingType(t) != null).ToArray();
                var rowsMetadata = new QueryMetadata
                {
                    Identifier = rowsTopic,
                    Namespace = ns,
                    Role = Role.Final1sStream.ToString(),
                    TimeframeRaw = "1s",
                    GraceSeconds = baseMetadata.GraceSeconds,
                    ForceGenericKey = true,
                    ForceGenericValue = true,
                    Keys = new QueryKeyShape(keyNames, keyTypes, keyNulls),
                    Projection = QueryProjectionShape.Empty
                };
                QueryMetadataWriter.Apply(em, rowsMetadata);

                // Register and map
                _entityModels[rowsType] = em;
                RegisterOrUpdateMapping(em);
            }
        }
        catch (Exception ex)
        {
            Logger?.LogDebug(ex, "InjectRowsSimpleEntities failed");
        }
    }
    private static bool IsRetryableKsqlError(string? message)
    {
        if (string.IsNullOrWhiteSpace(message)) return true;
        var m = message.ToLowerInvariant();
        // common transient indicators during startup/warmup
        return m.Contains("timeout while waiting for command topic")
            || m.Contains("could not write the statement")
            || m.Contains("failed to create new kafkaadminclient")
            || m.Contains("no resolvable bootstrap urls")
            || m.Contains("ksqldb server is not ready")
            || m.Contains("statement_error");
    }
    private static Type GetDerivedType(string name)
    {
        return Ksql.Linq.Query.Analysis.DerivedTypeFactory.GetDerivedType(name);
    }

    // Dynamic IL helpers no longer needed (moved to DerivedTypeFactory)
    private async Task WaitForEntityDdlAsync(EntityModel model, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var entityName = model.GetTopicName();
        var useExtended = model.StreamTableType == StreamTableType.Table;
        var describeTarget = NormalizeDescribeIdentifier(entityName);
        var describeSql = useExtended
            ? $"DESCRIBE {describeTarget} EXTENDED;"
            : $"DESCRIBE {describeTarget};";
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var res = await ExecuteStatementAsync(describeSql);
                if (res.IsSuccess && !string.IsNullOrWhiteSpace(res.Message))
                {
                    var msg = res.Message;
                    if (!msg.ToUpperInvariant().Contains("STATEMENT_ERROR") && HasDescribeInfo(msg))
                    {
                        await KsqlPersistentQueryMonitor.AssertTopicPartitionsAsync(
                            sql => ExecuteStatementAsync(sql),
                            model).ConfigureAwait(false);
                        return;
                    }
                }
            }
            catch
            {
                // ignore and retry
            }
            await Task.Delay(500);
        }
        static bool HasDescribeInfo(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return false;

            if (TryDescribeJsonHasInfo(message))
                return true;

            var lines = message.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            var lower = message.ToLowerInvariant();

            bool hasKey = lines.Any(l => l.Contains("Key format", StringComparison.OrdinalIgnoreCase)) || lower.Contains("\"keyformat\"");
            bool hasValue = lines.Any(l => l.Contains("Value format", StringComparison.OrdinalIgnoreCase)) || lower.Contains("\"valueformat\"");
            bool hasSchema = lines.Any(l => l.Contains('|')) || lower.Contains("\"fields\"");

            return hasKey && hasValue && hasSchema;
        }
        static bool TryDescribeJsonHasInfo(string message)
        {
            try
            {
                using var doc = JsonDocument.Parse(message);
                if (doc.RootElement.ValueKind != JsonValueKind.Array)
                    return false;

                foreach (var element in doc.RootElement.EnumerateArray())
                {
                    if (element.ValueKind != JsonValueKind.Object)
                        continue;

                    if (!element.TryGetProperty("sourceDescription", out var source) || source.ValueKind != JsonValueKind.Object)
                        continue;

                    var hasKey = source.TryGetProperty("keyFormat", out var keyFormat) && keyFormat.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(keyFormat.GetString());
                    var hasValue = source.TryGetProperty("valueFormat", out var valueFormat) && valueFormat.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(valueFormat.GetString());
                    var hasFields = source.TryGetProperty("fields", out var fields) && fields.ValueKind == JsonValueKind.Array && fields.GetArrayLength() > 0;

                    if (hasKey && hasValue && hasFields)
                        return true;
                }
            }
            catch
            {
            }

            return false;
        }
    }
    private Task EnsureQueryEntityDdlAsync(Type type, EntityModel model)
    {
        if (_queryDdlMonitor == null)
            throw new InvalidOperationException("Query DDL monitor not initialized.");

        return _queryDdlMonitor.EnsureQueryEntityDdlAsync(type, model);
    }

    private static bool TryGetQueryStateFromJson(string showQueriesOutput, string targetTopic, string? queryId, ref string? state)
    {
        var normalizedTarget = Infrastructure.Ksql.KsqlWaitService.NormalizeIdentifier(targetTopic);
        var normalizedQueryId = Infrastructure.Ksql.KsqlWaitService.NormalizeIdentifier(queryId);
        if (Infrastructure.Ksql.KsqlWaitService.TryGetQueryStateFromJson(showQueriesOutput, normalizedTarget, normalizedQueryId, out var resolved))
        {
            state = resolved;
            return true;
        }
        state = null;
        return false;
    }

    private static bool CheckQueryRunningInText(IEnumerable<string> lines, string targetUpper, string? normalizedQueryId)
        => Infrastructure.Ksql.KsqlWaitService.CheckQueryRunningInText(lines, targetUpper, normalizedQueryId);

    private async Task WaitForDerivedQueriesRunningAsync(TimeSpan timeout)
    {
        var targets = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var model in _entityModels.Values)
        {
            var metadata = model.GetOrCreateMetadata();
            var timeframe = PromoteTimeframe(model, ref metadata);
            if (string.IsNullOrWhiteSpace(timeframe))
                continue;

            if (!RequiresPersistentQuery(model, ref metadata))
                continue;

            var topic = model.GetTopicName();
            if (!string.IsNullOrWhiteSpace(topic))
                targets.Add(topic);
        }

        await KsqlPersistentQueryMonitor.WaitForDerivedQueriesRunningAsync(
            sql => ExecuteStatementAsync(sql),
            targets.ToList(),
            timeout).ConfigureAwait(false);
    }

    private static bool RequiresPersistentQuery(EntityModel model, ref QueryMetadata metadata)
    {
        var role = PromoteRole(model, ref metadata);
        if (!role.HasValue || role.Value == Role.Final1sStream)
            return false;

        var input = PromoteInputHint(model, ref metadata);
        if (!string.IsNullOrWhiteSpace(input) && input.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase))
            return false;

        return true;
    }

    private static Role? PromoteRole(EntityModel model, ref QueryMetadata metadata)
    {
        _ = model;
        if (!string.IsNullOrWhiteSpace(metadata.Role) && Enum.TryParse<Role>(metadata.Role, ignoreCase: true, out var parsed))
            return parsed;
        return null;
    }

    private static string? PromoteTimeframe(EntityModel model, ref QueryMetadata metadata)
    {
        _ = model;
        return metadata.TimeframeRaw;
    }

    private static string? PromoteInputHint(EntityModel model, ref QueryMetadata metadata)
    {
        _ = model;
        return metadata.InputHint;
    }

    private static TimeSpan GetQueryRunningTimeout()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_TIMEOUT_SECONDS");
        if (int.TryParse(env, out var seconds) && seconds > 0)
            return TimeSpan.FromSeconds(seconds);
        return TimeSpan.FromSeconds(180);
    }
    private async Task StabilizePersistentQueriesAsync(
        IReadOnlyList<PersistentQueryExecution> executions,
        EntityModel? baseModel,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (_adminService == null || executions == null || executions.Count == 0)
        {
            Logger?.LogWarning("KafkaAdminService unavailable; skipping persistent query stabilization.");
            return;
        }
        foreach (var execution in executions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var parents = GetParentTopicCandidates(execution, baseModel);
            var partitions = ResolveParentPartitions(parents, execution.TargetModel);
            await KsqlPersistentQueryMonitor.EnsureInternalTopicsReadyAsync(
                _adminService,
                Logger,
                execution.QueryId,
                partitions,
                timeout,
                cancellationToken).ConfigureAwait(false);

            await KsqlPersistentQueryMonitor.EnsureSchemaSubjectsReadyAsync(
                _schemaRegistryClient,
                Logger,
                execution.TargetModel,
                execution.TargetTopic,
                timeout,
                cancellationToken).ConfigureAwait(false);
            var outputTimeout = GetVerifyOutputTimeout(timeout);
            await KsqlPersistentQueryMonitor.VerifyOutputRecordsAsync(
                _adminService,
                _dslOptions.Common.BootstrapServers,
                Logger,
                execution.TargetTopic,
                outputTimeout,
                cancellationToken).ConfigureAwait(false);
        }
    }


    private IEnumerable<string> GetParentTopicCandidates(PersistentQueryExecution execution, EntityModel? baseModel)
    {
        var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(execution.InputTopic))
            topics.Add(execution.InputTopic!);
        var target = execution.TargetModel;
        if (target.QueryModel != null)
        {
            var sourceTypes = target.QueryModel.SourceTypes ?? Array.Empty<Type>();
            foreach (var source in sourceTypes)
            {
                if (_entityModels.TryGetValue(source, out var srcModel))
                {
                    topics.Add(srcModel.GetTopicName());
                    continue;
                }
                if (_dslOptions.SourceNameOverrides is { Count: > 0 } && _dslOptions.SourceNameOverrides.TryGetValue(source.Name, out var overrideName) && !string.IsNullOrWhiteSpace(overrideName))
                {
                    topics.Add(overrideName);
                    continue;
                }
                topics.Add(source.Name.ToUpperInvariant());
            }
        }
        else if (baseModel != null)
        {
            topics.Add(baseModel.GetTopicName());
        }
        return topics;
    }
    private int ResolveParentPartitions(IEnumerable<string> parentTopics, EntityModel fallbackModel)
    {
        foreach (var topic in parentTopics)
        {
            var candidate = topic?.Trim();
            if (string.IsNullOrWhiteSpace(candidate))
                continue;
            var metadata = _adminService.TryGetTopicMetadata(candidate);
            metadata ??= _adminService.TryGetTopicMetadata(candidate.ToLowerInvariant());
            if (metadata?.Partitions != null && metadata.Partitions.Count > 0)
                return metadata.Partitions.Count;
        }
        if (fallbackModel.Partitions > 0)
            return fallbackModel.Partitions;
        return 1;
    }

    private static string NormalizeDescribeIdentifier(string? identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            return identifier ?? string.Empty;

        var trimmed = identifier.Trim().Trim('`').Trim('"');
        return trimmed.ToUpperInvariant();
    }

    private static int GetPersistentQueryMaxAttempts()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_PERSISTENT_QUERY_MAX_ATTEMPTS");
        if (int.TryParse(env, out var attempts) && attempts > 0)
            return attempts;
        return 3;
    }
    private static TimeSpan GetPersistentQueryTimeout()
    {
        var env = Environment.GetEnvironmentVariable("KSQL_PERSISTENT_QUERY_READY_TIMEOUT_SECONDS");
        if (int.TryParse(env, out var seconds) && seconds > 0)
            return TimeSpan.FromSeconds(seconds);
        return TimeSpan.FromSeconds(45);
    }
    private static TimeSpan GetVerifyOutputTimeout(TimeSpan fallback)
    {
        var env = Environment.GetEnvironmentVariable("KSQL_VERIFY_OUTPUT_TIMEOUT_SECONDS");
        if (int.TryParse(env, out var seconds))
        {
            if (seconds <= 0)
                return TimeSpan.Zero;
            return TimeSpan.FromSeconds(seconds);
        }
        return fallback;
    }

    /// <summary>
    private async Task AlignDerivedMappingWithSchemaAsync(EntityModel model)
    {
        if (model == null || _mappingRegistry == null)
            return;

        try
        {
            var topic = model.GetTopicName();
            if (string.IsNullOrWhiteSpace(topic))
                return;

            var client = GetSchemaRegistryClient();
            var subject = $"{topic}-key";
            var schemaMetadata = await client.GetLatestSchemaAsync(subject).ConfigureAwait(false);
            var schemaString = schemaMetadata?.SchemaString;
            if (string.IsNullOrWhiteSpace(schemaString))
                return;

            KeyValueTypeMapping mapping;
            try
            {
                mapping = _mappingRegistry.GetMapping(model.EntityType);
            }
            catch (InvalidOperationException)
            {
                return;
            }

            if (Avro.Schema.Parse(schemaString) is not RecordSchema recordSchema)
                return;

            var existing = mapping.KeyProperties;
            if (existing == null || existing.Length == 0)
            {
                mapping.AvroKeySchema = schemaString;
                mapping.AvroKeyRecordSchema = recordSchema;
                if (string.IsNullOrWhiteSpace(model.KeySchemaFullName))
                    model.KeySchemaFullName = recordSchema.Fullname;
                return;
            }

            var source = existing ?? Array.Empty<PropertyMeta>();
            var updated = new PropertyMeta[source.Length];
            for (int i = 0; i < source.Length; i++)
            {
                var meta = source[i];
                var candidate = meta.SourceName ?? meta.PropertyInfo?.Name ?? meta.Name;
                var field = recordSchema.Fields.FirstOrDefault(f => string.Equals(f.Name, candidate, StringComparison.Ordinal))
                    ?? recordSchema.Fields.FirstOrDefault(f => string.Equals(f.Name, candidate, StringComparison.OrdinalIgnoreCase))
                    ?? (i < recordSchema.Fields.Count ? recordSchema.Fields[i] : null);

                if (field == null)
                {
                    updated[i] = meta;
                    continue;
                }

                updated[i] = new PropertyMeta
                {
                    Name = meta.Name,
                    SourceName = field.Name,
                    PropertyType = meta.PropertyType,
                    IsNullable = meta.IsNullable,
                    Precision = meta.Precision,
                    Scale = meta.Scale,
                    Format = meta.Format,
                    Attributes = meta.Attributes,
                    PropertyInfo = meta.PropertyInfo,
                    IsAutoGenerated = meta.IsAutoGenerated
                };
            }

            mapping.KeyProperties = updated;
            mapping.AvroKeySchema = schemaString;
            mapping.AvroKeyRecordSchema = recordSchema;
            if (string.IsNullOrWhiteSpace(model.KeySchemaFullName))
                model.KeySchemaFullName = recordSchema.Fullname;
        }
        catch (ConfluentSchemaRegistry.SchemaRegistryException ex) when (ex.ErrorCode == 404 || ex.ErrorCode == 40401)
        {
            Logger.LogDebug(ex, "Schema subject {Subject} not found while aligning mapping.", $"{model.GetTopicName()}-key");
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Failed to align derived mapping for {EntityType}", model.EntityType.Name);
        }
    }
    /// Register mapping information for a query-defined entity using its KsqlQueryModel.
    /// </summary>
    private void RegisterQueryModelMapping(EntityModel model)
    {
        if (model.QueryModel == null)
            return;
        var metadata = model.GetOrCreateMetadata();
        var derivedType = model.QueryModel!.DetermineType();
        var isTable = model.GetExplicitStreamTableType() == StreamTableType.Table ||
            derivedType == StreamTableType.Table;
        // Ensure query-defined entities get the correct Stream/Table classification
        // so downstream components (e.g., cache enabling/resolution) behave correctly.
        model.SetStreamTableType(isTable ? StreamTableType.Table : StreamTableType.Stream);
        // Derive key properties from query model/projection when not populated on the EntityModel.
        var keyProps = model.KeyProperties;
        if ((keyProps == null || keyProps.Length == 0))
        {
            try
            {
                var resultType = model.QueryModel.SelectProjection?.Body switch
                {
                    System.Linq.Expressions.NewExpression ne when ne.Type != null => ne.Type,
                    System.Linq.Expressions.MemberInitExpression mi when mi.Type != null => mi.Type,
                    System.Linq.Expressions.MemberExpression me when me.Type != null => me.Type,
                    System.Linq.Expressions.ParameterExpression pe when pe.Type != null => pe.Type,
                    _ => null
                };
                if (resultType != null)
                {
                    var keyNames = metadata.Keys.Names;

                    if (keyNames != null && keyNames.Length > 0)
                    {
                        var pis = keyNames
                            .Select(n => resultType.GetProperty(n, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.IgnoreCase))
                            .Where(p => p != null)
                            .Cast<System.Reflection.PropertyInfo>()
                            .ToArray();
                        if (pis.Length > 0)
                        {
                            keyProps = pis;
                        }
                    }
                }
            }
            catch { }
        }
        _mappingRegistry.RegisterQueryModel(
            model.EntityType,
            model.QueryModel!,
            keyProps ?? Array.Empty<System.Reflection.PropertyInfo>(),
            model.GetTopicName(),
            genericKey: isTable,
            genericValue: isTable);
    }
    private static object CreateDummyInstance(Type entityType)
    {
        var method = typeof(Application.DummyObjectFactory).GetMethod("CreateDummy")!
            .MakeGenericMethod(entityType);
        return method.Invoke(null, null)!;
    }
}















