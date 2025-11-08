using Confluent.Kafka;
using Ksql.Linq.Cache.Core;
using Ksql.Linq.Cache.Extensions;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Dlq;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Infrastructure.Admin;
using Ksql.Linq.Infrastructure.KsqlDb;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Infrastructure.Kafka;
using Ksql.Linq.Mapping;
using Ksql.Linq.Runtime;
using Ksql.Linq.Events;
using Ksql.Linq.Messaging.Consumers;
using Ksql.Linq.Messaging.Producers;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.Runtime.Heartbeat;
using Ksql.Linq.Runtime.Monitor;
using Ksql.Linq.Runtime.Context;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;
using System.Reflection;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ConfluentSchemaRegistry = Confluent.SchemaRegistry;

namespace Ksql.Linq;
/// <summary>
/// Base partial for KsqlContext; keeps public API surface under the flat Ksql.Linq namespace.
/// Partial files (see Context/ directory):
/// - KsqlContext.Model.cs: Entity model management (Set&lt;T&gt;, GetEntityModels)
/// - KsqlContext.Lifecycle.cs: Initialization, disposal, lifecycle coordination
/// - KsqlContext.Schema.cs: Schema registration and DDL operations
/// - KsqlContext.Execution.cs: Query execution (ExecuteStatementAsync, QueryRowsAsync, pull helpers)
/// </summary>
public abstract partial class KsqlContext : IKsqlContext
{
    private KafkaProducerManager _producerManager = null!;
    private readonly ConcurrentDictionary<Type, EntityModel> _entityModels = new();
    private readonly Dictionary<Type, object> _entitySets = new();
    private readonly Dictionary<Type, Configuration.ResolvedEntityConfig> _resolvedConfigs = new();
    private bool _disposed = false;
    private KafkaConsumerManager _consumerManager = null!;
    private IDlqProducer _dlqProducer = null!;
    private ICommitManager _commitManager = null!;
    private Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient> _schemaRegistryClient = null!;
    private IKsqlDbClient _ksqlDbClient = null!;
    private IKsqlExecutor? _ksqlExecutor;
    private ITopicAdmin? _topicAdmin;
    private Core.Dlq.IDlqClient _dlqClient = null!;
    private IRateLimiter _dlqLimiter = null!;
    private IMarketScheduleProvider _marketScheduleProvider = null!;
    private Task? _msRefreshTask;
    private Func<DateTime> _now = () => DateTime.UtcNow;
    private Func<TimeSpan, CancellationToken, Task> _delay = (t, ct) => Task.Delay(t, ct);
    private KsqlQueryDdlMonitor _queryDdlMonitor = null!;

    private KafkaAdminService _adminService = null!;
    private readonly KsqlDslOptions _dslOptions;
    private TableCacheRegistry? _cacheRegistry;
    private readonly MappingRegistry _mappingRegistry = new();
    private ILogger _logger = null!;
    private ILoggerFactory? _loggerFactory;
    private readonly ConcurrentDictionary<string, HubStreamBridgeController> _hubBridgeControllers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _enforceEarliestTopics = new(StringComparer.OrdinalIgnoreCase);
    private CancellationTokenSource? _hubBridgeCts;
    private IRowMonitorCoordinator? _rowMonitorCoordinator;
    private Ksql.Linq.Runtime.Schema.ISchemaRegistrar? _schemaRegistrar;
    private Ksql.Linq.Runtime.Fill.IStartupFillService? _startupFillService;

    internal ILogger Logger => _logger;

    // Dynamic types for derived entities are now centralized in Query.Analysis.DerivedTypeFactory



    /// <summary>
    /// Hook to decide whether schema registration should be skipped for tests
    /// </summary>
    protected virtual bool SkipSchemaRegistration => false;

    public const string DefaultSectionName = "KsqlDsl";

    protected KsqlContext(IConfiguration configuration, ILoggerFactory? loggerFactory = null)
        : this(configuration, DefaultSectionName, loggerFactory)
    {
    }

    protected KsqlContext(IConfiguration configuration, string sectionName, ILoggerFactory? loggerFactory = null)
    {
        _dslOptions = new KsqlDslOptions();
        configuration.GetSection(sectionName).Bind(_dslOptions);
        DefaultValueBinder.ApplyDefaults(_dslOptions);

        InitializeCore(loggerFactory);

    }

    protected KsqlContext(KsqlDslOptions options, ILoggerFactory? loggerFactory = null)
    {
        _dslOptions = options;
        DefaultValueBinder.ApplyDefaults(_dslOptions);
        InitializeCore(loggerFactory);
    }

    /// <summary>
    /// Explicit lifecycle entry for future orchestration. No-op for now.
    /// </summary>
    public virtual Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>
    /// Gracefully stop background monitors. Mirrors disposal path (idempotent).
    /// </summary>
    public virtual async Task StopAsync()
    {
        try { _hubBridgeCts?.Cancel(); } catch { }
        foreach (var bridge in _hubBridgeControllers.Values)
        {
            try { await bridge.StopAsync().ConfigureAwait(false); } catch { }
        }
        if (_rowMonitorCoordinator != null)
        {
            try { await _rowMonitorCoordinator.StopAsync().ConfigureAwait(false); } catch { }
        }
    }

    // Clear Streamiz table caches and optionally delete local RocksDB state.
    public void ClearStreamizState(bool deleteStateDirs = true)
    {
        try { _cacheRegistry?.Clear(deleteStateDirs); } catch { }
        try { this.ResetTableCache(); } catch { }
        // Re-initialize cache registry wiring so subsequent reads can lazily rebuild
        try
        {
            this.UseTableCache(_dslOptions, _loggerFactory);
            _cacheRegistry = this.GetTableCacheRegistry();
        }
        catch { }
    }

    internal void SetRowMonitorCoordinator(IRowMonitorCoordinator coordinator)
    {
        _rowMonitorCoordinator = coordinator;
    }

    internal void SetKsqlExecutor(IKsqlExecutor executor)
    {
        _ksqlExecutor = executor;
    }

    internal void ApplyDependencies(KsqlContextDependencies deps)
    {
        if (deps == null) return;
        if (deps.KsqlExecutor != null) SetKsqlExecutor(deps.KsqlExecutor);
        if (deps.TopicAdmin != null) SetTopicAdmin(deps.TopicAdmin);
        if (deps.SchemaRegistrar != null) SetSchemaRegistrar(deps.SchemaRegistrar);
        if (deps.RowMonitorCoordinator != null) SetRowMonitorCoordinator(deps.RowMonitorCoordinator);
        if (deps.StartupFillService != null) SetStartupFillService(deps.StartupFillService);
        // Producer/Consumer/DLQ/Cache services are wired via existing initialization paths; keep for later phases.
    }

    internal void SetTopicAdmin(ITopicAdmin admin)
    {
        _topicAdmin = admin;
    }

    internal void SetSchemaRegistrar(Ksql.Linq.Runtime.Schema.ISchemaRegistrar registrar)
    {
        _schemaRegistrar = registrar;
    }

    internal void SetStartupFillService(Ksql.Linq.Runtime.Fill.IStartupFillService svc)
    {
        _startupFillService = svc;
    }

    private void InitializeCore(ILoggerFactory? loggerFactory)
    {
        // Configure only per-property decimal overrides; global precision/scale options removed from docs
        DecimalPrecisionConfig.Configure(_dslOptions.Decimals);

        _schemaRegistryClient = new Lazy<ConfluentSchemaRegistry.ISchemaRegistryClient>(CreateSchemaRegistryClient);
        _ksqlDbClient = new KsqlDbClient(GetDefaultKsqlDbUrl(), loggerFactory?.CreateLogger<KsqlDbClient>());

        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLoggerOrNull<KsqlContext>();

        // Ensure default executor/registrar are wired when not provided via dependencies
        _ksqlExecutor ??= new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(
            _ksqlDbClient,
            _loggerFactory?.CreateLogger<Ksql.Linq.Infrastructure.Ksql.KsqlExecutor>());


        _adminService = new KafkaAdminService(
        Microsoft.Extensions.Options.Options.Create(_dslOptions),
        _loggerFactory);
        RebuildQueryDdlMonitor();
        InitializeEntityModels();
        try
        {
            _producerManager = new KafkaProducerManager(_mappingRegistry,
                 Microsoft.Extensions.Options.Options.Create(_dslOptions),
                 _loggerFactory);
            _dlqProducer = new Ksql.Linq.Messaging.Producers.DlqProducer(_producerManager, _dslOptions.DlqTopicName);

            _commitManager = new ManualCommitManager(_loggerFactory?.CreateLogger<Messaging.Consumers.ManualCommitManager>());

            ConfigureModel();
            ResolveEntityConfigurations();

            _dlqLimiter = new SimpleRateLimiter(_dslOptions.DlqOptions.MaxPerSecond);

            _marketScheduleProvider = new MarketScheduleProvider(_mappingRegistry);
            _consumerManager = new KafkaConsumerManager(_mappingRegistry,
                Microsoft.Extensions.Options.Options.Create(_dslOptions),
                _entityModels,
                _dlqProducer,
                _commitManager,
                _loggerFactory,
                _dlqLimiter);

            _dlqClient = new Core.Dlq.DlqClient(_dslOptions, _consumerManager, _loggerFactory);

            if (!SkipSchemaRegistration)
            {
                _schemaRegistrar ??= new Ksql.Linq.Runtime.Schema.SchemaRegistrar(this);
                _schemaRegistrar.RegisterAndMaterializeAsync().GetAwaiter().GetResult();
            }
            this.UseTableCache(_dslOptions, _loggerFactory);
            _cacheRegistry = this.GetTableCacheRegistry();

            // Optional: application-side startup continuity actions
            if (_dslOptions.Fill?.EnableAppSide == true)
            {
                try
                {
                    // Design policy: do not write synthetic rows. Delegate to service (no-op by default).
                    _startupFillService ??= new Ksql.Linq.Runtime.Fill.NoopStartupFillService();
                    _startupFillService.RunAsync(this, _hubBridgeCts?.Token ?? CancellationToken.None).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Startup fill service reported an issue (continuing)");
                }
            }

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"KsqlContext initialization failed: {ex.Message} ");
            throw;
        }
    }

    private void RebuildQueryDdlMonitor()
    {
        if (_ksqlDbClient == null)
            return;

        var deps = new KsqlQueryDdlMonitor.Dependencies
        {
            Logger = _logger,
            Options = _dslOptions,
            MappingRegistry = _mappingRegistry,
            EntityModels = _entityModels,
            KsqlDbClient = _ksqlDbClient,
            ExecuteStatementAsync = sql => ExecuteStatementAsync(sql),
            RegisterQueryModelMapping = RegisterQueryModelMapping,
            WaitForEntityDdlAsync = (model, timeout) => WaitForEntityDdlAsync(model, timeout),
            AlignDerivedMappingWithSchemaAsync = model => AlignDerivedMappingWithSchemaAsync(model),
            StabilizePersistentQueriesAsync = (executions, baseModel, timeout, cancellationToken) => StabilizePersistentQueriesAsync(executions, baseModel, timeout, cancellationToken),
            StartRowMonitorAsync = results => StartRowMonitorForResultsAsync(results),
            WaitForDerivedQueriesRunningAsync = timeout => WaitForDerivedQueriesRunningAsync(timeout),
            DelayAsync = (span, token) => _delay(span, token),
            EnsureRowsLastTableAsync = (execute, getTables, getStreams, model) => KsqlPersistentQueryMonitor.EnsureRowsLastTableAsync(
                execute,
                getTables,
                getStreams,
                model,
                _dslOptions.KsqlDdlRetryCount,
                _dslOptions.KsqlDdlRetryInitialDelayMs,
                evt => RuntimeEventBus.PublishAsync(evt)),
            WaitForQueryRunningAsync = (target, queryId, timeout) => KsqlWaitClient.WaitForQueryRunningAsync(sql => ExecuteStatementAsync(sql), target, queryId, timeout),
            TryGetQueryIdFromShowQueriesAsync = (targetTopic, statement) => KsqlWaitClient.TryGetQueryIdFromShowQueriesAsync(sql => ExecuteStatementAsync(sql), targetTopic, statement),
            TerminateQueriesAsync = executions => KsqlPersistentQueryMonitor.TerminateQueriesAsync(sql => ExecuteStatementAsync(sql), Logger, executions),
            AssertTopicPartitionsAsync = model => AssertTopicPartitionsAsync(model),
            PublishEventAsync = evt => RuntimeEventBus.PublishAsync(evt),
            WaitForPersistentQueryAsync = (targetTopic, statement, timeout) => KsqlWaitClient.WaitForPersistentQueryAsync(sql => ExecuteStatementAsync(sql), targetTopic, statement ?? string.Empty, timeout),
            GetPersistentQueryMaxAttempts = () => GetPersistentQueryMaxAttempts(),
            GetPersistentQueryTimeout = () => GetPersistentQueryTimeout(),
            GetQueryRunningTimeout = () => GetQueryRunningTimeout(),
            IsRowsRole = model => IsRowsRole(model)
        };

        _queryDdlMonitor = new KsqlQueryDdlMonitor(deps);
    }

    internal void RebuildQueryDdlMonitorForTesting()
    {
        RebuildQueryDdlMonitor();
    }

    private Task StartRowMonitorForResultsAsync(IReadOnlyList<DerivedTumblingPipeline.ExecutionResult> results)
    {
        var executionResults = results ?? Array.Empty<DerivedTumblingPipeline.ExecutionResult>();
        _hubBridgeCts ??= new CancellationTokenSource();
        var token = _hubBridgeCts.Token;

        if (_rowMonitorCoordinator != null)
        {
            return _rowMonitorCoordinator.StartForResults(executionResults.Cast<object>().ToList(), token);
        }

        var coordinator = new Runtime.Monitor.RowMonitorCoordinator(this);
        return coordinator.StartForResults(executionResults.Cast<object>().ToList(), token);
    }

    protected virtual void OnModelCreating(IModelBuilder modelBuilder) { }

    /// <summary>
    /// OnModelCreating 竊・execute automatic schema registration flow
    /// </summary>
    private ConfluentSchemaRegistry.ISchemaRegistryClient CreateSchemaRegistryClient()
    {
        var options = _dslOptions.SchemaRegistry;
        var config = new ConfluentSchemaRegistry.SchemaRegistryConfig
        {
            Url = options.Url,
            MaxCachedSchemas = options.MaxCachedSchemas,
            RequestTimeoutMs = options.RequestTimeoutMs
        };

        return new ConfluentSchemaRegistry.CachedSchemaRegistryClient(config);
    }


    private Uri GetDefaultKsqlDbUrl()
    {
        if (!string.IsNullOrWhiteSpace(_dslOptions.KsqlDbUrl) &&
            Uri.TryCreate(_dslOptions.KsqlDbUrl, UriKind.Absolute, out var configured))
        {
            return configured;
        }

        var schemaUrl = _dslOptions.SchemaRegistry.Url;
        if (!string.IsNullOrWhiteSpace(schemaUrl) &&
            Uri.TryCreate(schemaUrl, UriKind.Absolute, out var schemaUri))
        {
            var port = schemaUri.IsDefaultPort || schemaUri.Port == 8081 ? 8088 : schemaUri.Port;
            return new Uri($"{schemaUri.Scheme}://{schemaUri.Host}:{port}");
        }

        // Default to localhost if nothing configured (test-friendly)
        return new Uri("http://localhost:8088");
    }
    private HttpClient CreateClient()
    {
        return new HttpClient { BaseAddress = GetDefaultKsqlDbUrl() };
    }


    private static string Preview(string? sql, int max = 120)
    {
        if (string.IsNullOrWhiteSpace(sql)) return string.Empty;
        sql = sql.Replace("\n", " ").Replace("\r", " ").Trim();
        return sql.Length <= max ? sql : sql.Substring(0, max) + "...";
    }

    

    private static string TryQualifySimpleJoin(string ksql)
    {
        try
        {
            var text = ksql;
            var fromIdx = text.IndexOf("FROM ", StringComparison.OrdinalIgnoreCase);
            var joinIdx = text.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase);
            if (fromIdx < 0 || joinIdx < 0 || !(fromIdx < joinIdx)) return ksql;

            string ReadIdent(string s, int start)
            {
                int i = start;
                while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
                int j = i;
                while (j < s.Length && (char.IsLetterOrDigit(s[j]) || s[j] == '_' || s[j] == '.')) j++;
                return s.Substring(i, Math.Max(0, j - i)).Trim();
            }

            var left = ReadIdent(text, fromIdx + 5);
            var right = ReadIdent(text, joinIdx + 6);
            if (string.IsNullOrEmpty(left) || string.IsNullOrEmpty(right)) return ksql;

            // qualify a simple equality ON clause with bare identifiers
            var pattern = new System.Text.RegularExpressions.Regex(@"ON\s*\(\s*([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\s*\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            var replaced = pattern.Replace(text, m => $"ON ({left}.{m.Groups[1].Value} = {right}.{m.Groups[2].Value})");
            if (replaced.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase) >= 0 &&
                replaced.IndexOf("EMIT CHANGES", StringComparison.OrdinalIgnoreCase) < 0)
            {
                // insert EMIT CHANGES before trailing ';' if present
                var semi = replaced.LastIndexOf(';');
                if (semi >= 0)
                    replaced = replaced.Substring(0, semi) + " EMIT CHANGES" + replaced.Substring(semi);
                else
                    replaced += " EMIT CHANGES";
            }
            if (replaced.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase) >= 0 &&
                replaced.IndexOf(" WITHIN ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                var re = new System.Text.RegularExpressions.Regex(@"JOIN\s+([A-Za-z_][\w]*)\s+ON\s*\(", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                replaced = re.Replace(replaced, m => $"JOIN {m.Groups[1].Value} WITHIN 300 SECONDS ON (");
            }
            return replaced;
        }
        catch
        {
            return ksql;
        }
    }



    /// <summary>
    /// Core-level EventSet implementation (integrates higher-level services).
    /// </summary>
    protected virtual IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) where T : class
    {
        var model = EnsureEntityModel(typeof(T), entityModel);
        var baseSet = new EventSetWithServices<T>(this, model);
        if (model.GetExplicitStreamTableType() == StreamTableType.Table && model.EnableCache)
        {
            return new ReadCachedEntitySet<T>(this, model, null, baseSet);
        }
        return baseSet;
    }

    

    // Ensure `<base>_1s_rows_last` exists for the base entity T.
    // Intended for migration flows that will rely on rows_last presence for idempotency.
    public async Task EnsureRowsLastTableAsync<T>() where T : class
    {
        var baseType = typeof(T);
        var rowsTopic = Runtime.TimeBucketTypes.GetLiveTopicName(baseType, Runtime.Period.Seconds(1)); // <base>_1s_rows
        var rowsModel = _entityModels.Values.FirstOrDefault(m => string.Equals(m.GetTopicName(), rowsTopic, StringComparison.OrdinalIgnoreCase));
        if (rowsModel == null)
            throw new InvalidOperationException($"Rows entity model not found for topic '{rowsTopic}'. Ensure 1s window is configured.");
        await EnsureRowsLastTableForAsync(rowsModel).ConfigureAwait(false);
    }

    internal bool ShouldConsumeFromBeginning(string topic)
        => _enforceEarliestTopics.ContainsKey(topic);

    internal KafkaProducerManager GetProducerManager() => _producerManager;

    

    // Session check helper for runtime components (e.g., rows hub/continuation)
    internal bool IsInSession(IReadOnlyList<string> keyParts, DateTime timestampUtc)
    {
        try
        {
            return _marketScheduleProvider?.IsInSession(keyParts, timestampUtc) ?? true;
        }
        catch
        {
            return true;
        }
    }

    // StartHubBridgesForResults migrated to RowMonitorCoordinator (use RunRowMonitorForResults instead)

    private static bool IsRowsRole(EntityModel model)
    {
        if (model == null) return false;
        // Prefer stable identifier suffix *_1s_rows
        var metadata = model.GetOrCreateMetadata();
        if (!string.IsNullOrWhiteSpace(metadata.Identifier) && metadata.Identifier.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase))
            return true;
        // Fallback: topic name convention
        var topic = (model.TopicName ?? model.EntityType?.Name ?? string.Empty).ToLowerInvariant();
        return topic.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase);
    }

    private Task EnsureRowsLastTableForAsync(EntityModel rowsModel)
    {
        if (rowsModel == null)
            throw new ArgumentNullException(nameof(rowsModel));

        Task<HashSet<string>> GetStreamTopicsAsync()
        {
            if (_ksqlDbClient is Infrastructure.KsqlDb.KsqlDbClient concrete)
                return concrete.GetStreamTopicsAsync();
            return Task.FromResult(new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        }

        return KsqlPersistentQueryMonitor.EnsureRowsLastTableAsync(
            sql => ExecuteStatementAsync(sql),
            () => _ksqlDbClient.GetTableTopicsAsync(),
            GetStreamTopicsAsync,
            rowsModel,
            _dslOptions.KsqlDdlRetryCount,
            _dslOptions.KsqlDdlRetryInitialDelayMs,
            evt => RuntimeEventBus.PublishAsync(evt));
    }


    private IRowMonitorController? CreateRowMonitor(EntityModel sourceModel, EntityModel targetModel, KsqlQueryModel queryModel)
    {
        var method = typeof(KsqlContext).GetMethod(nameof(CreateRowMonitorGeneric), BindingFlags.NonPublic | BindingFlags.Instance);
        if (method == null)
            return null;

        try
        {
            var generic = method.MakeGenericMethod(sourceModel.EntityType, targetModel.EntityType);
            return (IRowMonitorController)generic.Invoke(this, new object[] { sourceModel, targetModel, queryModel })!;
        }
        catch (Exception ex)
        {
            Logger?.LogError(ex, "Failed to build row monitor for {Target}", GetTopicName(targetModel));
            return null;
        }
    }

    private IRowMonitorController CreateRowMonitorGeneric<TSource, TRow>(EntityModel sourceModel, EntityModel targetModel, KsqlQueryModel queryModel)
        where TSource : class
        where TRow : class
    {
        var monitorLogger = _loggerFactory?.CreateLogger<RowMonitor<TSource, TRow>>();
        return new RowMonitor<TSource, TRow>(this, sourceModel, targetModel, queryModel, monitorLogger);
    }

    internal IDisposable SubscribeRowsMonitor(string targetTopic, Func<RowMonitorEvent, Task> handler)
    {
        if (string.IsNullOrWhiteSpace(targetTopic))
            throw new ArgumentNullException(nameof(targetTopic));
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        var key = targetTopic.ToLowerInvariant();
        if (_hubBridgeControllers.TryGetValue(key, out var controller))
        {
            return controller.Subscribe(handler);
        }

        throw new InvalidOperationException($"Row monitor for {targetTopic} is not registered.");
    }

    private static string? GetExecutionId(DerivedTumblingPipeline.ExecutionResult execution)
    {
        var metadata = execution.Model.GetOrCreateMetadata();
        if (!string.IsNullOrWhiteSpace(metadata.Identifier))
            return metadata.Identifier;
        return GetTopicName(execution.Model);
    }

    

    private static string GetTopicName(EntityModel model)
        => (model.TopicName ?? model.EntityType.Name).ToLowerInvariant();

    // Adapters for RowMonitorCoordinator ownership
    internal bool IsRowsRoleAdapter(EntityModel model) => IsRowsRole(model);
    internal EntityModel EnsureEntityModelAdapter(Type entityType, EntityModel? model = null) => EnsureEntityModel(entityType, model);
    internal Query.Dsl.KsqlQueryModel? PopulateQueryModelIfMissingAdapter(EntityModel targetModel, Query.Dsl.KsqlQueryModel? fromExecution)
    {
        var qm = targetModel.QueryModel ?? fromExecution;
        if (targetModel.QueryModel == null && qm != null)
        {
            targetModel.QueryModel = qm;
            Logger?.LogDebug("Row monitor evaluation populated QueryModel for {Target} from execution result", GetTopicName(targetModel));
        }
        if (qm == null)
        {
            Logger?.LogWarning("Row monitor evaluation skipped: missing QueryModel for {Target}", GetTopicName(targetModel));
        }
        return qm;
    }
    internal async Task EnsureRowsLastTableForSafeAsync(EntityModel targetModel)
    {
        try { await EnsureRowsLastTableForAsync(targetModel).ConfigureAwait(false); } catch { }
    }
    internal Runtime.IRowMonitorController? CreateRowMonitorAdapter(EntityModel sourceModel, EntityModel targetModel, Query.Dsl.KsqlQueryModel qm)
        => CreateRowMonitor(sourceModel, targetModel, qm);
    internal string GetTopicNameAdapter(EntityModel model) => GetTopicName(model);
    internal void InitializeMarketScheduleIfNeededAdapter(System.Collections.Generic.IReadOnlyList<Query.Analysis.DerivedTumblingPipeline.ExecutionResult> results)
    {
        try
        {
            if (_msRefreshTask == null)
            {
                var scheduleType = results
                    .Select(r => r.Model?.QueryModel?.BasedOnType)
                    .FirstOrDefault(t => t != null);
                if (scheduleType != null)
                {
                    var setMethod = GetType().GetMethod(nameof(Set))!.MakeGenericMethod(scheduleType);
                    var set = setMethod.Invoke(this, null);
                    if (set != null)
                    {
                        _hubBridgeCts ??= new CancellationTokenSource();
                        var rows = ((dynamic)set).ToListAsync(_hubBridgeCts?.Token ?? CancellationToken.None).GetAwaiter().GetResult();
                        _marketScheduleProvider.InitializeAsync(scheduleType, rows, _hubBridgeCts?.Token ?? CancellationToken.None).GetAwaiter().GetResult();
                        StartDailyRefresh(scheduleType, set!, _hubBridgeCts?.Token ?? CancellationToken.None);
                    }
                }
            }
        }
        catch { }
    }
    internal void StartHubControllerAdapter(Runtime.IRowMonitorController monitor, string sourceTopic, string targetTopic)
    {
        _hubBridgeCts ??= new CancellationTokenSource();
        var controllerLogger = _loggerFactory?.CreateLogger<HubStreamBridgeController>();
        var controller = new HubStreamBridgeController(this, monitor, sourceTopic, targetTopic, controllerLogger, enforceEarliest: true);
        if (_hubBridgeControllers.TryAdd(targetTopic, controller))
        {
            Logger?.LogInformation("Starting row monitor controller target={Target} source={Source}", targetTopic, sourceTopic);
            controller.Start(_hubBridgeCts.Token);
        }
        else
        {
            Logger?.LogInformation("Row monitor evaluation duplicate: controller already registered for {Target}; disposing new instance", targetTopic);
            controller.Dispose();
        }
    }




    private Task RunHubBridgeAsync(Type finalType, Type rowsType, Delegate converter, string finalTopicName, string rowsTopicName, CancellationToken token)
    {
        _logger?.LogInformation("Hub bridge RunHubBridgeAsync {Final}->{Rows} ", finalTopicName, rowsTopicName);

        var method = typeof(KsqlContext).GetMethod(nameof(RunHubBridgeAsyncGeneric), BindingFlags.NonPublic | BindingFlags.Instance);
        if (method == null)
            throw new InvalidOperationException("RunHubBridgeAsyncGeneric missing");
        var generic = method.MakeGenericMethod(finalType, rowsType);
        return (Task)generic.Invoke(this, new object[] { converter, finalTopicName, rowsTopicName, token })!;
    }

    private Task RunHubBridgeAsyncGeneric<TFinal, TRows>(Func<TFinal, TRows> converter, string finalTopicName, string rowsTopicName, CancellationToken token)
        where TFinal : class
        where TRows : class, new()
    {
        var finalSet = Set<TFinal>();
        var rowsSet = Set<TRows>();
        if (finalSet is not EventSet<TFinal> finalEventSet)
            throw new InvalidOperationException($"Entity set for {typeof(TFinal).Name} does not support manual commit.");

        _logger?.LogInformation("Hub bridge consume RunHubBridgeAsyncGeneric {Final}->{Rows} ", finalTopicName, rowsTopicName);
        return finalSet.ForEachAsync(async (entity, headers, meta) =>
        {
            var converted = converter(entity);
            _logger?.LogInformation("Hub bridge consume {Final}->{Rows} @ {Timestamp}", finalTopicName, rowsTopicName, meta.TimestampUtc);
            await rowsSet.AddAsync(converted, headers, token).ConfigureAwait(false);
            _logger?.LogInformation("Hub bridge produce {Rows} @ {Timestamp}", rowsTopicName, meta.TimestampUtc);
            finalEventSet.Commit(entity);
        }, autoCommit: false, cancellationToken: token);
    }

    // Removed domain-specific sample entity and producer (DedupRateInput) from OSS core

    private sealed class HubStreamBridgeController : IDisposable
    {
        private readonly IRowMonitorController _monitor;
        private readonly string _sourceTopic;
        private readonly string _targetTopic;
        private readonly ILogger? _logger;
        private readonly bool _enforceEarliest;
        private readonly KsqlContext _owner;

        public HubStreamBridgeController(KsqlContext owner, IRowMonitorController monitor, string sourceTopic, string targetTopic, ILogger? logger, bool enforceEarliest)
        {
            _owner = owner;
            _monitor = monitor;
            _sourceTopic = sourceTopic;
            _targetTopic = targetTopic;
            _logger = logger;
            _enforceEarliest = enforceEarliest;
            if (_enforceEarliest)
            {
                _owner._enforceEarliestTopics[sourceTopic] = true;
            }
        }

        public void Start(CancellationToken token)
        {
            _logger?.LogDebug("Row monitor start {Source}->{Target}", _sourceTopic, _targetTopic);
            _monitor.Start(token);
        }

        public async Task StopAsync()
        {
            try
            {
                await _monitor.StopAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        public IDisposable Subscribe(Func<RowMonitorEvent, Task> handler) => _monitor.Subscribe(handler);

        public void Dispose()
        {
            try
            {
                _monitor.StopAsync().GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                if (_enforceEarliest)
                {
                    if (!_owner._hubBridgeControllers.TryGetValue(_targetTopic, out var existing) || ReferenceEquals(existing, this))
                    {
                        _owner._enforceEarliestTopics.TryRemove(_sourceTopic, out _);
                    }
                }
            }
        }
    }
    private void StartDailyRefresh(Type scheduleType, object set, CancellationToken token)
    {
        if (_msRefreshTask != null) return;
        _msRefreshTask = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                var now = _now();
                var next = new DateTime(now.Year, now.Month, now.Day, 0, 5, 0, DateTimeKind.Utc);
                if (now >= next) next = next.AddDays(1);
                await _delay(next - now, token);
                var fresh = await ((dynamic)set).ToListAsync(token);
                await _marketScheduleProvider.RefreshAsync(scheduleType, fresh, token);
            }
        }, token);
    }
    internal KafkaConsumerManager GetConsumerManager() => _consumerManager;
    internal IDlqProducer GetDlqProducer() => _dlqProducer;
    internal ICommitManager GetCommitManager() => _commitManager;
    internal DlqOptions DlqOptions => _dslOptions.DlqOptions;
    internal IRateLimiter DlqLimiter => _dlqLimiter;
    internal ConfluentSchemaRegistry.ISchemaRegistryClient GetSchemaRegistryClient() => _schemaRegistryClient.Value;
    internal MappingRegistry GetMappingRegistry() => _mappingRegistry;
    public Core.Dlq.IDlqClient Dlq => _dlqClient;

    /// <summary>
    /// Get the topic name from an entity type
    /// </summary>
    public string GetTopicName<T>()
    {
        var models = GetEntityModels();
        if (models.TryGetValue(typeof(T), out var model))
        {
            return (model.TopicName ?? typeof(T).Name).ToLowerInvariant();
        }
        return typeof(T).Name.ToLowerInvariant();
    }

    internal async Task<bool> IsEntityReadyAsync<T>(CancellationToken cancellationToken = default) where T : class
    {
        var models = GetEntityModels();
        if (!models.TryGetValue(typeof(T), out var model))
            return false;

        var statement = model.GetExplicitStreamTableType() == StreamTableType.Table
            ? "SHOW TABLES;"
            : "SHOW STREAMS;";

        var name = (model.TopicName ?? typeof(T).Name).ToUpperInvariant();
        var response = await ExecuteStatementAsync(statement);
        if (!response.IsSuccess)
            return false;

        try
        {
            using var doc = JsonDocument.Parse(response.Message);
            var listName = statement.Contains("TABLES") ? "tables" : "streams";
            foreach (var item in doc.RootElement.EnumerateArray())
            {
                if (!item.TryGetProperty(listName, out var arr))
                    continue;

                foreach (var element in arr.EnumerateArray())
                {
                    if (element.TryGetProperty("name", out var n) &&
                        string.Equals(n.GetString(), name, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
        }
        catch
        {
            // ignore parse errors
        }

        return false;
    }

    public async Task WaitForEntityReadyAsync<T>(TimeSpan timeout, CancellationToken cancellationToken = default) where T : class
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            if (await IsEntityReadyAsync<T>(cancellationToken))
                return;

            await Task.Delay(100, cancellationToken);
        }

        throw new TimeoutException($"Entity {typeof(T).Name} not ready after {timeout}.");
    }




    protected virtual void Dispose(bool disposing)
        {
        if (!_disposed && disposing)
        {
            try { _hubBridgeCts?.Cancel(); } catch { }
            foreach (var bridge in _hubBridgeControllers.Values)
            {
                try
                {
                    bridge.StopAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Logger?.LogDebug(ex, "Hub bridge stop failed");
                }
            }
            _hubBridgeControllers.Clear();
            _hubBridgeCts?.Dispose();
            _hubBridgeCts = null;

            foreach (var entitySet in _entitySets.Values)
            {
                if (entitySet is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _entitySets.Clear();
            _entityModels.Clear();
            _disposed = true;

            _producerManager?.Dispose();
            _consumerManager?.Dispose();
            _adminService?.Dispose();
            _cacheRegistry?.Dispose();

            if (_schemaRegistryClient.IsValueCreated)
            {
                _schemaRegistryClient.Value?.Dispose();
            }
            (_ksqlDbClient as IDisposable)?.Dispose();
        }
        }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore();
        Dispose(false);
        GC.SuppressFinalize(this);
    }

    protected virtual async ValueTask DisposeAsyncCore()
        {
            try { _hubBridgeCts?.Cancel(); } catch { }
            var bridgeTasks = _hubBridgeControllers.Values.Select(b => b.StopAsync()).ToArray();
            if (bridgeTasks.Length > 0)
            {
                try
                {
                    await Task.WhenAll(bridgeTasks).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
            _hubBridgeControllers.Clear();
            _hubBridgeCts?.Dispose();
            _hubBridgeCts = null;

            foreach (var entitySet in _entitySets.Values)
            {
                if (entitySet is IAsyncDisposable asyncDisposable)
                {
                    await asyncDisposable.DisposeAsync();
                }
                else if (entitySet is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _entitySets.Clear();

            _producerManager?.Dispose();
            _consumerManager?.Dispose();
            _adminService?.Dispose();
            _cacheRegistry?.Dispose();

            if (_schemaRegistryClient.IsValueCreated)
            {
                _schemaRegistryClient.Value?.Dispose();
            }
            (_ksqlDbClient as IDisposable)?.Dispose();

            await Task.CompletedTask;
        }

    public override string ToString()
    {
        return $"KafkaContextCore: {_entityModels.Count} entities, {_entitySets.Count} sets [schema auto-registration ready]";
    }

    
}


