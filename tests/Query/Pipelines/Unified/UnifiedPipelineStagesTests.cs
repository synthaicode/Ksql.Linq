using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Events;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Infrastructure.KsqlDb;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.Query.Pipelines.Unified;
using Ksql.Linq.Query.Pipelines.Unified.Stages;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ksql.Linq.Tests.Query.Pipelines.Unified;

public class UnifiedPipelineStagesTests
{
    private sealed class StubKsqlDbClient : IKsqlDbClient
    {
        private readonly HashSet<string> _tables;
        private readonly HashSet<string> _streams;

        public int TableTopicCalls { get; private set; }
        public int StreamTopicCalls { get; private set; }
        public HashSet<string>? LastTables { get; private set; }
        public HashSet<string>? LastStreams { get; private set; }

        public StubKsqlDbClient(HashSet<string>? tables = null, HashSet<string>? streams = null)
        {
            _tables = tables ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _streams = streams ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public Task<KsqlDbResponse> ExecuteStatementAsync(string statement)
            => Task.FromResult(new KsqlDbResponse(true, statement));

        public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql)
            => Task.FromResult(new KsqlDbResponse(true, string.Empty));

        public Task<HashSet<string>> GetTableTopicsAsync()
        {
            TableTopicCalls++;
            LastTables = new HashSet<string>(_tables, StringComparer.OrdinalIgnoreCase);
            return Task.FromResult(new HashSet<string>(_tables, StringComparer.OrdinalIgnoreCase));
        }

        public Task<HashSet<string>> GetStreamTopicsAsync()
        {
            StreamTopicCalls++;
            LastStreams = new HashSet<string>(_streams, StringComparer.OrdinalIgnoreCase);
            return Task.FromResult(new HashSet<string>(_streams, StringComparer.OrdinalIgnoreCase));
        }

        public Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null)
            => Task.FromResult(0);

        public Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null)
            => Task.FromResult(0);
    }

    [Fact]
    public async Task RowsLastStage_Final1sStream_EnsuresRowsLastTable()
    {
        var rowsModel = CreateRowsModel("bar_1s_rows");
        var metadata = new QueryMetadata
        {
            Identifier = "bar_1s_rows",
            Role = Role.Final1sStream.ToString(),
            TimestampColumn = "Timestamp",
            Keys = new QueryKeyShape(
                new[] { "BROKER", "SYMBOL" },
                new[] { typeof(string), typeof(string) },
                new[] { false, false }),
            Projection = new QueryProjectionShape(
                new[] { "TIMESTAMP", "BROKERHEAD", "ROUNDAVG1" },
                new[] { typeof(DateTime), typeof(string), typeof(double) },
                new[] { false, false, false })
        };
        QueryMetadataWriter.Apply(rowsModel, metadata);

        var execution = CreateExecution(rowsModel, Role.Final1sStream, "bar_1s_rows");
        var context = new UnifiedPipelineContext(rowsModel.EntityType, rowsModel, new[] { execution });
        var executedSql = new List<string>();
        var ensureCalled = false;

        var deps = CreateDependencies(
            executeStatement: sql =>
            {
                executedSql.Add(sql);
                if (string.Equals(sql.Trim(), "SHOW QUERIES;", StringComparison.OrdinalIgnoreCase))
                {
                    var json = @"[{""queries"":[{""id"":""Q1"",""state"":""RUNNING"",""sinks"":[""BAR_1S_ROWS_LAST""]}]}]";
                    return Task.FromResult(new KsqlDbResponse(true, json));
                }
                return Task.FromResult(new KsqlDbResponse(true, string.Empty));
            },
            ksqlClient: new StubKsqlDbClient(
                tables: new HashSet<string>(StringComparer.OrdinalIgnoreCase),
                streams: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "bar_1s_rows" }),
            ensureRowsLast: (execute, getTables, getStreams, model) =>
            {
                ensureCalled = true;
                return Task.CompletedTask;
            });

        var stage = new RowsLastStage(deps);
        await stage.ExecuteAsync(context, CreateRequest(rowsModel));

        Assert.True(ensureCalled);
        Assert.Empty(executedSql);
    }

    [Fact]
    public async Task RowsLastStage_NonFinalRole_SkipsEnsure()
    {
        var rowsModel = CreateRowsModel("bar_1s_rows_alt");
        var metadata = new QueryMetadata
        {
            Identifier = "bar_1s_rows_alt",
            Role = Role.Live.ToString(),
            TimestampColumn = "Timestamp"
        };
        QueryMetadataWriter.Apply(rowsModel, metadata);

        var execution = CreateExecution(rowsModel, Role.Live, "bar_1s_rows_alt");
        var context = new UnifiedPipelineContext(rowsModel.EntityType, rowsModel, new[] { execution });

        var executedSql = new List<string>();
        var deps = CreateDependencies(
            executeStatement: sql =>
            {
                executedSql.Add(sql);
                return Task.FromResult(new KsqlDbResponse(true, string.Empty));
            },
            ksqlClient: new StubKsqlDbClient());

        var stage = new RowsLastStage(deps);
        await stage.ExecuteAsync(context, CreateRequest(rowsModel));

        Assert.Empty(executedSql);
    }

    [Fact]
    public async Task PersistentQueryStage_CollectsAndStabilizesPersistentQueries()
    {
        var baseModel = new EntityModel { EntityType = typeof(object) };
        var persistentModel = new EntityModel { EntityType = typeof(object), TopicName = "bar_1m_live" };

        var includeExecution = CreateExecution(
            persistentModel,
            Role.Live,
            "bar_1m_live",
            inputTopic: "bar_1m_rows");

        var skipRowsExecution = CreateExecution(
            persistentModel,
            Role.Live,
            "bar_1m_last",
            inputTopic: "bar_1s_rows");

        var transientExecution = CreateExecution(
            persistentModel,
            Role.Live,
            "bar_1m_live",
            inputTopic: null,
            statement: "INSERT INTO bar_1m_live SELECT * FROM foo;");

        var context = new UnifiedPipelineContext(typeof(object), baseModel, new[]
        {
            includeExecution,
            skipRowsExecution,
            transientExecution
        });

        IReadOnlyList<PersistentQueryExecution>? stabilized = null;
        var waitCalled = false;
        string? capturedTopic = null;
        string? capturedStatement = null;

        var deps = CreateDependencies(
            tryGetQueryId: (topic, statement) =>
            {
                capturedTopic = topic;
                capturedStatement = statement;
                return Task.FromResult<string?>("Q1");
            },
            stabilize: (execs, _, __, ___) =>
            {
                stabilized = execs;
                return Task.CompletedTask;
            },
            waitDerived: _ =>
            {
                waitCalled = true;
                return Task.CompletedTask;
            });

        var stage = new PersistentQueryStage(deps);
        await stage.ExecuteAsync(context, CreateRequest(baseModel));

        Assert.NotNull(stabilized);
        var single = Assert.Single(stabilized!);
        Assert.Equal("Q1", single.QueryId);
        Assert.Same(includeExecution.Model, single.TargetModel);
        Assert.True(waitCalled);
        Assert.Equal("bar_1m_live", capturedTopic);
        Assert.Equal(includeExecution.Statement, capturedStatement);
        Assert.Equal("Q1", Assert.Single(context.PersistentExecutions).QueryId);
    }

    [Fact]
    public async Task RowMonitorStage_StartsMonitorWithExecutions()
    {
        var baseModel = new EntityModel { EntityType = typeof(object) };
        var modelA = new EntityModel { EntityType = typeof(object), TopicName = "bar_1m_live" };
        var modelB = new EntityModel { EntityType = typeof(object), TopicName = "bar_5m_live" };

        var execA = CreateExecution(modelA, Role.Live, "bar_1m_live");
        var execB = CreateExecution(modelB, Role.Live, "bar_5m_live");

        var context = new UnifiedPipelineContext(typeof(object), baseModel, new[] { execA, execB });

        IReadOnlyList<DerivedTumblingPipeline.ExecutionResult>? monitorInput = null;
        var deps = CreateDependencies(
            startRowMonitor: execs =>
            {
                monitorInput = execs;
                return Task.CompletedTask;
            },
            isRowsRole: m => string.Equals(m.TopicName, "bar_1m_live", StringComparison.OrdinalIgnoreCase));

        var stage = new RowMonitorStage(deps);
        await stage.ExecuteAsync(context, CreateRequest(baseModel));

        Assert.NotNull(monitorInput);
        Assert.Equal(2, monitorInput!.Count);
        Assert.Contains(execA, monitorInput);
        Assert.Contains(execB, monitorInput);
    }

    private static EntityModel CreateRowsModel(string topic)
    {
        return new EntityModel
        {
            EntityType = typeof(RowsRecord),
            TopicName = topic,
            KeyProperties = new[]
            {
                typeof(RowsRecord).GetProperty(nameof(RowsRecord.Broker))!,
                typeof(RowsRecord).GetProperty(nameof(RowsRecord.Symbol))!
            },
            AllProperties = typeof(RowsRecord).GetProperties(),
            QueryModel = new Ksql.Linq.Query.Dsl.KsqlQueryModel()
        };
    }

    private static DerivedTumblingPipeline.ExecutionResult CreateExecution(
        EntityModel model,
        Role role,
        string targetTopic,
        string? inputTopic = "source_topic",
        string? statement = null)
    {
        return new DerivedTumblingPipeline.ExecutionResult(
            Model: model,
            Role: role,
            Statement: statement ?? $"CREATE TABLE {targetTopic} AS SELECT * FROM source EMIT CHANGES;",
            InputTopic: inputTopic,
            Response: new KsqlDbResponse(true, string.Empty),
            QueryId: null);
    }

    private static UnifiedPipelineRequest CreateRequest(EntityModel baseModel)
    {
        var queryModel = baseModel.QueryModel ?? new Ksql.Linq.Query.Dsl.KsqlQueryModel();
        return new UnifiedPipelineRequest(
            baseModel.EntityType ?? typeof(object),
            baseModel,
            queryModel,
            new MappingRegistry(),
            new ConcurrentDictionary<Type, EntityModel>(),
            NullLogger.Instance,
            (_, __) => Task.FromResult(new KsqlDbResponse(true, string.Empty)),
            null);
    }

    private static KsqlQueryDdlMonitor.Dependencies CreateDependencies(
        IKsqlDbClient? ksqlClient = null,
        Func<string, Task<KsqlDbResponse>>? executeStatement = null,
        Func<IReadOnlyList<PersistentQueryExecution>, EntityModel?, TimeSpan, CancellationToken, Task>? stabilize = null,
        Func<TimeSpan, Task>? waitDerived = null,
        Func<string, string?, Task<string?>>? tryGetQueryId = null,
        Func<IReadOnlyList<DerivedTumblingPipeline.ExecutionResult>, Task>? startRowMonitor = null,
        Func<RuntimeEvent, Task>? publishEvent = null,
        Func<EntityModel, bool>? isRowsRole = null,
        Func<Func<string, Task<KsqlDbResponse>>, Func<Task<HashSet<string>>>, Func<Task<HashSet<string>>>, EntityModel, Task>? ensureRowsLast = null)
    {
        var options = new KsqlDslOptions
        {
            KsqlDdlRetryCount = 0,
            KsqlDdlRetryInitialDelayMs = 0
        };

        return new KsqlQueryDdlMonitor.Dependencies
        {
            Logger = NullLogger.Instance,
            Options = options,
            MappingRegistry = new MappingRegistry(),
            EntityModels = new ConcurrentDictionary<Type, EntityModel>(),
            KsqlDbClient = ksqlClient ?? new StubKsqlDbClient(),
            ExecuteStatementAsync = executeStatement ?? (_ => Task.FromResult(new KsqlDbResponse(true, string.Empty))),
            RegisterQueryModelMapping = _ => { },
            WaitForEntityDdlAsync = (_, __) => Task.CompletedTask,
            AlignDerivedMappingWithSchemaAsync = _ => Task.CompletedTask,
            StabilizePersistentQueriesAsync = stabilize ?? ((_, __, ___, ____) => Task.CompletedTask),
            StartRowMonitorAsync = startRowMonitor ?? (_ => Task.CompletedTask),
            WaitForDerivedQueriesRunningAsync = waitDerived ?? (_ => Task.CompletedTask),
            DelayAsync = (_, __) => Task.CompletedTask,
            EnsureRowsLastTableAsync = ensureRowsLast ?? ((execute, getTables, getStreams, model) => Task.CompletedTask),
            WaitForQueryRunningAsync = (_, __, ___) => Task.CompletedTask,
            TryGetQueryIdFromShowQueriesAsync = tryGetQueryId ?? ((_, __) => Task.FromResult<string?>(null)),
            TerminateQueriesAsync = _ => Task.CompletedTask,
            AssertTopicPartitionsAsync = _ => Task.CompletedTask,
            PublishEventAsync = publishEvent ?? (_ => Task.CompletedTask),
            WaitForPersistentQueryAsync = (_, __, ___) => Task.CompletedTask,
            GetPersistentQueryMaxAttempts = () => 1,
            GetPersistentQueryTimeout = () => TimeSpan.FromSeconds(5),
            GetQueryRunningTimeout = () => TimeSpan.FromSeconds(5),
            IsRowsRole = isRowsRole ?? (_ => false)
        };
    }

    private sealed class RowsRecord
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string BrokerHead { get; set; } = string.Empty;
        public double RoundAvg1 { get; set; }
    }
}