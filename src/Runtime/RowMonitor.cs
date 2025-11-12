using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using Ksql.Linq.Messaging;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Window;
using Ksql.Linq.Query.Metadata;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Events;

namespace Ksql.Linq.Runtime;

internal interface IRowMonitorController
{
    void Start(CancellationToken token);
    Task StopAsync();
    IDisposable Subscribe(Func<RowMonitorEvent, Task> handler);
}

internal readonly record struct RowMonitorEvent(
    object Payload,
    IReadOnlyDictionary<string, object?>? KeyParts,
    DateTimeOffset RecordTimestampUtc,
    TimeSpan GraceWindow,
    MessageMeta Meta,
    IReadOnlyDictionary<string, string> Headers);

internal sealed class RowMonitor<TSource, TRow> : IRowMonitorController, IAsyncDisposable
    where TSource : class
    where TRow : class
{
    private static readonly TimeSpan WindowSize = TimeSpan.FromSeconds(1);

    private readonly KsqlContext _context;
    private readonly EntityModel _sourceModel;
    private readonly EntityModel _targetModel;
    private readonly KsqlQueryModel _queryModel;
    private readonly ILogger? _logger;
    private readonly TimeSpan _flushInterval;
    private readonly TimeSpan _graceWindow;
    private readonly Func<DateTimeOffset> _utcNow;
    private readonly string _targetTopic;
    private readonly string _sourceTopic;
    private readonly KeyValueTypeMapping? _targetMapping;
    private readonly QueryMetadata _targetMetadata;
    private readonly bool _requireAssignment;
    private string[] _keyNames = Array.Empty<string>();

    private readonly Func<TSource, bool>? _wherePredicate;
    private readonly Func<TSource, object> _groupKeySelector;
    private readonly Delegate _selectDelegate;
    private readonly Func<TSource, DateTime> _eventTimeSelector;
    private readonly Type _groupKeyType;

    private readonly object _lifecycleSync = new();
    private readonly object _subscriberSync = new();
    private readonly object _pendingSync = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastEventTimeByKey = new(StringComparer.Ordinal);

    private CancellationTokenSource? _cts;
    private Task? _consumeTask;
    private IEntitySet<TSource>? _sourceSet;
    private EventSet<TSource>? _sourceEventSet;
    private IEntitySet<TRow>? _targetSet;
    private WindowAggregator<TSource, object, FlushEnvelope>? _aggregator;

    private readonly List<Func<RowMonitorEvent, Task>> _subscribers = new();
    private readonly Dictionary<string, PendingEntry> _pending = new();
    private string _runId = string.Empty;

    // Continuation (per-second filler) state
    private readonly bool _continuationEnabled = false;
    private sealed class ContinuationState
    {
        public object GroupKey { get; init; } = default!;
        public DateTime LastProcessedUtc { get; set; } = DateTime.MinValue;
        public decimal? LastClose { get; set; }
        public IReadOnlyDictionary<string, object?>? KeyParts { get; set; }
    }
    private readonly ConcurrentDictionary<string, ContinuationState> _continuation = new(StringComparer.Ordinal);
    private readonly string _bucketColumnName = "BucketStart";

    private sealed record PendingEntry(
        object GroupKey,
        DateTime BucketStartUtc,
        Dictionary<string, string> Headers,
        MessageMeta LastMeta,
        DateTimeOffset LastRecordTimestampUtc,
        int Count);

    private sealed record FlushEnvelope(
        TRow Row,
        object GroupKey,
        DateTime BucketStartUtc,
        DateTime BucketEndUtc,
        IReadOnlyList<TSource> Elements);

    public RowMonitor(
        KsqlContext context,
        EntityModel sourceModel,
        EntityModel targetModel,
        KsqlQueryModel queryModel,
        ILogger? logger,
        TimeSpan? flushInterval = null)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _sourceModel = sourceModel ?? throw new ArgumentNullException(nameof(sourceModel));
        _targetModel = targetModel ?? throw new ArgumentNullException(nameof(targetModel));
        _queryModel = queryModel ?? throw new ArgumentNullException(nameof(queryModel));
        _logger = logger;

        _targetTopic = GetTopicName(_targetModel);
        _sourceTopic = GetTopicName(_sourceModel);
        _targetMapping = ResolveMapping(_context, _targetModel, _logger);
        _targetMetadata = _targetModel.GetOrCreateMetadata();
        _logger?.LogWarning("Row monitor target type={TargetType} rowType={RowType}", _targetModel.EntityType, typeof(TRow));
        try
        {
            var names = _targetModel?.KeyProperties?.Select(p => p.Name).ToArray();
            if (names == null || names.Length == 0)
            {
                var meta = _targetMapping?.KeyProperties;
                names = meta != null && meta.Length > 0 ? meta.Select(m => m.Name).ToArray() : null;
            }
            if ((names == null || names.Length == 0))
            {
                var metaKeys = _targetMetadata.Keys.Names;
                if (metaKeys.Length > 0)
                    names = metaKeys;
            }
            _keyNames = names ?? Array.Empty<string>();
        }
        catch { _keyNames = Array.Empty<string>(); }

        _flushInterval = flushInterval ?? TimeSpan.FromSeconds(1);
        _graceWindow = TimeSpan.FromSeconds(_queryModel.GraceSeconds ?? 0);
        _utcNow = static () => DateTimeOffset.UtcNow;

        // Feature flags from query model extras (defaults are conservative for OSS)
        _requireAssignment = TryGetBoolExtra(_queryModel, "requireAssignment") ?? true; // default: require Kafka assignment

        _wherePredicate = CompileWherePredicate(_queryModel.WhereCondition);

        // Prefer original group key type; rewriting to tuples can break select projections that rely on anonymous members
        var groupExpr = _queryModel.GroupByExpression;
        _groupKeyType = groupExpr?.ReturnType ?? typeof(TSource);
        _groupKeySelector = CompileGroupKeySelector(groupExpr);
        var projectionString = _queryModel.SelectProjection?.ToString() ?? "(null)";
        _selectDelegate = CompileSelectProjection(_queryModel.SelectProjection);
        _logger?.LogWarning("Row monitor select projection for {Target}: {Projection}", _targetTopic, projectionString);
        _eventTimeSelector = ResolveEventTimeSelector(_queryModel, logger);

        // Enable continuation only for 1s rows targets when DSL requested it
        try
        {
            var identifier = _targetMetadata.Identifier;
            var isRows = string.Equals(_targetMetadata.Role, "Final1sStream", StringComparison.OrdinalIgnoreCase)
                && !string.IsNullOrWhiteSpace(identifier)
                && identifier!.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase);
            _continuationEnabled = isRows && (_queryModel?.Continuation ?? false);
            // Resolve bucket column name (from query model or best-effort inference)
            var col = _queryModel?.BucketColumnName;
            if (!string.IsNullOrWhiteSpace(col))
            {
                _bucketColumnName = col!;
            }
            else
            {
                try
                {
                    // Prefer property containing "bucket" in target entity
                    var p = typeof(TRow).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                        .FirstOrDefault(pi => pi.Name.IndexOf("bucket", StringComparison.OrdinalIgnoreCase) >= 0);
                    if (p != null) _bucketColumnName = p.Name;
                }
                catch { }
            }
        }
        catch { _continuationEnabled = false; }
    }

    // テスト用: 時刻供給関数を注入可能なオーバーロード
    public RowMonitor(
        KsqlContext context,
        EntityModel sourceModel,
        EntityModel targetModel,
        KsqlQueryModel queryModel,
        ILogger? logger,
        TimeSpan? flushInterval,
        Func<DateTimeOffset> utcNowProvider)
        : this(context, sourceModel, targetModel, queryModel, logger, flushInterval)
    {
        _utcNow = utcNowProvider ?? (() => DateTimeOffset.UtcNow);
    }

    public void Start(CancellationToken token)
    {
        lock (_lifecycleSync)
        {
            if (_consumeTask != null)
            {
                return;
            }

            _sourceSet = _context.Set<TSource>();
            _targetSet = _context.Set<TRow>();
            _sourceEventSet = _sourceSet as EventSet<TSource>;
            if (_sourceEventSet == null)
            {
                throw new InvalidOperationException($"Entity set for {typeof(TSource).Name} must derive from EventSet.");
            }

            EnsureAggregator();

            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            var linkedToken = _cts.Token;

            _runId = $"row-monitor-{_targetTopic}-{_utcNow().ToUnixTimeMilliseconds()}";
            _logger?.LogInformation(
                "Row monitor hub start runId={RunId} sourceEntity={SourceEntity} sourceTopic={SourceTopic} targetTopic={TargetTopic} flushIntervalMs={FlushMs} graceMs={GraceMs} windowSizeMs={WindowMs}",
                _runId,
                typeof(TSource).Name,
                _sourceTopic,
                _targetTopic,
                (long)_flushInterval.TotalMilliseconds,
                (long)_graceWindow.TotalMilliseconds,
                (long)WindowSize.TotalMilliseconds);

            _consumeTask = Task.Factory.StartNew(
                () => ConsumeWithAssignmentAsync(linkedToken),
                linkedToken,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default).Unwrap();
        }
    }

    private async Task ConsumeWithAssignmentAsync(CancellationToken token)
    {
        var manager = _context.GetConsumerManager();

        while (!token.IsCancellationRequested)
        {
            // 1) Wait until this instance owns the source topic (assigned), unless skipped for in-memory tests
            if (_requireAssignment)
            {
                try
                {
                    await WaitForTopicAssignmentAsync(_sourceTopic, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
            }

            // 2) Ensure fresh aggregator per-assignment
            try { _aggregator?.StopWithoutFlushAndClear(); } catch { }
            _aggregator = null;
            EnsureAggregator();

            // 3) Run consume loop until partitions are revoked for this topic
            using var runCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            void OnRevoked(System.Collections.Generic.IReadOnlyList<Confluent.Kafka.TopicPartitionOffset> parts)
            {
                if (parts != null && parts.Any(p => string.Equals(p.Topic, _sourceTopic, StringComparison.Ordinal)))
                {
                    try { _logger?.LogDebug("Row monitor observed revocation for topic {Topic}; stopping consumption", _sourceTopic); } catch { }
                    try
                    {
                        // Emit leader-loss event for hub consumer
                        RuntimeEventBus.PublishAsync(new RuntimeEvent
                        {
                            Name = "hub.consumer.leader",
                            Phase = "lost",
                            Entity = typeof(TSource).Name,
                            Topic = _sourceTopic,
                            Message = "Hub consumer assignment revoked (leader lost)",
                            Success = true
                        });
                    }
                    catch { }
                    runCts.Cancel();
                }
            }
            manager.PartitionsRevoked += OnRevoked;
            try
            {
                // Start filler loop for continuation when enabled; tied to this assignment lifetime
                Task? filler = null;
                if (_continuationEnabled)
                {
                    var fillerToken = runCts.Token;
                    filler = Task.Run(async () =>
                    {
                        while (!fillerToken.IsCancellationRequested)
                        {
                            try { await EmitContinuationFillersAsync(_utcNow().UtcDateTime, fillerToken).ConfigureAwait(false); } catch { }
                            try { await Task.Delay(TimeSpan.FromSeconds(1), fillerToken).ConfigureAwait(false); } catch { break; }
                        }
                    }, fillerToken);
                }
                try
                {
                    await ConsumeAsync(runCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    if (token.IsCancellationRequested) break; // overall shutdown
                }
                // After normal completion, flush pending windows; if revoked, drop without flush.
                try
                {
                    if (runCts.IsCancellationRequested)
                    {
                        _aggregator?.StopWithoutFlushAndClear();
                    }
                    else if (_aggregator != null)
                    {
                        try { await _aggregator.FlushAsync(CancellationToken.None).ConfigureAwait(false); } catch { }
                        try { await _aggregator.DisposeAsync().ConfigureAwait(false); } catch { }
                    }
                    if (filler != null)
                    {
                        try { await filler.ConfigureAwait(false); } catch { }
                    }
                }
                catch { }
                finally
                {
                    _aggregator = null;
                }
            }
            finally
            {
                manager.PartitionsRevoked -= OnRevoked;
            }

            // Loop back to wait for next assignment (standby->active など)
        }
    }

    private async Task WaitForTopicAssignmentAsync(string topic, CancellationToken token)
    {
        // Gate start until this instance gets an active assignment for the source topic.
        // This uses KafkaConsumerManager's partition assignment events (HBなし構成対応)。
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        void OnAssigned(System.Collections.Generic.IReadOnlyList<Confluent.Kafka.TopicPartition> parts)
        {
            if (parts != null && parts.Any(p => string.Equals(p.Topic, topic, StringComparison.Ordinal)))
            {
                try
                {
                    RuntimeEventBus.PublishAsync(new RuntimeEvent
                    {
                        Name = "hub.consumer.leader",
                        Phase = "elected",
                        Entity = typeof(TSource).Name,
                        Topic = topic,
                        Message = "Hub consumer assignment acquired (leader elected)",
                        Success = true
                    });
                }
                catch { }
                tcs.TrySetResult(true);
            }
        }
        void OnRevoked(System.Collections.Generic.IReadOnlyList<Confluent.Kafka.TopicPartitionOffset> parts)
        {
            // No-op for now; start is one-shot on first assignment
        }

        var manager = _context.GetConsumerManager();
        manager.PartitionsAssigned += OnAssigned;
        manager.PartitionsRevoked += OnRevoked;
        try
        {
            using (token.Register(() => tcs.TrySetCanceled(token)))
            {
                // Best-effort timeout to avoid indefinite wait in edge cases
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(60), token);
                var winner = await Task.WhenAny(tcs.Task, timeoutTask).ConfigureAwait(false);
                if (winner != tcs.Task)
                    return; // timeout or cancellation
                _logger?.LogDebug("Row monitor assignment observed for topic {Topic}", topic);
            }
        }
        finally
        {
            manager.PartitionsAssigned -= OnAssigned;
            manager.PartitionsRevoked -= OnRevoked;
        }
    }

    public async Task StopAsync()
    {
        CancellationTokenSource? cts;
        Task? consume;
        lock (_lifecycleSync)
        {
            cts = _cts;
            consume = _consumeTask;
            _cts = null;
            _consumeTask = null;
        }

        if (cts != null)
        {
            try { cts.Cancel(); } catch { }
        }

        if (consume != null)
        {
            try { await consume.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }

        if (_aggregator != null)
        {
            try
            {
                await _aggregator.FlushAsync(CancellationToken.None).ConfigureAwait(false);
                await _aggregator.DisposeAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            _aggregator = null;
        }

        lock (_pendingSync)
        {
            _pending.Clear();
        }

        cts?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    public IDisposable Subscribe(Func<RowMonitorEvent, Task> handler)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        lock (_subscriberSync)
        {
            _subscribers.Add(handler);
        }

        return new Subscription(this, handler);
    }

    private async Task ConsumeAsync(CancellationToken token)
    {
        var sourceSet = _sourceSet ?? throw new InvalidOperationException("Source set not initialised.");

        _logger?.LogInformation(
            "Row monitor runId={RunId} consuming sourceTopic={SourceTopic} targetTopic={TargetTopic} autoCommit={AutoCommit} fetchSize={FetchSize} flushIntervalMs={FlushMs} graceMs={GraceMs}",
            _runId,
            _sourceTopic,
            _targetTopic,
            false,
            ResolveFetchSize(_queryModel),
            (long)_flushInterval.TotalMilliseconds,
            (long)_graceWindow.TotalMilliseconds);

        await sourceSet.ForEachAsync((entity, headers, meta) =>
        {
            token.ThrowIfCancellationRequested();

            if (_wherePredicate != null && !_wherePredicate(entity))
            {
                return Task.CompletedTask;
            }

            var eventTimeUtc = FloorToSecond(_eventTimeSelector(entity));
            var groupKey = _groupKeySelector(entity);
            var bufferKey = BuildBufferKey(groupKey, eventTimeUtc);
            // Monotonic guard: drop out-of-order events earlier than the last seen event time per key
            try
            {
                var stableKey = DescribeGroupKey(groupKey);
                if (_lastEventTimeByKey.TryGetValue(stableKey, out var lastSeen))
                {
                    if (eventTimeUtc < lastSeen)
                    {
                        _logger?.LogWarning(
                            "Out-of-order event dropped for key {Key}: ts={EventTs:o} < last={LastTs:o}",
                            stableKey, eventTimeUtc, lastSeen);
                        return Task.CompletedTask;
                    }
                }
                _ = _lastEventTimeByKey.AddOrUpdate(stableKey, eventTimeUtc, (_, old) => old >= eventTimeUtc ? old : eventTimeUtc);
            }
            catch { }
            var headerCopy = headers != null
                ? new Dictionary<string, string>(headers, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            int updatedCount;
            bool isNewBucket;

            lock (_pendingSync)
            {
                if (_pending.TryGetValue(bufferKey, out var existing))
                {
                    _pending[bufferKey] = existing with
                    {
                        Headers = headerCopy,
                        LastMeta = meta,
                        LastRecordTimestampUtc = meta.TimestampUtc,
                        Count = existing.Count + 1
                    };
                    updatedCount = existing.Count + 1;
                    isNewBucket = false;
                }
                else
                {
                    _pending[bufferKey] = new PendingEntry(
                        groupKey,
                        eventTimeUtc,
                        headerCopy,
                        meta,
                        meta.TimestampUtc,
                        1);
                    updatedCount = 1;
                    isNewBucket = true;
                }
            }

            var level = isNewBucket ? LogLevel.Information : LogLevel.Debug;
            if (_logger != null && _logger.IsEnabled(level))
            {
                _logger.Log(
                    level,
                    "Row monitor runId={RunId} received sourceEntity={SourceEntity} key={Key} bucketStart={BucketStart:o} eventTimestamp={EventTimestamp:o} headerCount={HeaderCount} sequence={Sequence}",
                    _runId,
                    typeof(TSource).Name,
                    DescribeGroupKey(groupKey),
                    eventTimeUtc,
                    meta.TimestampUtc,
                    headerCopy.Count,
                    updatedCount);
            }

            _aggregator!.ProcessMessage(entity);
            return Task.CompletedTask;
        }, autoCommit: false, cancellationToken: token).ConfigureAwait(false);
    }

    private void EnsureAggregator()
    {
        if (_aggregator != null)
        {
            return;
        }

        // Policy adjustments for 1s_rows (Final1sStream):
        // - Do not delay emission by grace (grace=0)
        // - Sweep at 1-second cadence, not faster
        var graceForAggregator = _graceWindow;
        var sweepForAggregator = _flushInterval;
        try
        {
            if (string.Equals(_targetMetadata.Role, "Final1sStream", StringComparison.OrdinalIgnoreCase))
            {
                graceForAggregator = TimeSpan.Zero;
                sweepForAggregator = WindowSize; // enforce 1s cadence
            }
        }
        catch { }

        // Build a best-effort deduplication key: concatenate KsqlKey properties and Timestamp
        Func<TSource, object?> deduplicationKeySelector = (s) =>
        {
            try
            {
                var t = typeof(TSource);
                var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                var keyParts = new System.Collections.Generic.List<string>(4);
                foreach (var p in props)
                {
                    try
                    {
                        // Prefer KsqlKey-annotated properties
                        var isKey = p.GetCustomAttribute<KsqlKeyAttribute>(inherit: true) != null;
                        if (!isKey) continue;
                        var v = p.GetValue(s);
                        keyParts.Add(Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty);
                    }
                    catch { }
                }
                // Append timestamp
                try
                {
                    var tsProp = props.FirstOrDefault(pp => pp.GetCustomAttribute<KsqlTimestampAttribute>(inherit: true) != null)
                                  ?? props.FirstOrDefault(pp => string.Equals(pp.Name, "Timestamp", StringComparison.OrdinalIgnoreCase));
                    if (tsProp != null)
                    {
                        var ts = tsProp.GetValue(s);
                        if (ts is DateTime dt)
                            keyParts.Add(dt.ToUniversalTime().ToString("O"));
                        else
                            keyParts.Add(Convert.ToString(ts, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty);
                    }
                }
                catch { }
                return string.Join("|", keyParts);
            }
            catch { return null; }
        };

        _aggregator = new WindowAggregator<TSource, object, FlushEnvelope>(
            windowSize: WindowSize,
            gracePeriod: graceForAggregator,
            sweepInterval: sweepForAggregator,
            idleThreshold: sweepForAggregator + _graceWindow + sweepForAggregator,
            keySelector: _groupKeySelector,
            timestampSelector: s => FloorToSecond(_eventTimeSelector(s)),
            resultSelector: CreateFlushEnvelope,
            emitCallback: EmitAsync,
            emitFailureHandler: OnEmitFailureAsync,
            messageValidator: null,
            metrics: null,
            logger: _logger,
            utcNowProvider: () => DateTime.UtcNow,
            deduplicationKeySelector: deduplicationKeySelector);

        _aggregator.Start();
    }

    private FlushEnvelope CreateFlushEnvelope(IWindowGrouping<object, TSource> grouping)
    {
        var elements = grouping.ToList();
        TRow row;
        try
        {
            var typedGrouping = CreateWindowGrouping(grouping.Key, grouping.WindowStart, elements);
            var rawRow = _selectDelegate.DynamicInvoke(typedGrouping)!;
            row = rawRow is TRow direct
                ? direct
                : (TRow)ConvertToTargetRow(rawRow, grouping.Key, grouping.WindowStart, elements);
        }
        catch (MissingMethodException ex)
        {
            _logger?.LogWarning(ex, "Row monitor projection invoke failed (MissingMethod); falling back to reflection for {Target}", _targetTopic);
            row = (TRow)BuildRowViaReflection(typeof(TRow), grouping.Key, grouping.WindowStart, elements);
        }
        catch (ArgumentException ex)
        {
            _logger?.LogWarning(ex, "Row monitor projection invoke failed (ArgumentException); falling back to reflection for {Target}", _targetTopic);
            row = (TRow)BuildRowViaReflection(typeof(TRow), grouping.Key, grouping.WindowStart, elements);
        }
        catch (TargetParameterCountException ex)
        {
            _logger?.LogWarning(ex, "Row monitor projection invoke failed (ParameterCount); falling back to reflection for {Target}", _targetTopic);
            row = (TRow)BuildRowViaReflection(typeof(TRow), grouping.Key, grouping.WindowStart, elements);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Row monitor projection failed for {Target}", _targetTopic);
            throw;
        }

        return new FlushEnvelope(row, grouping.Key, grouping.WindowStart, grouping.WindowEnd, elements);
    }

    private async ValueTask EmitAsync(FlushEnvelope envelope, CancellationToken token)
    {
        if (_targetSet == null)
            throw new InvalidOperationException("Target set not initialised.");

        PendingEntry? entry = null;
        var bufferKey = BuildBufferKey(envelope.GroupKey, envelope.BucketStartUtc);
        lock (_pendingSync)
        {
            if (_pending.TryGetValue(bufferKey, out var existing))
            {
                entry = existing;
                _pending.Remove(bufferKey);
            }
        }

        var headers = entry?.Headers ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        MessageMeta meta;
        var metaTimestamp = new DateTimeOffset(envelope.BucketStartUtc, TimeSpan.Zero);
        if (entry != null)
        {
            meta = new MessageMeta(
                _targetTopic,
                Partition: 0,
                Offset: 0,
                TimestampUtc: metaTimestamp,
                SchemaIdKey: entry.LastMeta.SchemaIdKey,
                SchemaIdValue: entry.LastMeta.SchemaIdValue,
                KeyIsNull: false,
                HeaderAllowList: headers);
        }
        else
        {
            meta = new MessageMeta(
                _targetTopic,
                Partition: 0,
                Offset: 0,
                TimestampUtc: metaTimestamp,
                SchemaIdKey: null,
                SchemaIdValue: null,
                KeyIsNull: false,
                HeaderAllowList: headers);
        }

        await _targetSet.AddAsync(envelope.Row, headers, token).ConfigureAwait(false);
        if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Row monitor runId={RunId} produced row topic={Topic} key={KeyDesc} bucket={Bucket} payload={Payload}",
                _runId,
                _targetTopic,
                DescribeGroupKey(envelope.GroupKey),
                envelope.BucketStartUtc,
                DescribeRow(envelope.Row));
        }

        if (_sourceEventSet != null)
        {
            foreach (var entity in envelope.Elements)
            {
                _sourceEventSet.Commit(entity);
            }
        }

        var keyParts = ExtractKeyParts(envelope.Row);
        var evt = new RowMonitorEvent(envelope.Row, keyParts, meta.TimestampUtc, _graceWindow, meta, headers);
        await NotifySubscribersAsync(evt).ConfigureAwait(false);

        // Continuation (gap-filling) between previous processed second and this real second - 1
        if (_continuationEnabled)
        {
            try
            {
                var stableKey = DescribeGroupKey(envelope.GroupKey);
                var state = _continuation.GetOrAdd(stableKey, _ => new ContinuationState { GroupKey = envelope.GroupKey, KeyParts = keyParts });
                var prevLast = state.LastProcessedUtc;
                var prevClose = state.LastClose;
                if (prevLast != DateTime.MinValue && prevClose != null)
                {
                    var end = envelope.BucketStartUtc.AddSeconds(-1);
                    var next = prevLast.AddSeconds(1);
                    for (var t = next; t <= end; t = t.AddSeconds(1))
                    {
                        var rowFill = BuildSyntheticRow(typeof(TRow), state.GroupKey, state.KeyParts, _bucketColumnName, t, prevClose.Value, _keyNames);
                        try { await _targetSet.AddAsync((TRow)rowFill, null, token).ConfigureAwait(false); }
                        catch { break; }
                        state.LastProcessedUtc = t;
                    }
                }
            }
            catch { }
        }

        // Update continuation state using this real emission (so future fillers won't duplicate it)
        if (_continuationEnabled)
        {
            try
            {
                var stableKey = DescribeGroupKey(envelope.GroupKey);
                var state = _continuation.GetOrAdd(stableKey, _ => new ContinuationState { GroupKey = envelope.GroupKey, KeyParts = keyParts });
                state.LastProcessedUtc = envelope.BucketStartUtc > state.LastProcessedUtc ? envelope.BucketStartUtc : state.LastProcessedUtc;
                var closeVal = TryReadCloseDecimal(envelope.Row);
                if (closeVal != null) state.LastClose = closeVal;
                if (keyParts != null && keyParts.Count > 0) state.KeyParts = keyParts;
            }
            catch { }
        }

        if (entry != null)
        {
            var reason = DetermineFlushReason(entry);
            _logger?.LogDebug(
                "Row monitor runId={RunId} flushed {Target} bucket {BucketStart} via {Reason} after {Count} events",
                _runId,
                _targetTopic,
                envelope.BucketStartUtc,
                reason,
                entry.Count);
        }

        // ops-lag warning removed by policy; external monitoring should be used.
    }

    private ValueTask OnEmitFailureAsync(FlushEnvelope envelope, Exception exception, CancellationToken token)
    {
        _logger?.LogError(exception, "Row monitor runId={RunId} flush failed for {Target} bucket {BucketStart}", _runId, _targetTopic, envelope.BucketStartUtc);
        return ValueTask.CompletedTask;
    }

    private string DetermineFlushReason(PendingEntry entry)
    {
        if (_graceWindow <= TimeSpan.Zero)
        {
            return "timer";
        }

        var expected = entry.LastRecordTimestampUtc + _graceWindow;
        return DateTimeOffset.UtcNow >= expected ? "grace" : "timer";
    }

    private string BuildBufferKey(object groupKey, DateTime bucketStartUtc)
    {
        var sb = new StringBuilder();
        AppendKeyComponents(groupKey, sb);
        if (sb.Length > 0)
        {
            sb.Append('|');
        }
        sb.Append(bucketStartUtc.ToString("O"));
        return sb.ToString();
    }

    private static string DescribeGroupKey(object groupKey)
    {
        var sb = new StringBuilder();
        if (groupKey is ITuple tuple)
        {
            for (int i = 0; i < tuple.Length; i++)
            {
                if (i > 0) sb.Append('|');
                sb.Append(tuple[i]);
            }
        }
        else
        {
            AppendKeyComponents(groupKey, sb);
        }
        return sb.ToString();
    }

    private static string DescribeRow(object row)
    {
        if (row == null) return "(null)";
        var type = row.GetType();
        var props = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var sb = new StringBuilder();
        sb.Append('{');
        for (int i = 0; i < props.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            var p = props[i];
            object? value = null;
            try { value = p.GetValue(row); } catch { }
            sb.Append(p.Name);
            sb.Append('=');
            sb.Append(value);
        }
        sb.Append('}');
        return sb.ToString();
    }

    private static bool TryRewriteGroupByToTuple(LambdaExpression? groupExpr, out LambdaExpression rewritten, out Type keyType)
    {
        rewritten = null!;
        keyType = null!;
        if (groupExpr == null)
            return false;

        // Heuristic: if GroupBy constructs an anonymous new { ... } with 1..7 members,
        // rewrite to a ValueTuple<string,...> preserving original order.
        if (groupExpr.Body is NewExpression ne && ne.Arguments != null && ne.Arguments.Count >= 1 && ne.Arguments.Count <= 7)
        {
            var arity = ne.Arguments.Count;
            Type tupleType = arity switch
            {
                1 => typeof(ValueTuple<string>),
                2 => typeof(ValueTuple<string, string>),
                3 => typeof(ValueTuple<string, string, string>),
                4 => typeof(ValueTuple<string, string, string, string>),
                5 => typeof(ValueTuple<string, string, string, string, string>),
                6 => typeof(ValueTuple<string, string, string, string, string, string>),
                7 => typeof(ValueTuple<string, string, string, string, string, string, string>),
                _ => null!
            };
            if (tupleType != null)
            {
                var ctor = tupleType.GetConstructors().FirstOrDefault(ci =>
                {
                    var ps = ci.GetParameters();
                    return ps.Length == arity && ps.All(p => p.ParameterType == typeof(string));
                });
                if (ctor != null)
                {
                    var p = groupExpr.Parameters.Count == 1 ? groupExpr.Parameters[0] : Expression.Parameter(typeof(TSource), "x");
                    var args = new Expression[arity];
                    for (int i = 0; i < arity; i++)
                    {
                        var a = ne.Arguments[i];
                        if (a.Type != typeof(string)) a = Expression.Convert(a, typeof(string));
                        args[i] = a;
                    }
                    var body = Expression.New(ctor, args);
                    rewritten = Expression.Lambda(body, p);
                    keyType = tupleType;
                    return true;
                }
            }
        }
        return false;
    }

    private static int ResolveFetchSize(KsqlQueryModel model)
    {
        if (model.Extras.TryGetValue("fetchSize", out var value) && value != null)
        {
            switch (value)
            {
                case int i:
                    return i;
                case long l:
                    return (int)l;
                case string s when int.TryParse(s, out var parsed):
                    return parsed;
            }
        }
        return 0;
    }

    private static void AppendKeyComponents(object groupKey, StringBuilder sb)
    {
        if (groupKey == null)
        {
            sb.Append("null");
            return;
        }

        var type = groupKey.GetType();
        if (type.IsPrimitive || groupKey is string or Guid or DateTime or DateTimeOffset or decimal)
        {
            sb.Append(groupKey);
            return;
        }

        var props = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        for (var i = 0; i < props.Length; i++)
        {
            if (i > 0)
            {
                sb.Append('|');
            }
            var value = props[i].GetValue(groupKey);
            sb.Append(value);
        }
    }

    private async Task NotifySubscribersAsync(RowMonitorEvent evt)
    {
        Func<RowMonitorEvent, Task>[] snapshot;
        lock (_subscriberSync)
        {
            snapshot = _subscribers.ToArray();
        }

        foreach (var subscriber in snapshot)
        {
            try
            {
                await subscriber(evt).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Row monitor subscriber failed for {Target}", _targetTopic);
            }
        }
    }

    private static KeyValueTypeMapping? ResolveMapping(KsqlContext context, EntityModel model, ILogger? logger)
    {
        try
        {
            return context.GetMappingRegistry().GetMapping(model.EntityType);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Failed to resolve mapping for {Entity}", model.EntityType.Name);
            return null;
        }
    }

    private async ValueTask EmitContinuationFillersAsync(DateTime nowUtc, CancellationToken token)
    {
        if (!_continuationEnabled || _targetSet == null)
            return;
        var floorNow = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, nowUtc.Minute, nowUtc.Second, DateTimeKind.Utc);
        var cutoff = floorNow.AddSeconds(-1);
        foreach (var kv in _continuation.ToArray())
        {
            token.ThrowIfCancellationRequested();
            var state = kv.Value;
            if (state.LastClose == null || state.GroupKey == null)
                continue;
            if (state.LastProcessedUtc == DateTime.MinValue)
                continue;
            var next = state.LastProcessedUtc.AddSeconds(1);
            for (var t = next; t <= cutoff; t = t.AddSeconds(1))
            {
                var row = BuildSyntheticRow(typeof(TRow), state.GroupKey, state.KeyParts, _bucketColumnName, t, state.LastClose!.Value, _keyNames);
                try { await _targetSet.AddAsync((TRow)row, null, token).ConfigureAwait(false); } catch { break; }
                state.LastProcessedUtc = t;
            }
        }
    }

    private static object BuildSyntheticRow(Type rowType, object groupKey, IReadOnlyDictionary<string, object?>? keyParts, string bucketColumnName, DateTime bucketStartUtc, decimal close, string[]? keyNames)
    {
        var row = Activator.CreateInstance(rowType)!;
        void TrySet(string name, object? value)
        {
            var p = rowType.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (p != null && p.CanWrite)
            {
                try { p.SetValue(row, value); } catch { }
            }
        }
        // Prefer exact key column names from emitted row (KeyParts), fallback to groupKey members
        if (keyParts != null && keyParts.Count > 0)
        {
            foreach (var kv in keyParts)
                TrySet(kv.Key, kv.Value);
        }
        else if (groupKey != null)
        {
            if (groupKey is ITuple t)
            {
                if (keyNames != null && keyNames.Length > 0)
                {
                    for (int i = 0; i < t.Length && i < keyNames.Length; i++)
                        TrySet(keyNames[i], t[i]);
                }
                // no fixed-name fallback; if keyNames are unavailable, property-based reflection branch below will apply
            }
            else
            {
                var kt = groupKey.GetType();
                foreach (var p in kt.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    var val = p.GetValue(groupKey);
                    TrySet(p.Name, val);
                }
            }
        }
        TrySet(bucketColumnName, bucketStartUtc);
        TrySet("Timestamp", bucketStartUtc);
        // Assign terminal price only (Close or KsqlTimeFrameClose). Avoid fixed-name OHLC fan-out.
        var hasClose = rowType.GetProperty("Close", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase) != null;
        if (hasClose) TrySet("Close", ConvertPrice(rowType, "Close", close)); else TrySet("KsqlTimeFrameClose", ConvertPrice(rowType, "KsqlTimeFrameClose", close));
        return row;

        static object ConvertPrice(Type rowType, string prop, decimal value)
        {
            var p = rowType.GetProperty(prop, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)
                    ?? rowType.GetProperty("KsqlTimeFrameClose", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            var target = p?.PropertyType ?? typeof(decimal);
            if (target == typeof(decimal)) return value;
            if (target == typeof(double)) return (double)value;
            if (target == typeof(float)) return (float)value;
            if (target == typeof(long)) return (long)value;
            if (target == typeof(int)) return (int)value;
            return Convert.ChangeType(value, target);
        }
    }

    private object ConvertToTargetRow(object rawRow, object groupKey, DateTime bucketStartUtc, IReadOnlyList<TSource> elements)
    {
        if (rawRow == null)
            return BuildRowViaReflection(typeof(TRow), groupKey, bucketStartUtc, elements);

        if (rawRow is TRow direct)
            return direct;

        var row = BuildRowViaReflection(typeof(TRow), groupKey, bucketStartUtc, elements);
        CopyMatchingProperties(rawRow, row);
        try
        {
            var tsProp = row.GetType().GetProperty("Timestamp", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (tsProp != null && tsProp.CanWrite)
            {
                var current = tsProp.GetValue(row);
                if (current == null ||
                    (current is DateTime dt && dt == default) ||
                    (current is DateTimeOffset dto && dto == default))
                {
                    if (tsProp.PropertyType == typeof(DateTime) || tsProp.PropertyType == typeof(DateTime?))
                        tsProp.SetValue(row, bucketStartUtc);
                    else if (tsProp.PropertyType == typeof(DateTimeOffset) || tsProp.PropertyType == typeof(DateTimeOffset?))
                        tsProp.SetValue(row, new DateTimeOffset(bucketStartUtc, TimeSpan.Zero));
                }
            }
        }
        catch { }
        return row;
    }

    private static void CopyMatchingProperties(object source, object target)
    {
        if (source == null || target == null)
            return;

        var sourceProps = source.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
        if (sourceProps.Length == 0)
            return;

        var targetProps = target.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);

        foreach (var sp in sourceProps)
        {
            if (!sp.CanRead)
                continue;
            if (!targetProps.TryGetValue(sp.Name, out var tp) || tp.SetMethod == null)
                continue;
            try
            {
                var value = sp.GetValue(source);
                if (value != null && !tp.PropertyType.IsInstanceOfType(value))
                {
                    var targetType = Nullable.GetUnderlyingType(tp.PropertyType) ?? tp.PropertyType;
                    value = Convert.ChangeType(value, targetType);
                }
                tp.SetValue(target, value);
            }
            catch
            {
                // ignore conversion issues
            }
        }
    }

    private static decimal? TryReadCloseDecimal(object row)
    {
        try
        {
            var t = row.GetType();
            var p = t.GetProperty("Close", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)
                    ?? t.GetProperty("KsqlTimeFrameClose", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (p == null) return null;
            var v = p.GetValue(row);
            if (v == null) return null;
            if (v is decimal dec) return dec;
            if (v is double d) return (decimal)d;
            if (v is float f) return (decimal)f;
            if (v is long l) return l;
            if (v is int i) return i;
            return Convert.ToDecimal(v);
        }
        catch { return null; }
    }

    private static string GetTopicName(EntityModel model)
        => model.GetTopicName();

    private Func<TSource, bool>? CompileWherePredicate(Expression? whereExpression)
    {
        if (whereExpression == null)
            return null;

        var param = Expression.Parameter(typeof(TSource), "x");
        var body = RebindParameter(whereExpression, param);
        var lambda = Expression.Lambda<Func<TSource, bool>>(body, param);
        return lambda.Compile();
    }

    private Func<TSource, object> CompileGroupKeySelector(Expression? groupExpression)
    {
        if (groupExpression == null)
        {
            return _ => _ ?? throw new InvalidOperationException("GroupBy source cannot be null");
        }

        var param = Expression.Parameter(typeof(TSource), "x");
        // If the incoming expression is itself a lambda (Func<TSource, ...>), use its Body;
        // otherwise treat the expression as a value expression over the source parameter.
        var expr = groupExpression is LambdaExpression le ? le.Body : groupExpression;
        var body = RebindParameter(expr, param);
        var converted = Expression.Convert(body, typeof(object));
        var lambda = Expression.Lambda<Func<TSource, object>>(converted, param);
        return lambda.Compile();
    }

    private Delegate CompileSelectProjection(LambdaExpression? selectProjection)
    {
        if (selectProjection == null)
            throw new InvalidOperationException("Select projection is required for row monitoring.");
        return selectProjection.Compile();
    }

    private static Expression RebindParameter(Expression expression, ParameterExpression parameter)
    {
        return new ParameterReplacer(parameter).Visit(expression) ?? throw new InvalidOperationException("Failed to rebind parameter.");
    }

    private Func<TSource, DateTime> ResolveEventTimeSelector(KsqlQueryModel model, ILogger? logger)
    {
        if (!string.IsNullOrWhiteSpace(model.TimeKey))
        {
            return CreateTimeAccessor(model.TimeKey!);
        }

        var timestampProp = typeof(TSource).GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(p => p.GetCustomAttributes(true).OfType<KsqlTimestampAttribute>().Any());
        if (timestampProp != null)
        {
            return CreateTimeAccessor(timestampProp.Name);
        }

        logger?.LogWarning("No timestamp selector found for {Source}; defaulting to UtcNow", typeof(TSource).Name);
        return _ => DateTime.UtcNow;

        Func<TSource, DateTime> CreateTimeAccessor(string propertyName)
        {
            var prop = typeof(TSource).GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (prop == null)
            {
                throw new InvalidOperationException($"Timestamp property {propertyName} not found on {typeof(TSource).Name}.");
            }

            var param = Expression.Parameter(typeof(TSource), "x");
            var body = Expression.Property(param, prop);
            Expression converted;
            if (prop.PropertyType == typeof(DateTime))
            {
                converted = body;
            }
            else if (prop.PropertyType == typeof(DateTimeOffset))
            {
                converted = Expression.Property(body, nameof(DateTimeOffset.UtcDateTime));
            }
            else
            {
                throw new InvalidOperationException($"Timestamp property {propertyName} must be DateTime or DateTimeOffset");
            }

            var lambda = Expression.Lambda<Func<TSource, DateTime>>(converted, param);
            return lambda.Compile();
        }
    }

    private static DateTime FloorToSecond(DateTime value)
    {
        if (value.Kind == DateTimeKind.Unspecified)
        {
            value = DateTime.SpecifyKind(value, DateTimeKind.Utc);
        }
        var utc = value.ToUniversalTime();
        return new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, DateTimeKind.Utc);
    }

    private object CreateWindowGrouping(object groupKey, DateTime bucketStartUtc, IReadOnlyList<TSource> elements)
    {
        var groupingType = typeof(WindowGrouping<,>).MakeGenericType(_groupKeyType, typeof(TSource));
        var bucketEnd = bucketStartUtc.Add(WindowSize);
        var ctor = groupingType.GetConstructor(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            types: new[] { _groupKeyType, typeof(DateTime), typeof(DateTime), typeof(IReadOnlyList<>).MakeGenericType(typeof(TSource)) },
            modifiers: null);
        if (ctor == null)
            throw new MissingMethodException($"Constructor not found for {groupingType.FullName}");
        return ctor.Invoke(new object[] { groupKey, bucketStartUtc, bucketEnd, elements });
    }

    private object BuildRowViaReflection(Type rowType, object groupKey, DateTime bucketStartUtc, IReadOnlyList<TSource> elements)
    {
        var row = Activator.CreateInstance(rowType)!;
        void TrySet(string name, object? value)
        {
            var p = rowType.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            if (p != null && p.CanWrite)
            {
                try { p.SetValue(row, value); } catch { }
            }
        }
        if (groupKey is ITuple t)
        {
            if (_keyNames.Length > 0)
            {
                for (int i = 0; i < t.Length && i < _keyNames.Length; i++)
                    TrySet(_keyNames[i], t[i]);
            }
            // no fixed-name fallback; rely on property-based path below if key names are unknown
        }
        else if (groupKey != null)
        {
            var kt = groupKey.GetType();
            if (_keyNames.Length > 0)
            {
                foreach (var k in _keyNames)
                {
                    var v = kt.GetProperty(k, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)?.GetValue(groupKey);
                    if (v != null) TrySet(k, v);
                }
            }
            else
            {
                // no fixed-name fallback; attempt to copy all public properties as-is
                foreach (var p in kt.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    object? v = null; try { v = p.GetValue(groupKey); } catch { }
                    if (v != null) TrySet(p.Name, v);
                }
            }
        }
        // Fallback: if Broker/Symbol still null, derive from first element in window
        try
        {
            if (elements != null && elements.Count > 0)
            {
                var el0 = elements[0];
                var et = el0!.GetType();
                if (_keyNames.Length > 0)
                {
                    foreach (var k in _keyNames)
                    {
                        var cur = rowType.GetProperty(k, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)?.GetValue(row);
                        if (cur != null) continue;
                        var v = et.GetProperty(k, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)?.GetValue(el0);
                        if (v != null) TrySet(k, v);
                    }
                }
            }
        }
        catch { }
        TrySet("BucketStart", bucketStartUtc);
        // Aggregate within the 1s bucket in C# to mirror DSL semantics
        // earliest -> Open, latest -> Close, max -> High, min -> Low
        // Year is derived from BucketStart (UTC)
        decimal? o = null, h = null, l = null, c = null;
        foreach (var el in (elements ?? Array.Empty<TSource>()))
        {
            var v = ReadPrice(el);
            if (v == null) continue;
            if (o == null) o = v;
            if (h == null || v > h) h = v;
            if (l == null || v < l) l = v;
            c = v;
        }
        // Assign Year from bucket
        var yearProp = rowType.GetProperty("Year", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
        if (yearProp != null && yearProp.CanWrite)
        {
            try { yearProp.SetValue(row, bucketStartUtc.Year); } catch { }
        }
        // Assign OHLC (fallback: if Open is missing but Close exists, set Open=Close)
        if (c.HasValue)
        {
            if (rowType.GetProperty("Close", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase) != null)
                TrySet("Close", ConvertPrice(rowType, "Close", c.Value));
            else
                TrySet("KsqlTimeFrameClose", ConvertPrice(rowType, "KsqlTimeFrameClose", c.Value));
        }
        if (!o.HasValue && c.HasValue) o = c;
        if (o.HasValue && rowType.GetProperty("Open", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase) != null)
            TrySet("Open", ConvertPrice(rowType, "Open", o.Value));
        if (h.HasValue && rowType.GetProperty("High", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase) != null)
            TrySet("High", ConvertPrice(rowType, "High", h.Value));
        if (l.HasValue && rowType.GetProperty("Low", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase) != null)
            TrySet("Low", ConvertPrice(rowType, "Low", l.Value));
        return row;
        static decimal? ReadPrice(TSource el)
        {
            var t = el!.GetType();
            object? v = t.GetProperty("Bid", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)?.GetValue(el)
                        ?? t.GetProperty("Close", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)?.GetValue(el);
            if (v == null) return null;
            try { return Convert.ToDecimal(v); } catch { return null; }
        }
        static object ConvertPrice(Type rowType, string prop, decimal value)
        {
            var p = rowType.GetProperty(prop, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase)
                    ?? rowType.GetProperty("KsqlTimeFrameClose", BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            var target = p?.PropertyType ?? typeof(decimal);
            if (target == typeof(decimal)) return value;
            if (target == typeof(double)) return (double)value;
            if (target == typeof(float)) return (float)value;
            if (target == typeof(long)) return (long)value;
            if (target == typeof(int)) return (int)value;
            return Convert.ChangeType(value, target);
        }
    }

    private IReadOnlyDictionary<string, object?>? ExtractKeyParts(TRow row)
    {
        if (_targetMapping == null || _targetMapping.KeyProperties.Length == 0)
        {
            return null;
        }

        var dict = new Dictionary<string, object?>(_targetMapping.KeyProperties.Length, StringComparer.Ordinal);
        var runtimeType = row.GetType();
        foreach (var meta in _targetMapping.KeyProperties)
        {
            var prop = meta.PropertyInfo;
            if (prop != null)
            {
                var declaring = prop.DeclaringType;
                if (declaring != null && !declaring.IsAssignableFrom(runtimeType))
                    prop = null;
            }

            prop ??= runtimeType.GetProperty(meta.SourceName ?? meta.PropertyInfo?.Name ?? meta.Name,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

            if (prop == null)
            {
                continue;
            }

            dict[prop.Name] = prop.GetValue(row);
        }

        return dict;
    }

    // ops-lag warning removed

    private static bool? TryGetBoolExtra(KsqlQueryModel model, string key)
    {
        if (model?.Extras == null)
            return null;
        if (!model.Extras.TryGetValue(key, out var v) || v == null)
            return null;
        return v switch
        {
            bool b => b,
            string s when bool.TryParse(s, out var parsed) => parsed,
            int i => i != 0,
            long l => l != 0,
            _ => null
        };
    }

    private void RemoveSubscriber(Func<RowMonitorEvent, Task> handler)
    {
        lock (_subscriberSync)
        {
            _subscribers.Remove(handler);
        }
    }

    private sealed class Subscription : IDisposable
    {
        private readonly RowMonitor<TSource, TRow> _owner;
        private readonly Func<RowMonitorEvent, Task> _handler;
        private bool _disposed;

        public Subscription(RowMonitor<TSource, TRow> owner, Func<RowMonitorEvent, Task> handler)
        {
            _owner = owner;
            _handler = handler;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _owner.RemoveSubscriber(_handler);
        }
    }

    private sealed class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter;

        public ParameterReplacer(ParameterExpression parameter)
        {
            _parameter = parameter;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            return _parameter;
        }
    }
}

