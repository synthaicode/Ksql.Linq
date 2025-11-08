using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Events;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Threading;

[Topic("rates")]
public class Rate
{
    [KsqlKey(Order = 0)] public string Broker { get; set; } = "B1";
    [KsqlKey(Order = 1)] public string Symbol { get; set; } = "S1";
    [KsqlTimestamp] public System.DateTime Timestamp { get; set; }
    public decimal Price { get; set; }
}

public sealed class RuntimeEventsContext : KsqlContext
{
    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        // 最小: ソース定義（rows/hubや上位足は設計に依存）
        modelBuilder.Entity<Rate>();
    }
}

public sealed class ConsoleSink : IRuntimeEventSink
{
    public Task PublishAsync(RuntimeEvent e, CancellationToken ct = default)
    {
        Console.WriteLine($"[{e.TimestampUtc:O}] {e.Name}:{e.Phase} entity={e.Entity} topic={e.Topic} app={e.AppId} state={e.State} ok={e.Success} msg={e.Message}");
        return Task.CompletedTask;
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // 1) イベントシンクを登録（未設定なら発火しない）
        RuntimeEventBus.SetSink(new ConsoleSink());

        // 2) コンテキストを構築
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "KsqlDsl_")
            .Build();
        var loggerFactory = LoggerFactory.Create(b => b.AddConsole());

        await using var ctx = KsqlContextBuilder.Create()
            .UseConfiguration(configuration)
            .EnableLogging(loggerFactory)
            .BuildContext<RuntimeEventsContext>();

        // 3) rows_last の ready を待つ（例: bar_1s_rows_last）
        var rowsLastTopic = configuration["RowsLastTopic"] ?? "bar_1s_rows_last";
        var rowsReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = SubscribeOnce(e =>
        {
            if (e.Name == "rows_last.ready" && e.Phase == "done" && string.Equals(e.Topic, rowsLastTopic, StringComparison.OrdinalIgnoreCase))
                rowsReady.TrySetResult();
        });

        // 4) Streamiz RUNNING を待つ（例: bar_1m_live）
        var liveTopic = configuration["LiveTopic"] ?? "bar_1m_live";
        var running = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _s = SubscribeOnce(e =>
        {
            if (e.Name == "streamiz.state" && e.Phase == "running" && string.Equals(e.Topic, liveTopic, StringComparison.OrdinalIgnoreCase))
                running.TrySetResult();
        });

        // 任意: 並列に待つ（60s 以内）
        await Task.WhenAll(rowsReady.Task.WaitAsync(TimeSpan.FromSeconds(60)),
                           running.Task.WaitAsync(TimeSpan.FromSeconds(60)));

        Console.WriteLine("[sample] rows_last.ready + streamiz.running observed.");
    }

    private static IDisposable SubscribeOnce(Action<RuntimeEvent> onEvent)
    {
        var sink = new LambdaSink(e => { onEvent(e); return Task.CompletedTask; });
        // 既存シンクは保持したまま、個別の一時サブスクとして積む
        var current = RuntimeEventBus.Sink;
        RuntimeEventBus.SetSink(new TeeSink(current, sink));
        return new ActionDisposable(() =>
        {
            var tee = RuntimeEventBus.Sink as TeeSink;
            if (tee != null) RuntimeEventBus.SetSink(tee.Left);
        });
    }

    private sealed class TeeSink : IRuntimeEventSink
    {
        public IRuntimeEventSink? Left { get; }
        private readonly IRuntimeEventSink _right;
        public TeeSink(IRuntimeEventSink? left, IRuntimeEventSink right) { Left = left; _right = right; }
        public async Task PublishAsync(RuntimeEvent evt, CancellationToken ct = default)
        {
            if (Left != null) await Left.PublishAsync(evt, ct).ConfigureAwait(false);
            await _right.PublishAsync(evt, ct).ConfigureAwait(false);
        }
    }

    private sealed class LambdaSink : IRuntimeEventSink
    {
        private readonly Func<RuntimeEvent, Task> _f;
        public LambdaSink(Func<RuntimeEvent, Task> f) => _f = f;
        public Task PublishAsync(RuntimeEvent evt, CancellationToken ct = default) => _f(evt);
    }

    private sealed class ActionDisposable : IDisposable
    {
        private readonly Action _dispose; public ActionDisposable(Action d) => _dispose = d; public void Dispose() => _dispose();
    }
}

