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
        // 譛蟆・ 繧ｽ繝ｼ繧ｹ螳夂ｾｩ・・ows/hub繧・ｸ贋ｽ崎ｶｳ縺ｯ險ｭ險医↓萓晏ｭ假ｼ・        modelBuilder.Entity<Rate>();
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
        // 1) 繧､繝吶Φ繝医す繝ｳ繧ｯ繧堤匳骭ｲ・域悴險ｭ螳壹↑繧臥匱轣ｫ縺励↑縺・ｼ・        RuntimeEventBus.SetSink(new ConsoleSink());

        // 2) 繧ｳ繝ｳ繝・く繧ｹ繝医ｒ讒狗ｯ・        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "KsqlDsl_")
            .Build();
        var loggerFactory = LoggerFactory.Create(b => b.AddConsole());

        await using var ctx = KsqlContextBuilder.Create()
            .UseConfiguration(configuration)
            .EnableLogging(loggerFactory)
            .BuildContext<RuntimeEventsContext>();

        // 3) rows_last 縺ｮ ready 繧貞ｾ・▽・井ｾ・ bar_1s_rows_last・・        var rowsLastTopic = configuration["RowsLastTopic"] ?? "bar_1s_rows_last";
        var rowsReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = SubscribeOnce(e =>
        {
            if (e.Name == "rows_last.ready" && e.Phase == "done" && string.Equals(e.Topic, rowsLastTopic, StringComparison.OrdinalIgnoreCase))
                rowsReady.TrySetResult();
        });

        // 4) Streamiz RUNNING 繧貞ｾ・▽・井ｾ・ bar_1m_live・・        var liveTopic = configuration["LiveTopic"] ?? "bar_1m_live";
        var running = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _s = SubscribeOnce(e =>
        {
            if (e.Name == "streamiz.state" && e.Phase == "running" && string.Equals(e.Topic, liveTopic, StringComparison.OrdinalIgnoreCase))
                running.TrySetResult();
        });

        // 莉ｻ諢・ 荳ｦ蛻励↓蠕・▽・・0s 莉･蜀・ｼ・        await Task.WhenAll(rowsReady.Task.WaitAsync(TimeSpan.FromSeconds(60)),
                           running.Task.WaitAsync(TimeSpan.FromSeconds(60)));

        Console.WriteLine("[sample] rows_last.ready + streamiz.running observed.");
    }

    private static IDisposable SubscribeOnce(Action<RuntimeEvent> onEvent)
    {
        var sink = new LambdaSink(e => { onEvent(e); return Task.CompletedTask; });
        // 譌｢蟄倥す繝ｳ繧ｯ縺ｯ菫晄戟縺励◆縺ｾ縺ｾ縲∝句挨縺ｮ荳譎ゅし繝悶せ繧ｯ縺ｨ縺励※遨阪・
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
