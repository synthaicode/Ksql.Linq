using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Events;

namespace PhysicalTestEnv;

internal sealed class TestRuntimeEventSink : IRuntimeEventSink
{
    private readonly ConcurrentQueue<RuntimeEvent> _events = new();

    public Task PublishAsync(RuntimeEvent evt, CancellationToken ct = default)
    {
        _events.Enqueue(evt);
        try
        {
            Console.WriteLine($"[runtime] name={evt.Name} phase={evt.Phase} topic={evt.Topic} entity={evt.Entity} ok={evt.Success} msg={evt.Message}");
            // Persist cache.* events to reports folder for offline inspection
            if (!string.IsNullOrWhiteSpace(evt.Name) && evt.Name.StartsWith("cache.", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    var baseDir = AppContext.BaseDirectory;
                    var reports = System.IO.Path.Combine(baseDir, "reports", "physical");
                    System.IO.Directory.CreateDirectory(reports);
                    var topicSafe = (evt.Topic ?? "cache").ToLowerInvariant();
                    var file = System.IO.Path.Combine(reports, $"cache_{topicSafe}.log");
                    var line = $"{DateTime.UtcNow:o}\tname={evt.Name}\tphase={evt.Phase}\ttopic={evt.Topic}\tentity={evt.Entity}\tok={evt.Success}\tmsg={evt.Message}";
                    System.IO.File.AppendAllText(file, line + Environment.NewLine);
                }
                catch { }
            }
        }
        catch { }
        return Task.CompletedTask;
    }

    public async Task<RuntimeEvent?> WaitAsync(Func<RuntimeEvent, bool> predicate, TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            if (_events.TryPeek(out _))
            {
                foreach (var e in _events.ToArray())
                {
                    if (predicate(e))
                        return e;
                }
            }
            try { await Task.Delay(200, ct).ConfigureAwait(false); } catch { break; }
        }
        return null;
    }

    public bool Any(Func<RuntimeEvent, bool> predicate)
        => _events.Any(predicate);
}