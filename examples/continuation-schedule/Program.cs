using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Runtime;

// Continuation schedule sample (DSL-first, aligned with physical tests)
// Steps: From 竊・TimeFrame 竊・Tumbling(continuation: true) 竊・GroupBy/Select 竊・Rollup
// Note: Select must include exactly one WindowStart() (bucket column)

public class Tick
{
    [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
    [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
    [KsqlTimestamp] public DateTime TimestampUtc { get; set; }
    public decimal Bid { get; set; }
}

public class MarketSchedule
{
    [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
    [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
    public DateTime OpenTimeUtc { get; set; }
    public DateTime CloseTimeUtc { get; set; }
    public DateTime MarketDate { get; set; }
}

public class Bar
{
    [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
    [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
    [KsqlKey(3)] public DateTime BucketStart { get; set; }
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
}

public sealed class SampleContext : KsqlContext
{
    public SampleContext() : base(new Ksql.Linq.Configuration.KsqlDslOptions()) { }
    public EventSet<Tick> Ticks { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Bar>()
            .ToQuery(q => q.From<Tick>()
                .TimeFrame<MarketSchedule>((r, s) =>
                       r.Broker == s.Broker
                    && r.Symbol == s.Symbol
                    && s.OpenTimeUtc <= r.TimestampUtc && r.TimestampUtc < s.CloseTimeUtc,
                    dayKey: s => s.MarketDate)
                .Tumbling(r => r.TimestampUtc, new Windows { Minutes = new[] { 1, 5 } }, continuation: true)
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new Bar
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart(),
                    Open = g.EarliestByOffset(x => x.Bid),
                    High = g.Max(x => x.Bid),
                    Low = g.Min(x => x.Bid),
                    Close = g.LatestByOffset(x => x.Bid)
                })
                // Continuation=true: session蜀・・繧ｮ繝｣繝・・縺ｯ蜑垢lose縺ｧ1陦後ｒ邯咏ｶ夂函謌・            );
    }
}

class Program
{
    static async Task Main()
    {
        using var ctx = new SampleContext();

        // Seed ticks with a gap (continuation=true will materialize missing minute as carry-forward)
        var broker = "B1"; var symbol = "S1";
        var t0 = DateTime.UtcNow.AddMinutes(-10);
        await ctx.Ticks.AddAsync(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = t0.AddSeconds(1), Bid = 100m });
        await ctx.Ticks.AddAsync(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = t0.AddSeconds(20), Bid = 105m });
        await ctx.Ticks.AddAsync(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = t0.AddSeconds(40), Bid = 99m });
        await ctx.Ticks.AddAsync(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = t0.AddMinutes(2).AddSeconds(5), Bid = 101m });

        // Wait until rows are available in TimeBucket (1m)
        await WaitForAvailableAsync(ctx, Period.Minutes(1), broker, symbol, TimeSpan.FromSeconds(30));

        // Verify via TimeBucket (facade)
        var rows1m = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { broker, symbol }, CancellationToken.None);
        var rows5m = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(5), new[] { broker, symbol }, CancellationToken.None);

        Console.WriteLine($"1m rows: {rows1m.Count}");
        foreach (var b in rows1m.OrderBy(x => x.BucketStart))
            Console.WriteLine($"{b.BucketStart:HH:mm} O:{b.Open} H:{b.High} L:{b.Low} C:{b.Close}");

        Console.WriteLine($"5m rows: {rows5m.Count}");
        foreach (var b in rows5m.OrderBy(x => x.BucketStart))
            Console.WriteLine($"[5m] {b.BucketStart:HH:mm} O:{b.Open} H:{b.High} L:{b.Low} C:{b.Close}");
    }

    private static async Task WaitForAvailableAsync(KsqlContext ctx, Period p, string broker, string symbol, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            using var cts = new CancellationTokenSource(timeout);
            var rows = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, p, new[] { broker, symbol }, cts.Token);
            if (rows.Count > 0) return;
            Console.WriteLine("[warn] No rows materialized within the wait window.");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[warn] Timeout waiting for TimeBucket rows.");
        }
    }
}