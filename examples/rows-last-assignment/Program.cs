using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[KsqlTopic("deduprates")]
public class Rate
{
    [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
    [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
    [KsqlTimestamp] public DateTime Timestamp { get; set; }
    public double Bid { get; set; }
}

public class Bar
{
    [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
    [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
    [KsqlKey(3)] [KsqlTimestamp] public DateTime BucketStart { get; set; }
    public double Open { get; set; }
    public double High { get; set; }
    public double Low { get; set; }
    public double KsqlTimeFrameClose { get; set; }
}

public sealed class RowsLastAssignmentContext : KsqlContext
{
    private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
    {
        b.AddConsole();
        b.SetMinimumLevel(LogLevel.Information);
        b.AddFilter("Ksql.Linq", LogLevel.Debug);
    });

    public RowsLastAssignmentContext(IConfiguration cfg) : base(cfg, _lf) { }

    public EventSet<Rate> Rates { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Bar>()
            .ToQuery(q => q.From<Rate>()
                .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
                .GroupBy(r => new { r.Broker, r.Symbol })
                .Select(g => new Bar
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart(),
                    Open = g.EarliestByOffset(x => x.Bid),
                    High = g.Max(x => x.Bid),
                    Low = g.Min(x => x.Bid),
                    KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                }));
    }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: false).Build();
        await using var ctx = new RowsLastAssignmentContext(cfg);

        // produce a few 1s ticks to drive bar_1s_rows and rows_last creation
        var now = DateTime.UtcNow;
        for (int i = 0; i < 5; i++)
        {
            await ctx.Rates.AddAsync(new Rate
            {
                Broker = "B1",
                Symbol = "S1",
                Timestamp = now.AddSeconds(i),
                Bid = 200 + i
            });
        }

        Console.WriteLine("Produced 5 Rate events. Waiting for assignment and processing (10s)...");
        await Task.Delay(TimeSpan.FromSeconds(10));

        Console.WriteLine("Done. For assignment failover test, run a second instance with same group and observe logs.");
    }
}


