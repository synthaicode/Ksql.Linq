using System;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Runtime;

/// <summary>
/// Minimal sample: define a hopping aggregation and pull results by key/time range.
/// </summary>
public class HoppingSample
{
    public sealed class Transaction
    {
        [KsqlKey(1)] public string TransactionId { get; set; } = string.Empty;
        public string UserId { get; set; } = string.Empty;
        public double Amount { get; set; }
        public string Currency { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime TransactionTime { get; set; }
    }

    public sealed class UserTransactionStat : IWindowedRecord
    {
        [KsqlKey(1)] public string UserId { get; set; } = string.Empty;
        public DateTime WindowStart { get; set; }
        public DateTime WindowEnd { get; set; }
        public DateTime TransactionTime { get; set; }
        public long TransactionCount { get; set; }
        public double TotalAmount { get; set; }
        public double MaxAmount { get; set; }
        DateTime IWindowedRecord.WindowStart => WindowStart;
        DateTime IWindowedRecord.WindowEnd => WindowEnd;
    }

    private sealed class SampleContext : KsqlContext
    {
        public SampleContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        })
        { }

        public EventSet<Transaction> Transactions { get; set; } = null!;

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UserTransactionStat>()
                .ToQuery(q => q.From<Transaction>()
                    .Hopping(t => t.TransactionTime, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
                    .GroupBy(t => new { t.UserId })
                    .Select(g => new UserTransactionStat
                    {
                        UserId = g.Key.UserId,
                        TransactionTime = g.Max(x => x.TransactionTime),
                        TransactionCount = g.Count(),
                        TotalAmount = g.Sum(x => x.Amount),
                        MaxAmount = g.Max(x => x.Amount)
                    }));
        }
    }

    public static async Task RunAsync()
    {
        await using var ctx = new SampleContext();
        await ctx.StartAsync();

        var now = DateTime.UtcNow;
        await ctx.Transactions.AddAsync(new Transaction
        {
            TransactionId = "demo-1",
            UserId = "user_demo",
            Amount = 100,
            Currency = "USD",
            TransactionTime = now
        });
        await ctx.Transactions.AddAsync(new Transaction
        {
            TransactionId = "demo-2",
            UserId = "user_demo",
            Amount = 150,
            Currency = "EUR",
            TransactionTime = now.AddMinutes(1)
        });

        // Pull hopping results by key and time range via extension
        var rows = await ctx.ReadHoppingAsync<UserTransactionStat>(
            key: new { UserId = "user_demo" },
            from: now.AddMinutes(-1),
            to: now.AddMinutes(10),
            limit: 10,
            timeout: TimeSpan.FromSeconds(5));

        foreach (var r in rows)
        {
            Console.WriteLine($"user={r.UserId}, window=[{r.WindowStart:o} - {r.WindowEnd:o}], count={r.TransactionCount}, total={r.TotalAmount}, max={r.MaxAmount}");
        }
    }
}
