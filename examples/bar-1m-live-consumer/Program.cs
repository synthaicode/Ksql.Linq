using Ksql.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

public sealed class LiveConsumerContext : KsqlContext
{
    public LiveConsumerContext(IConfiguration cfg, ILoggerFactory? lf = null) : base(cfg, lf) { }
}

class Program
{
    static async Task Main()
    {
        var cfg = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        using var lf = LoggerFactory.Create(b => b.AddConsole());
        await using var ctx = new LiveConsumerContext(cfg, lf);

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

        var consumeTask = Task.Run(() =>
            ctx.Set<Bar1mLive>().ForEachAsync((b, headers, meta) =>
            {
                Console.WriteLine($"{b.Symbol} {b.BucketStart:u} {b.Close}");
                return Task.CompletedTask;
            }, cancellationToken: cts.Token));

        await ctx.WaitForEntityReadyAsync<Bar1mLive>(TimeSpan.FromSeconds(15));

        // Pull examples: rows_last (bar_1s_rows_last)
        try
        {
            var table = "BAR_1S_ROWS_LAST";
            // Latest across all keys
            var lastSql = $"SELECT BROKER, SYMBOL, BUCKETSTART, CLOSE FROM {table} ORDER BY BUCKETSTART DESC LIMIT 1;";
            var last = await ctx.QueryRowsAsync(lastSql, TimeSpan.FromSeconds(5));
            if (last.Count > 0)
            {
                var r = last[0];
                Console.WriteLine($"[rows_last] latest: broker={(r[0]??"-")} symbol={(r[1]??"-")} bucket={(r[2]??"-")} close={(r[3]??"-")}");
            }

            // Presence check for a given key + bucket
            var broker = Environment.GetEnvironmentVariable("DEMO_BROKER") ?? "X";
            var symbol = Environment.GetEnvironmentVariable("DEMO_SYMBOL") ?? "FOO";
            var bucket = DateTime.UtcNow.AddMinutes(-1);
            var where = $"WHERE BROKER='{broker.Replace("'","''")}' AND SYMBOL='{symbol.Replace("'","''")}' AND BUCKETSTART='{bucket:O}'";
            var existsSql = $"SELECT 1 FROM {table} {where} LIMIT 1;";
            var exists = await ctx.QueryCountAsync(existsSql, TimeSpan.FromSeconds(5));
            Console.WriteLine($"[rows_last] exists({broker},{symbol},{bucket:O}) => {(exists>0 ? "yes" : "no")}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[rows_last] pull examples skipped: {ex.Message}");
        }
        await consumeTask;
    }
}