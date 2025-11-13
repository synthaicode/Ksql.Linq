using Ksql.Linq;
using Microsoft.Extensions.Configuration;
using Ksql.Linq.Application;
using Microsoft.Extensions.Logging;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "KsqlDsl_")
            .Build();
        var loggerFactory = LoggerFactory.Create(b => b.AddConsole());

        await using var ctx = KsqlContextBuilder.Create()
            .UseConfiguration(configuration)
            .EnableLogging(loggerFactory)
            .BuildContext<KsqlContextMinimal>();

        // Example 1: Count from TABLE by WHERE clause
        var c1 = await ctx.PullCountAsync("bar_1m_live", "Broker='B1' AND Symbol='S1'");
        Console.WriteLine($"count={c1}");

        // Example 2: Rows with LIMIT
        var rows = await ctx.PullRowsAsync("bar_1s_rows_last", "Broker='B1' AND Symbol='S1'", limit: 5);
        foreach (var r in rows)
        {
            Console.WriteLine(string.Join(",", r.Select(x => x?.ToString() ?? "<null>")));
        }
    }

    // Minimal context for direct PullQuery execution
    private sealed class KsqlContextMinimal : KsqlContext
    {
        public KsqlContextMinimal(KsqlContextOptions options) : base(options.Configuration, options.LoggerFactory) {}
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }
}
