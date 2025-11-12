using Ksql.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Examples.StartupWarmup;

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
            .BuildContext<MinimalContext>();

        // Enable app-side startup fill actions in options if needed by your environment
        // This example runs the warmup explicitly regardless of the flag, to keep it predictable.

        var tables = new[] { "MY_TABLE_A", "MY_TABLE_B" };
        var streams = new[] { "MY_STREAM_A" };

        var warmup = new WarmupStartupFillService(tablesToProbe: tables, streamsToProbe: streams);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await warmup.RunAsync(ctx, cts.Token);

        Console.WriteLine("[startup-warmup] Completed warmup (read-only).\nYou can integrate this into your host startup if desired.");
    }

    private sealed class MinimalContext : KsqlContext
    {
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }
}

