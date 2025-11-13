using Ksql.Linq;
using Ksql.Linq.Application;
using Microsoft.Extensions.Configuration;
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
            .BuildContext<MinimalContext>();

        Console.WriteLine("[streamiz-clear] Clearing Streamiz caches (RocksDB delete=true)...");
        ctx.ClearStreamizState(deleteStateDirs: true);
        Console.WriteLine("[streamiz-clear] Done. Next ToListAsync()/Pull will rebuild on demand.");
    }

    private sealed class MinimalContext : KsqlContext
    {
        public MinimalContext(KsqlContextOptions options) : base(options.Configuration, options.LoggerFactory) { }
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }
}
