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

        var res = await ctx.ExecuteStatementAsync("SHOW STREAMS;");
        Console.WriteLine($"IsSuccess={res.IsSuccess}\nBody snippet={Preview(res.Message)}");
    }

    private static string Preview(string? s)
    {
        s ??= string.Empty;
        return s.Length <= 200 ? s : s.Substring(0, 200) + "...";
    }

    private sealed class MinimalContext : KsqlContext
    {
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }
}

