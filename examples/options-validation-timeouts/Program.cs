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

        // Demonstrate ConfigureValidation and WithTimeouts
        var options = KsqlContextBuilder.Create()
            .UseConfiguration(configuration)
            .EnableLogging(loggerFactory)
            .ConfigureValidation(autoRegister: false, failOnErrors: false, enablePreWarming: false)
            .WithTimeouts(schemaRegistrationTimeout: TimeSpan.FromSeconds(60))
            .Build();

        await using var ctx = new MinimalContext(options);
        Console.WriteLine("Configured validation and timeouts successfully.");
    }

    private sealed class MinimalContext : KsqlContext
    {
        public MinimalContext(KsqlContextOptions opts) : base(opts) { }
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }
}

