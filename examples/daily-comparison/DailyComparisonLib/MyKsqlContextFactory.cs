using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;

namespace DailyComparisonLib;

/// <summary>
/// Design-time factory for creating MyKsqlContext instances.
/// Used by CLI tools and utilities that need to instantiate the context without DI.
/// </summary>
public class MyKsqlContextFactory : IDesignTimeKsqlContextFactory<MyKsqlContext>
{
    /// <summary>
    /// Creates a MyKsqlContext instance for design-time operations.
    /// </summary>
    /// <param name="args">Command-line arguments. Optionally specify config file path as first argument.</param>
    /// <returns>A configured MyKsqlContext instance.</returns>
    public MyKsqlContext CreateContext(string[] args)
    {
        var configPath = args.Length > 0 ? args[0] : "appsettings.json";

        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(configPath, optional: false)
            .AddEnvironmentVariables()
            .Build();

        var options = new KsqlDslOptions();
        configuration.GetSection(KsqlContext.DefaultSectionName).Bind(options);

        return new MyKsqlContext(options);
    }
}
