using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;

namespace DesignTimeGeneration;

/// <summary>
/// Design-time factory for creating SampleKsqlContext.
/// Used by dotnet-ksql CLI tool to generate KSQL scripts and Avro schemas.
/// </summary>
public class SampleKsqlContextFactory : IDesignTimeKsqlContextFactory<SampleKsqlContext>
{
    public SampleKsqlContext CreateContext(string[] args)
    {
        // Determine config file path
        var configPath = args.Length > 0 ? args[0] : "appsettings.json";

        // Build configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(configPath, optional: false)
            .AddEnvironmentVariables()
            .Build();

        // Bind options
        var options = new KsqlDslOptions();
        configuration.GetSection(KsqlContext.DefaultSectionName).Bind(options);

        return new SampleKsqlContext(options);
    }
}
