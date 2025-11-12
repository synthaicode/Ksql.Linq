using System;
using System.Configuration;
using Microsoft.Extensions.Configuration;

namespace Ksql.Linq.Configuration;

/// <summary>
/// Resolves <see cref="WindowConfiguration"/> entries from configuration.
/// </summary>
public sealed class WindowConfigurationProvider
{
    private readonly IConfiguration _configuration;
    private readonly WindowConfiguration _defaultConfiguration;

    public WindowConfigurationProvider(IConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _defaultConfiguration = BuildDefault(configuration);
    }

    public WindowConfiguration GetConfiguration(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Key is required.", nameof(key));
        }

        var section = _configuration.GetSection($"KsqlDsl:Windows:{key}");
        if (!section.Exists())
        {
            return _defaultConfiguration;
        }

        return WindowConfiguration.FromConfiguration(section);
    }

    private static WindowConfiguration BuildDefault(IConfiguration configuration)
    {
        var section = configuration.GetSection("KsqlDsl:Windows:Default");
        if (!section.Exists())
        {
            return new WindowConfiguration
            {
                WindowSize = TimeSpan.FromMinutes(1)
            };
        }

        return WindowConfiguration.FromConfiguration(section);
    }
}
