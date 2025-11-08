using Ksql.Linq.Core.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Ksql.Linq.Tests.Core;

public class LoggerConfigurationTests
{
    [Fact]
    public void CreateLoggerFactory_FromConfiguration_RespectsNamespaceLevels()
    {
        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.logging.json")
            .Build();

        using var factory = config.CreateLoggerFactory();

        var msgLogger = factory.CreateLogger("Ksql.Linq.Messaging.Sample");
        var coreLogger = factory.CreateLogger("Ksql.Linq.Core.Sample");
        // Serialization namespace removed
        Assert.False(msgLogger.IsEnabled(LogLevel.Information));
        Assert.True(msgLogger.IsEnabled(LogLevel.Warning));
        Assert.True(coreLogger.IsEnabled(LogLevel.Information));
    }
}