using Ksql.Linq;
using Ksql.Linq.Configuration;
using Microsoft.Extensions.Configuration;

namespace DailyComparisonLib;

public class MyKsqlContext : KafkaKsqlContext
{
    public MyKsqlContext(KsqlDslOptions options) : base(options)
    {
    }

    public static MyKsqlContext FromConfiguration(IConfiguration configuration)
    {
        var options = new KsqlDslOptions();
        configuration.GetSection(KsqlContext.DefaultSectionName).Bind(options);
        return new MyKsqlContext(options);
    }

    public static MyKsqlContext FromAppSettings(string path)
    {
        var builder = new ConfigurationBuilder()
            .AddJsonFile(path, optional: false)
            ;
        var config = builder.Build();
        return FromConfiguration(config);
    }
}


