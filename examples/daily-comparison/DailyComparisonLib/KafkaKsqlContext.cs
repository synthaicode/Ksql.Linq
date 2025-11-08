using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Modeling;
using DailyComparisonLib.Models;

namespace DailyComparisonLib;

public class KafkaKsqlContext : KsqlContext
{
    public KafkaKsqlContext(KsqlDslOptions options) : base(options)
    {
    }

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        // Updated IF: remove deprecated WithWindow pipeline. Keep entity registrations minimal.
        modelBuilder.Entity<MarketSchedule>();
        modelBuilder.Entity<RateCandle>();
        modelBuilder.Entity<DailyComparison>();
    }
}