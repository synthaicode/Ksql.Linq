using DesignTimeGeneration.Entities;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Modeling;

namespace DesignTimeGeneration;

/// <summary>
/// Sample KsqlContext with multiple entity types.
/// </summary>
public class SampleKsqlContext : KsqlContext
{
    public SampleKsqlContext(KsqlDslOptions options) : base(options)
    {
    }

    // Entity sets
    public IEntitySet<Rate> Rates => Set<Rate>();
    public IEntitySet<RateCandle1m> RateCandles1m => Set<RateCandle1m>();
    public IEntitySet<Trade> Trades => Set<Trade>();

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        // Configure Rate stream
        modelBuilder.Entity<Rate>(entity =>
        {
            entity.ToTopic("rates");
            entity.HasKey(r => r.Symbol);
        });

        // Configure RateCandle1m table with retention
        modelBuilder.Entity<RateCandle1m>(entity =>
        {
            entity.ToTopic("rate_candles_1m");
            entity.HasKey(c => new { c.Symbol, c.WindowStart });
        });

        // Configure Trade table
        modelBuilder.Entity<Trade>(entity =>
        {
            entity.ToTopic("trades");
            entity.HasKey(t => t.TradeId);
        });
    }
}
