using Ksql.Linq.Core.Attributes;

namespace DesignTimeGeneration.Entities;

/// <summary>
/// Table entity for aggregated 1-minute candles.
/// </summary>
[KsqlTopic("rate_candles_1m", PartitionCount = 6, ReplicationFactor = 3)]
[KsqlTable]
public class RateCandle1m
{
    [KsqlKey]
    public string Symbol { get; set; } = string.Empty;

    [KsqlKey]
    public DateTime WindowStart { get; set; }

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal Open { get; set; }

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal High { get; set; }

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal Low { get; set; }

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal Close { get; set; }

    public long Count { get; set; }
}
