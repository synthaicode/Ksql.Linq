using Ksql.Linq.Core.Attributes;

namespace DesignTimeGeneration.Entities;

/// <summary>
/// Base stream entity for market rate data.
/// </summary>
[KsqlTopic("rates", PartitionCount = 6, ReplicationFactor = 3)]
[KsqlStream]
public class Rate
{
    [KsqlKey]
    public string Symbol { get; set; } = string.Empty;

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal Bid { get; set; }

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal Ask { get; set; }

    [KsqlTimestamp]
    public DateTime Timestamp { get; set; }

    public string Source { get; set; } = string.Empty;
}
