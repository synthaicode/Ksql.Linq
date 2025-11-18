using Ksql.Linq.Core.Attributes;

namespace DesignTimeGeneration.Entities;

/// <summary>
/// Table entity for trade data with user metadata.
/// </summary>
[KsqlTopic("trades", PartitionCount = 12, ReplicationFactor = 3)]
[KsqlTable]
public class Trade
{
    [KsqlKey]
    public string TradeId { get; set; } = string.Empty;

    public string Symbol { get; set; } = string.Empty;

    [KsqlDecimal(Precision = 18, Scale = 8)]
    public decimal Price { get; set; }

    [KsqlDecimal(Precision = 18, Scale = 4)]
    public decimal Quantity { get; set; }

    public string Side { get; set; } = string.Empty; // "BUY" or "SELL"

    public string UserId { get; set; } = string.Empty;

    [KsqlTimestamp]
    public DateTime ExecutedAt { get; set; }

    public Dictionary<string, string>? Metadata { get; set; }
}
