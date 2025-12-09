using Ksql.Linq.Core.Attributes;
using System;

namespace HoppingWindowExample;

/// <summary>
/// Consumer entity for user_transaction_stats topic
/// Represents aggregated transaction statistics from the hopping window
/// </summary>
[KsqlTopic("user_transaction_stats")]
public class UserTransactionStatsConsumer
{
    [KsqlKey(0)]
    public string UserId { get; set; } = string.Empty;

    public DateTime WindowStart { get; set; }
    public DateTime WindowEnd { get; set; }
    public long TransactionCount { get; set; }
    public double TotalAmount { get; set; }
    public double AvgAmount { get; set; }
    public double MinAmount { get; set; }
    public double MaxAmount { get; set; }
    public string[] Currencies { get; set; } = Array.Empty<string>();

    public override string ToString()
    {
        var currencies = string.Join(", ", Currencies);
        return $"User: {UserId}, Window: {WindowStart:HH:mm:ss} - {WindowEnd:HH:mm:ss}, " +
               $"Count: {TransactionCount}, Total: ${TotalAmount:F2}, Avg: ${AvgAmount:F2}, " +
               $"Min: ${MinAmount:F2}, Max: ${MaxAmount:F2}, Currencies: [{currencies}]";
    }
}
