using System;

namespace HoppingWindowExample;

/// <summary>
/// Represents a financial transaction
/// </summary>
public class Transaction
{
    public string TransactionId { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public double Amount { get; set; }
    public string Currency { get; set; } = string.Empty;
    public DateTime TransactionTime { get; set; }
}

/// <summary>
/// Represents aggregated transaction statistics for a hopping window
/// </summary>
public class UserTransactionStats
{
    public string UserId { get; set; } = string.Empty;
    public DateTime WindowStart { get; set; }
    public DateTime WindowEnd { get; set; }
    public long TransactionCount { get; set; }
    public double TotalAmount { get; set; }
    public double AvgAmount { get; set; }
    public double MinAmount { get; set; }
    public double MaxAmount { get; set; }
    public string[] Currencies { get; set; } = Array.Empty<string>();
}
