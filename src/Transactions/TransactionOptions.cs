using System;

namespace Ksql.Linq.Transactions;

/// <summary>
/// Configuration options for Kafka transactions.
/// </summary>
public class TransactionOptions
{
    /// <summary>
    /// Required: Unique identifier for the transactional producer.
    /// Only one producer with this ID can be active at a time.
    /// </summary>
    public string TransactionalId { get; set; } = string.Empty;

    /// <summary>
    /// Transaction timeout. Default is 60 seconds.
    /// </summary>
    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Consumer isolation level for reading committed messages.
    /// </summary>
    public KafkaIsolationLevel IsolationLevel { get; set; } = KafkaIsolationLevel.ReadCommitted;

    /// <summary>
    /// Enable idempotent producer (required for transactions).
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Maximum number of in-flight requests per connection.
    /// Must be 5 or less for idempotent/transactional producers.
    /// </summary>
    public int MaxInFlight { get; set; } = 5;

    /// <summary>
    /// Acknowledgement level. Must be "all" for transactions.
    /// </summary>
    public string Acks { get; set; } = "all";
}

/// <summary>
/// Kafka consumer isolation level for transactional reads.
/// </summary>
public enum KafkaIsolationLevel
{
    /// <summary>
    /// Read all messages including aborted transactions.
    /// </summary>
    ReadUncommitted,

    /// <summary>
    /// Only read committed messages (recommended for EOS).
    /// </summary>
    ReadCommitted
}
