using Ksql.Linq.Configuration.Messaging;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Configuration;
using System.Collections.Generic;
using System.ComponentModel;

namespace Ksql.Linq.Configuration;
public class KsqlDslOptions
{
    public KsqlDslOptions()
    {
        // Default constructor
    }
    /// <summary>
    /// Validation mode (root level setting)
    /// </summary>
    public ValidationMode ValidationMode { get; init; }

    /// <summary>
    /// Common settings (BootstrapServers, ClientId, etc.)
    /// </summary>
    public CommonSection Common { get; init; } = new();

    public KsqlServerOptions KsqlServer { get; init; } = new();

    /// <summary>
    /// Per-topic settings (manage producer/consumer settings per topic)
    /// </summary>
    public Dictionary<string, TopicSection> Topics { get; init; } = new();

    // Heartbeat options removed (legacy HeartbeatRunner deprecated)
    public FillOptions Fill { get; init; } = new();

    /// <summary>
    /// Schema Registry settings
    /// </summary>
    public SchemaRegistrySection SchemaRegistry { get; init; } = new();

    /// <summary>
    /// ksqlDB server URL. If omitted, SchemaRegistry.Url's host with port 8088 is used.
    /// </summary>
    public string? KsqlDbUrl { get; init; }

    public List<EntityConfiguration> Entities { get; init; } = new();

    public DlqOptions DlqOptions { get; init; } = new();

    public string DlqTopicName
    {
        get => DlqOptions.TopicName;
        set => DlqOptions.TopicName = value;
    }

    /// <summary>
    /// Policy when deserialization fails
    /// </summary>
    public DeserializationErrorPolicy DeserializationErrorPolicy { get; init; }

    /// <summary>
    /// Whether reading from the Final topic is enabled by default
    /// </summary>
    public bool ReadFromFinalTopicByDefault { get; init; }

    /// <summary>
    /// Global decimal precision applied when mapping decimal properties.
    /// </summary>
    public int DecimalPrecision { get; init; }

    /// <summary>
    /// Global decimal scale applied when mapping decimal properties.
    /// </summary>
    public int DecimalScale { get; init; }

    /// <summary>
    /// Optional per-property decimal overrides keyed by entity and property name.
    /// </summary>
    public Dictionary<string, Dictionary<string, DecimalSetting>>? Decimals { get; init; }

    public record DecimalSetting
    {
        public int Precision { get; init; }
        public int Scale { get; init; }
    }

    /// <summary>
    /// Optional overrides for ksqlDB source object names when generating
    /// CREATE STREAM/TABLE AS SELECT (FROM/JOIN) for query-defined entities.
    /// Key: C# type name (e.g., "Order"). Value: ksqlDB object name (e.g., "ORDERS").
    /// </summary>
    public Dictionary<string, string> SourceNameOverrides { get; init; } = new();

    /// <summary>
    /// Warm-up delay in milliseconds before issuing DDL to ksqlDB during initialization.
    /// App settings key: KsqlWarmupDelayMs. Default: 3000ms.
    /// </summary>
    public int KsqlWarmupDelayMs { get; init; } = 3000;

    /// <summary>
    /// Number of retry attempts for ksqlDB DDL statements (CREATE STREAM/TABLE, CSAS/CTAS).
    /// Default: 3 retries.
    /// </summary>
    public int KsqlDdlRetryCount { get; init; } = 5;

    /// <summary>
    /// Initial delay in milliseconds used for exponential backoff when retrying DDL.
    /// Default: 500ms.
    /// </summary>
    public int KsqlDdlRetryInitialDelayMs { get; init; } = 1000;

    /// <summary>
    /// If true, when creating topics the admin will adjust ReplicationFactor down to the available
    /// broker count (useful for single-broker dev clusters). If false, no proactive adjustment is
    /// made and broker-side validation errors are surfaced (with a fallback retry path still in place).
    /// </summary>
    public bool AdjustReplicationFactorToBrokerCount { get; init; } = true;

    /// <summary>
    /// Number of consecutive RUNNING observations required in SHOW QUERIES before a query is
    /// considered stable. Used when waiting for persistent queries after CTAS/CSAS.
    /// </summary>
    public int KsqlQueryRunningConsecutiveCount { get; init; } = 5;

    /// <summary>
    /// Poll interval in milliseconds between SHOW QUERIES checks when waiting for a query to
    /// reach RUNNING state.
    /// </summary>
    public int KsqlQueryRunningPollIntervalMs { get; init; } = 2000;

    /// <summary>
    /// Additional stability window in seconds after the required consecutive RUNNING observations
    /// before confirming the query as stable.
    /// </summary>
    public int KsqlQueryRunningStabilityWindowSeconds { get; init; } = 15;

    /// <summary>
    /// Overall timeout in seconds for waiting until a persistent query reports RUNNING.
    /// Falls back to 180 seconds when unset.
    /// </summary>
    public int KsqlQueryRunningTimeoutSeconds { get; init; } = 180;

    /// <summary>
    /// Warmup window in seconds for simple entity DDL (non-query entities) before issuing CREATE
    /// statements to ksqlDB.
    /// </summary>
    public int KsqlSimpleEntityWarmupSeconds { get; init; } = 15;

    /// <summary>
    /// Warmup window in seconds for query-defined entity DDL (CSAS/CTAS) before issuing CREATE
    /// statements to ksqlDB.
    /// </summary>
    public int KsqlQueryEntityWarmupSeconds { get; init; } = 10;

    /// <summary>
    /// Visibility timeout in seconds when waiting for ksqlDB metadata (SHOW TABLES/STREAMS)
    /// to reflect newly created entities.
    /// </summary>
    public int KsqlEntityDdlVisibilityTimeoutSeconds { get; init; } = 12;

    /// <summary>
    /// Default HTTP timeout in seconds for ksqlDB REST calls when no explicit timeout is provided.
    /// </summary>
    public int KsqlHttpTimeoutSeconds { get; init; } = 60;
}
