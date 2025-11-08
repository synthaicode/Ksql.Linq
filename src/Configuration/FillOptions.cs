using System.ComponentModel;

namespace Ksql.Linq.Configuration;

public sealed class FillOptions
{
    /// <summary>
    /// Enable application-side startup backfill.
    /// </summary>
    [DefaultValue(false)]
    public bool EnableAppSide { get; init; }

    /// <summary>
    /// Maximum number of buckets to backfill per key.
    /// </summary>
    [DefaultValue(30)]
    public int MaxBackfillBuckets { get; init; } = 30;

    /// <summary>
    /// Backfill horizon in minutes. Buckets older than this window are ignored.
    /// </summary>
    [DefaultValue(120)]
    public int BackfillHorizonMinutes { get; init; } = 120;

    /// <summary>
    /// Optional header name to mark synthesized fill records.
    /// </summary>
    [DefaultValue("x-fill")]
    public string HeaderName { get; init; } = "x-fill";

    /// <summary>
    /// Optional header value to mark synthesized fill records.
    /// </summary>
    [DefaultValue("true")]
    public string HeaderValue { get; init; } = "true";

    /// <summary>
    /// Optional compacted state topic to persist idempotency markers (keyed by base/key/bucketEnd).
    /// </summary>
    [DefaultValue("fill_state")]
    public string StateTopicName { get; init; } = "fill_state";

    /// <summary>
    /// Only the leader instance executes startup backfill when true.
    /// </summary>
    [DefaultValue(true)]
    public bool LeaderOnly { get; init; } = true;
}