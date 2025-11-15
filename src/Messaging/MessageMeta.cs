namespace Ksql.Linq.Messaging;

/// <summary>
/// Metadata derived from a Kafka message.
/// </summary>
public readonly record struct MessageMeta(
    string Topic,
    int Partition,
    long Offset,
    System.DateTimeOffset TimestampUtc,
    int? SchemaIdKey,
    int? SchemaIdValue,
    bool KeyIsNull,
    System.Collections.Generic.IReadOnlyDictionary<string, string> HeaderAllowList
)
{
    public string Topic { get; init; } = Topic;
    public int Partition { get; init; } = Partition;
    public long Offset { get; init; } = Offset;
    public System.DateTimeOffset TimestampUtc { get; init; } = TimestampUtc;
    public int? SchemaIdKey { get; init; } = SchemaIdKey;
    public int? SchemaIdValue { get; init; } = SchemaIdValue;
    public bool KeyIsNull { get; init; } = KeyIsNull;
    public System.Collections.Generic.IReadOnlyDictionary<string, string> HeaderAllowList { get;init; } = HeaderAllowList;
}
