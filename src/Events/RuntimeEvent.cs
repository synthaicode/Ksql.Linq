using System;

namespace Ksql.Linq.Events;

public sealed class RuntimeEvent
{
    public string Name { get; init; } = string.Empty;
    public string? Phase { get; init; }
    public string? Entity { get; init; }
    public string? Topic { get; init; }
    public string? Timeframe { get; init; }
    public string? Role { get; init; }
    public string? SqlPreview { get; init; }
    public string? QueryId { get; init; }
    public bool? Success { get; init; }
    public string? Message { get; init; }
    public Exception? Exception { get; init; }
    public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
    public string? AppId { get; init; }
    public string? State { get; init; }
}

