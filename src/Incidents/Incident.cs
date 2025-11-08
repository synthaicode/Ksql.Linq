using System;
using System.Collections.Generic;

namespace Ksql.Linq.Incidents;

public sealed class Incident
{
    public string Name { get; init; } = string.Empty;
    public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
    public string Entity { get; init; } = string.Empty;
    public string Period { get; init; } = string.Empty;
    public IReadOnlyList<string>? Keys { get; init; }
    public DateTime? BucketStartUtc { get; init; }
    public int? ObservedCount { get; init; }
    public string? Notes { get; init; }
}

