using System;
using System.Collections.Generic;

namespace Ksql.Linq.Query.Metadata;

/// <summary>
/// Typed representation of the derived query metadata previously stored inside
/// <see cref="Core.Abstractions.EntityModel.AdditionalSettings"/>.
/// </summary>
public sealed record QueryMetadata
{
    public string? Identifier { get; init; }
    public string? Namespace { get; init; }
    public string? Role { get; init; }
    public string? TimeframeRaw { get; init; }
    public int? GraceSeconds { get; init; }
    public string? TimeKey { get; init; }
    public string? TimestampColumn { get; init; }
    public long? RetentionMs { get; init; }
    public bool? ForceGenericKey { get; init; }
    public bool? ForceGenericValue { get; init; }
    public string? BaseDirectory { get; init; }
    public string? StoreName { get; init; }

    public QueryKeyShape Keys { get; init; } = QueryKeyShape.Empty;
    public QueryProjectionShape Projection { get; init; } = QueryProjectionShape.Empty;
    public QueryBasedOnShape? BasedOn { get; init; }
    public string? InputHint { get; init; }

    /// <summary>
    /// Additional metadata not yet promoted to a strongly-typed property.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Extras { get; init; } = new Dictionary<string, object?>();
}

public sealed record QueryKeyShape(string[] Names, Type[] Types, bool[] NullableFlags)
{
    public static readonly QueryKeyShape Empty = new(Array.Empty<string>(), Array.Empty<Type>(), Array.Empty<bool>());
}

public sealed record QueryProjectionShape(string[] Names, Type[] Types, bool[] NullableFlags)
{
    public static readonly QueryProjectionShape Empty = new(Array.Empty<string>(), Array.Empty<Type>(), Array.Empty<bool>());
}

public sealed record QueryBasedOnShape(
    string[] JoinKeys,
    string? OpenProperty,
    string? CloseProperty,
    string? DayKey,
    bool? IsOpenInclusive,
    bool? IsCloseInclusive);
