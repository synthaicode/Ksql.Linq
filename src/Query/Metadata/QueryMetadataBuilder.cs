using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Ksql.Linq.Query.Analysis;

namespace Ksql.Linq.Query.Metadata;

internal static class QueryMetadataBuilder
{
    public static QueryMetadata FromDerivedEntity(
        DerivedEntity entity,
        IReadOnlyList<ColumnShape> keyShape,
        IReadOnlyList<ColumnShape> valueShape)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        keyShape ??= entity.KeyShape;
        valueShape ??= entity.ValueShape;

        var keyNames = keyShape.Select(k => k.Name).ToArray();
        var keyTypes = keyShape.Select(k => k.Type).ToArray();
        var keyNulls = keyShape.Select(k => k.IsNullable).ToArray();

        var valueNames = valueShape.Select(v => v.Name).ToArray();
        var valueTypes = valueShape.Select(v => v.Type).ToArray();
        var valueNulls = valueShape.Select(v => v.IsNullable).ToArray();

        var basedOn = entity.BasedOnSpec;
        var basedOnShape = new QueryBasedOnShape(
            basedOn.JoinKeys.ToArray(),
            basedOn.OpenProp,
            basedOn.CloseProp,
            basedOn.DayKey,
            basedOn.IsOpenInclusive,
            basedOn.IsCloseInclusive);

        var timeframeToken = $"{entity.Timeframe.Value}{entity.Timeframe.Unit}";
        var namespaceValue = BuildNamespace(entity);

        return new QueryMetadata
        {
            Identifier = entity.Id,
            Namespace = namespaceValue,
            Role = entity.Role.ToString(),
            TimeframeRaw = timeframeToken,
            GraceSeconds = entity.GraceSeconds,
            TimeKey = string.IsNullOrWhiteSpace(entity.TimeKey) ? null : entity.TimeKey,
            Keys = new QueryKeyShape(keyNames, keyTypes, keyNulls),
            Projection = new QueryProjectionShape(valueNames, valueTypes, valueNulls),
            BasedOn = basedOnShape,
            InputHint = entity.InputHint,
            Extras = new Dictionary<string, object?>()
        };
    }

    private static string? BuildNamespace(DerivedEntity entity)
    {
        var nsSource = entity.TopicHint ?? entity.Id;
        if (string.IsNullOrWhiteSpace(nsSource))
            return null;

        var baseNs = nsSource;
        if (entity.TopicHint == null)
        {
            baseNs = entity.Role switch
            {
                Role.Live => TrimSuffix(baseNs, $"_{entity.Timeframe.Value}{entity.Timeframe.Unit}_live"),
                Role.Final1sStream => TrimSuffix(baseNs, "_1s_rows"),
                _ => baseNs
            };
        }

        var sanitized = Regex.Replace(baseNs.ToLowerInvariant(), "[^a-z0-9_]", "_");
        return $"runtime_{sanitized}_ksql";
    }

    private static string TrimSuffix(string value, string suffix)
        => value.EndsWith(suffix, StringComparison.Ordinal) ? value[..^suffix.Length] : value;
}