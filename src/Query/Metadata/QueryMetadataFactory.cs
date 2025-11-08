using System;
using System.Collections.Generic;
using System.Linq;
using Ksql.Linq.Query.Builders.Utilities;

namespace Ksql.Linq.Query.Metadata;

internal static class QueryMetadataFactory
{
    private static readonly string[] KnownKeys =
    [
        "id",
        "namespace",
        "role",
        "timeframe",
        "graceSeconds",
        "grace_seconds",
        "keys",
        "keys/types",
        "keys/nulls",
        "projection",
        "projection/types",
        "projection/nulls",
        "basedOn/joinKeys",
        "basedOn/openProp",
        "basedOn/closeProp",
        "basedOn/dayKey",
        "basedOn/openInclusive",
        "basedOn/closeInclusive",
        "timeKey",
        "timestamp",
        "forceGenericKey",
        "forceGenericValue",
        "BaseDirectory",
        "StoreName",
        "retention.ms",
        "retentionMs",
        "input"
    ];

    public static QueryMetadata FromAdditionalSettings(IReadOnlyDictionary<string, object> settings)
    {
        settings ??= new Dictionary<string, object>();

        settings.TryGetValue("id", out var idObj);
        settings.TryGetValue("namespace", out var nsObj);
        settings.TryGetValue("role", out var roleObj);
        settings.TryGetValue("timeframe", out var timeframeObj);

        var graceSeconds = TryReadInt(settings, "graceSeconds") ?? TryReadInt(settings, "grace_seconds");
        var timeKey = settings.TryGetValue("timeKey", out var tkObj) ? tkObj?.ToString() : null;
        var timestamp = settings.TryGetValue("timestamp", out var tsObj) ? tsObj?.ToString() : null;
        var baseDir = settings.TryGetValue("BaseDirectory", out var bdObj) ? bdObj?.ToString() : null;
        var storeName = settings.TryGetValue("StoreName", out var snObj) ? snObj?.ToString() : null;
        var retention = TryReadRetention(settings);

        var forceGenericKey = TryReadBool(settings, "forceGenericKey");
        var forceGenericValue = TryReadBool(settings, "forceGenericValue");

        var keyShape = BuildKeyShape(settings);
        var valueShape = BuildProjectionShape(settings);
        var basedOn = BuildBasedOnShape(settings);
        var input = settings.TryGetValue("input", out var inputObj) ? inputObj?.ToString() : null;

        var extras = settings.Where(kv => !KnownKeys.Contains(kv.Key, StringComparer.Ordinal))
            .ToDictionary(kv => kv.Key, kv => (object?)kv.Value);

        return new QueryMetadata
        {
            Identifier = idObj?.ToString(),
            Namespace = nsObj?.ToString(),
            Role = roleObj?.ToString(),
            TimeframeRaw = timeframeObj?.ToString(),
            GraceSeconds = graceSeconds,
            TimeKey = timeKey,
            TimestampColumn = timestamp,
            RetentionMs = retention,
            ForceGenericKey = forceGenericKey,
            ForceGenericValue = forceGenericValue,
            BaseDirectory = baseDir,
            StoreName = storeName,
            Keys = keyShape,
            Projection = valueShape,
            BasedOn = basedOn,
            InputHint = input,
            Extras = extras
        };
    }

    private static QueryKeyShape BuildKeyShape(IReadOnlyDictionary<string, object> settings)
    {
        var names = settings.TryGetValue("keys", out var keysObj) && keysObj is string[] kn ? kn : Array.Empty<string>();
        var types = settings.TryGetValue("keys/types", out var ktObj) && ktObj is Type[] kt ? kt : Array.Empty<Type>();
        var nulls = settings.TryGetValue("keys/nulls", out var knObj) && knObj is bool[] nb ? nb : Array.Empty<bool>();
        return new QueryKeyShape(names, types, nulls);
    }

    private static QueryProjectionShape BuildProjectionShape(IReadOnlyDictionary<string, object> settings)
    {
        var names = settings.TryGetValue("projection", out var pObj) && pObj is string[] pn ? pn : Array.Empty<string>();
        var types = settings.TryGetValue("projection/types", out var ptObj) && ptObj is Type[] pt ? pt : Array.Empty<Type>();
        var nulls = settings.TryGetValue("projection/nulls", out var pnObj) && pnObj is bool[] pb ? pb : Array.Empty<bool>();
        return new QueryProjectionShape(names, types, nulls);
    }

    private static QueryBasedOnShape? BuildBasedOnShape(IReadOnlyDictionary<string, object> settings)
    {
        var joinKeys = settings.TryGetValue("basedOn/joinKeys", out var jkObj) && jkObj is string[] jk ? jk : null;
        var openProp = settings.TryGetValue("basedOn/openProp", out var opObj) ? opObj?.ToString() : null;
        var closeProp = settings.TryGetValue("basedOn/closeProp", out var cpObj) ? cpObj?.ToString() : null;
        var dayKey = settings.TryGetValue("basedOn/dayKey", out var dkObj) ? dkObj?.ToString() : null;
        var openIncl = TryReadBool(settings, "basedOn/openInclusive");
        var closeIncl = TryReadBool(settings, "basedOn/closeInclusive");

        if (joinKeys == null && openProp == null && closeProp == null && dayKey == null && openIncl == null && closeIncl == null)
            return null;

        return new QueryBasedOnShape(joinKeys ?? Array.Empty<string>(), openProp, closeProp, dayKey, openIncl, closeIncl);
    }

    private static long? TryReadRetention(IReadOnlyDictionary<string, object> settings)
    {
        if (settings.TryGetValue("retentionMs", out var camel) && WithClauseBuilder.TryConvertRetention(camel, out var camelValue))
            return camelValue;
        if (settings.TryGetValue("retention.ms", out var dotted) && WithClauseBuilder.TryConvertRetention(dotted, out var dottedValue))
            return dottedValue;
        return null;
    }

    private static int? TryReadInt(IReadOnlyDictionary<string, object> settings, string key)
    {
        if (!settings.TryGetValue(key, out var value) || value is null)
            return null;

        return value switch
        {
            int i => i,
            short s => s,
            long l => (int)l,
            string str when int.TryParse(str, out var parsed) => parsed,
            _ => null
        };
    }

    private static bool? TryReadBool(IReadOnlyDictionary<string, object> settings, string key)
    {
        if (!settings.TryGetValue(key, out var value) || value is null)
            return null;

        return value switch
        {
            bool b => b,
            string str when bool.TryParse(str, out var parsed) => parsed,
            _ => null
        };
    }
}
