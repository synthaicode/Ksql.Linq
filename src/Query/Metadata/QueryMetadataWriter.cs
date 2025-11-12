using System;
using System.Collections.Generic;
using Ksql.Linq.Core.Abstractions;

namespace Ksql.Linq.Query.Metadata;

internal static class QueryMetadataWriter
{
    public static void Apply(EntityModel model, QueryMetadata metadata)
    {
        if (model == null) throw new ArgumentNullException(nameof(model));
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));

        model.QueryMetadata = metadata;

        var settings = model.AdditionalSettings;

        SetOrRemove(settings, "id", metadata.Identifier);
        SetOrRemove(settings, "namespace", metadata.Namespace);
        SetOrRemove(settings, "role", metadata.Role);
        SetOrRemove(settings, "timeframe", metadata.TimeframeRaw);

        SetOrRemove(settings, "graceSeconds", metadata.GraceSeconds);
        settings.Remove("grace_seconds");

        SetOrRemove(settings, "timeKey", metadata.TimeKey);
        SetOrRemove(settings, "timestamp", metadata.TimestampColumn);

        SetOrRemove(settings, "forceGenericKey", metadata.ForceGenericKey);
        SetOrRemove(settings, "forceGenericValue", metadata.ForceGenericValue);

        SetOrRemove(settings, "BaseDirectory", metadata.BaseDirectory);
        SetOrRemove(settings, "StoreName", metadata.StoreName);
        if (metadata.RetentionMs.HasValue && metadata.RetentionMs.Value > 0)
        {
            settings["retentionMs"] = metadata.RetentionMs.Value;
            settings["retention.ms"] = metadata.RetentionMs.Value;
        }
        else
        {
            settings.Remove("retentionMs");
            settings.Remove("retention.ms");
        }

        WriteShape(settings, metadata.Keys, "keys");
        WriteShape(settings, metadata.Projection, "projection");

        WriteBasedOn(settings, metadata.BasedOn);
        SetOrRemove(settings, "input", metadata.InputHint);

        if (metadata.Extras != null)
        {
            foreach (var kv in metadata.Extras)
            {
                if (kv.Value is null)
                    settings.Remove(kv.Key);
                else
                    settings[kv.Key] = kv.Value;
            }
        }
    }

    private static void WriteShape(Dictionary<string, object> settings, QueryKeyShape shape, string prefix)
    {
        if (shape == null)
        {
            settings.Remove($"{prefix}");
            settings.Remove($"{prefix}/types");
            settings.Remove($"{prefix}/nulls");
            return;
        }

        if (shape.Names.Length > 0)
            settings[$"{prefix}"] = shape.Names;
        else
            settings.Remove($"{prefix}");

        if (shape.Types.Length > 0)
            settings[$"{prefix}/types"] = shape.Types;
        else
            settings.Remove($"{prefix}/types");

        if (shape.NullableFlags.Length > 0)
            settings[$"{prefix}/nulls"] = shape.NullableFlags;
        else
            settings.Remove($"{prefix}/nulls");
    }

    private static void WriteShape(Dictionary<string, object> settings, QueryProjectionShape shape, string prefix)
    {
        if (shape == null)
        {
            settings.Remove($"{prefix}");
            settings.Remove($"{prefix}/types");
            settings.Remove($"{prefix}/nulls");
            return;
        }

        if (shape.Names.Length > 0)
            settings[$"{prefix}"] = shape.Names;
        else
            settings.Remove($"{prefix}");

        if (shape.Types.Length > 0)
            settings[$"{prefix}/types"] = shape.Types;
        else
            settings.Remove($"{prefix}/types");

        if (shape.NullableFlags.Length > 0)
            settings[$"{prefix}/nulls"] = shape.NullableFlags;
        else
            settings.Remove($"{prefix}/nulls");
    }

    private static void WriteBasedOn(Dictionary<string, object> settings, QueryBasedOnShape? basedOn)
    {
        if (basedOn == null)
        {
            settings.Remove("basedOn/joinKeys");
            settings.Remove("basedOn/openProp");
            settings.Remove("basedOn/closeProp");
            settings.Remove("basedOn/dayKey");
            settings.Remove("basedOn/openInclusive");
            settings.Remove("basedOn/closeInclusive");
            return;
        }

        settings["basedOn/joinKeys"] = basedOn.JoinKeys ?? Array.Empty<string>();
        SetOrRemove(settings, "basedOn/openProp", basedOn.OpenProperty);
        SetOrRemove(settings, "basedOn/closeProp", basedOn.CloseProperty);
        SetOrRemove(settings, "basedOn/dayKey", basedOn.DayKey);
        SetOrRemove(settings, "basedOn/openInclusive", basedOn.IsOpenInclusive);
        SetOrRemove(settings, "basedOn/closeInclusive", basedOn.IsCloseInclusive);
    }

    private static void SetOrRemove(Dictionary<string, object> settings, string key, object? value)
    {
        if (value is null)
        {
            settings.Remove(key);
            return;
        }

        settings[key] = value;
    }
}
