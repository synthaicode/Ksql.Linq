using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Core;
using Ksql.Linq.Query.Builders.Utilities;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Hub.Analysis;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Ksql.Linq.Query.Metadata;

namespace Ksql.Linq.Query.Analysis;

internal static class DerivedEntityDdlPlanner
{
    public static (string? ddl, Type entityType, string? ns, string? inputOverride, bool shouldExecute) Build(
        string baseName,
        KsqlQueryModel queryModel,
        EntityModel model,
        Role role,
        Func<string, Type> resolveType,
        long defaultRowsStreamRetentionMs)
    {
        var context = InitializeContext(baseName, queryModel, model, role, resolveType, defaultRowsStreamRetentionMs);

        var ddl = role == Role.Final1sStream
            ? BuildRowsStreamDdl(context)
            : BuildWindowedDdl(context);

        var finalMetadata = context.Metadata;
        QueryMetadataWriter.Apply(model, finalMetadata);

        var finalKeys = finalMetadata.Keys;
        var finalProjection = finalMetadata.Projection;

        Type entityType;
        if (role == Role.Final1sStream)
        {
            var keyNames = finalKeys.Names ?? Array.Empty<string>();
            var valueNames = finalProjection.Names ?? Array.Empty<string>();
            var valueTypes = finalProjection.Types ?? Array.Empty<Type>();
            entityType = DerivedTypeFactory.GetDerivedType(context.Name, keyNames, valueNames, valueTypes);
        }
        else if (role == Role.Live)
        {
            // Live 繧よ兜蠖ｱ繝｡繧ｿ縺ｫ蝓ｺ縺･縺丞虚逧・梛繧堤函謌撰ｼ磯寔險亥・繧貞梛縺ｫ霈峨○縺ｦ繝槭ャ繝斐Φ繧ｰ貍上ｌ繧帝亟縺撰ｼ・            var keyNames = finalKeys.Names ?? Array.Empty<string>();
            var valueNames = finalProjection.Names ?? Array.Empty<string>();
            var valueTypes = finalProjection.Types?.Select(t => t ?? typeof(object)).ToArray() ?? Array.Empty<Type>();
            entityType = DerivedTypeFactory.GetDerivedType(context.Name, keyNames, valueNames, valueTypes);
        }
        else
        {
            entityType = context.ResolveType(context.Name);
        }

        model.EntityType = entityType;
        model.TopicName = context.Name;
        model.SetStreamTableType(context.QueryModel.DetermineType());
        if (role == Role.Final1sStream)
        {
            model.SetStreamTableType(StreamTableType.Stream);
        }
        else if (role == Role.Live)
        {
            model.SetStreamTableType(StreamTableType.Table);
        }

        var ns = finalMetadata.Namespace;
        ddl = DecimalCastUtils.InjectDecimalCasts(ddl, model);
        model.QueryModel = context.QueryModel;

        return (ddl, entityType, ns, context.InputOverride, true);
    }

    private static BuildContext InitializeContext(
        string baseName,
        KsqlQueryModel queryModel,
        EntityModel model,
        Role role,
        Func<string, Type> resolveType,
        long defaultRowsStreamRetentionMs)
    {
        var qm = queryModel.Clone();
        var metadata = model.GetOrCreateMetadata();
        var inputOverride = metadata.InputHint;
        var timeframe = metadata.TimeframeRaw ?? qm.Windows.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(timeframe))
        {
            throw new InvalidOperationException("Derived entity is missing timeframe metadata.");
        }

        var spec = RoleTraits.For(role);
        var emit = spec.Emit != null ? $"EMIT {spec.Emit}" : null;
        var name = role switch
        {
            Role.Final1sStream => $"{baseName}_{timeframe}_rows",
            Role.Live => $"{baseName}_{timeframe}_live",
            _ => $"{baseName}_{timeframe}"
        };

        return new BuildContext(
            name,
            timeframe,
            role,
            qm,
            model,
            metadata,
            inputOverride,
            emit,
            resolveType,
            defaultRowsStreamRetentionMs);
    }

    private static string BuildRowsStreamDdl(BuildContext context)
    {
        var qm = context.QueryModel;
        var metadata = context.Metadata;

        var sourceType = (qm.SourceTypes?.Length ?? 0) > 0 ? qm.SourceTypes![0] : null;
        var projectionMeta = EnsureRowsProjectionMetadata(qm);
        if (projectionMeta != null)
        {
            qm.SelectProjectionMetadata = projectionMeta;
        }

        var keyDefinitions = BuildKeyDefinitions(context, sourceType, projectionMeta);
        var valueResult = BuildValueDefinitions(context, sourceType, projectionMeta);
        var orderedValues = OrderValueDefinitions(valueResult, projectionMeta);

        ApplyKeyAndValueShapes(context, keyDefinitions, orderedValues);

        context.Model.ValueSchemaFullName = null;
        var streamSettings = ResolveStreamSettings(context);

        var keyMetas = BuildPropertyMetas(context.Model, sourceType, keyDefinitions);
        var valueMetas = BuildPropertyMetas(context.Model, sourceType, orderedValues);
        var timestampColumn = string.IsNullOrWhiteSpace(valueResult.TimestampName)
            ? null
            : valueResult.TimestampName.ToUpperInvariant();

        var ddl = GenerateCreateStreamDdl(context, keyMetas, valueMetas, streamSettings, timestampColumn);
        TryRecordHubInput(context.Name, orderedValues.Select(v => v.Name).ToArray());

        return ddl;
    }

    private static ProjectionMetadata? EnsureRowsProjectionMetadata(KsqlQueryModel qm)
    {
        if (qm.SelectProjectionMetadata != null)
        {
            return qm.SelectProjectionMetadata;
        }

        return qm.SelectProjection != null
            ? ProjectionMetadataAnalyzer.Build(qm, isHubInput: false)
            : null;
    }

    private static List<(string Name, Type Type, bool IsNullable)> BuildKeyDefinitions(
        BuildContext context,
        Type? sourceType,
        ProjectionMetadata? projectionMeta)
    {
        var metadata = context.Metadata;
        var keyDefs = new List<(string Name, Type Type, bool IsNullable)>();

        if (sourceType != null)
        {
            foreach (var prop in sourceType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (prop.GetCustomAttribute<KsqlKeyAttribute>(inherit: true) == null)
                {
                    continue;
                }
                keyDefs.Add((prop.Name, prop.PropertyType, false));
            }
        }

        var existingKeyNames = metadata.Keys.Names ?? Array.Empty<string>();
        var existingKeyTypes = metadata.Keys.Types ?? Array.Empty<Type>();
        var existingKeyNulls = metadata.Keys.NullableFlags ?? Array.Empty<bool>();

        if (keyDefs.Count == 0 && existingKeyNames.Length > 0)
        {
            for (var i = 0; i < existingKeyNames.Length; i++)
            {
                var keyNameCandidate = existingKeyNames[i];
                if (string.IsNullOrWhiteSpace(keyNameCandidate))
                {
                    continue;
                }

                var type = i < existingKeyTypes.Length ? existingKeyTypes[i] : typeof(string);
                var isNullable = i < existingKeyNulls.Length ? existingKeyNulls[i] : false;
                keyDefs.Add((keyNameCandidate, type, isNullable));
            }
        }

        if (keyDefs.Count == 0 && projectionMeta != null)
        {
            foreach (var member in projectionMeta.Members.Where(m => m.Kind == ProjectionMemberKind.Key))
            {
                var alias = !string.IsNullOrWhiteSpace(member.ResolvedColumnName)
                    ? member.ResolvedColumnName
                    : member.Alias;
                if (string.IsNullOrWhiteSpace(alias))
                {
                    continue;
                }
                if (keyDefs.Any(k => string.Equals(k.Name, alias, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                var memberType = member.ResultType ?? typeof(string);
                keyDefs.Add((alias!, memberType, member.IsNullable));
            }
        }

        if (keyDefs.Count == 0)
        {
            throw new InvalidOperationException(
                $"Tumbling queries must define GroupBy keys. No grouping keys could be inferred for '{context.Name}'. " +
                "Ensure the source query includes a GroupBy(...) specifying aggregation keys.");
        }

        return keyDefs;
    }

    private static ValueDefinitionResult BuildValueDefinitions(
        BuildContext context,
        Type? sourceType,
        ProjectionMetadata? projectionMeta)
    {
        var metadata = context.Metadata;

        var existingValNames = metadata.Projection.Names ?? Array.Empty<string>();
        var existingValTypes = metadata.Projection.Types ?? Array.Empty<Type?>();
        var existingValNulls = metadata.Projection.NullableFlags ?? Array.Empty<bool>();

        var valueDefs = new List<(string Name, Type Type, bool IsNullable)>();
        for (var i = 0; i < existingValNames.Length; i++)
        {
            var valueName = existingValNames[i];
            if (string.IsNullOrWhiteSpace(valueName) ||
                valueName.Equals("BucketStart", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var type = i < existingValTypes.Length && existingValTypes[i] != null
                ? existingValTypes[i]!
                : typeof(object);
            var isNullable = i < existingValNulls.Length ? existingValNulls[i] : true;
            if (valueDefs.Any(v => string.Equals(v.Name, valueName, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            valueDefs.Add((valueName, type, isNullable));
        }

        var (timestampName, timestampType, timestampNullable) = ResolveTimestamp(sourceType, metadata);
        metadata = metadata with { TimestampColumn = timestampName };
        context.UpdateMetadata(metadata);

        void EnsureValue(string name, Type type, bool isNullable)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return;
            }

            var idx = valueDefs.FindIndex(v => string.Equals(v.Name, name, StringComparison.OrdinalIgnoreCase));
            if (idx >= 0)
            {
                valueDefs[idx] = (name, type, isNullable);
            }
            else
            {
                valueDefs.Add((name, type, isNullable));
            }
        }

        EnsureValue(timestampName, timestampType, timestampNullable);

        // Do not auto-inject non-aggregate computed columns here.
        // Rows DDL 縺ｯ ToQuery 縺ｮ險ｭ險医↓蝓ｺ縺･縺丞・縺ｮ縺ｿ繧貞ｮ｣險縺吶ｋ縲・
        static bool IsNullableType(Type type)
            => Nullable.GetUnderlyingType(type) != null || !type.IsValueType;

        string ResolveBasedOn(string key) =>
            key switch
            {
                "basedOn/openProp" => metadata.BasedOn?.OpenProperty ?? string.Empty,
                "basedOn/closeProp" => metadata.BasedOn?.CloseProperty ?? string.Empty,
                "basedOn/dayKey" => metadata.BasedOn?.DayKey ?? string.Empty,
                _ => string.Empty
            };

        var basedOnOpenProp = ResolveBasedOn("basedOn/openProp");
        var basedOnCloseProp = ResolveBasedOn("basedOn/closeProp");
        Type? priceTypeCandidate = null;
        var priceNullable = true;

        if (!string.IsNullOrWhiteSpace(basedOnOpenProp))
        {
            var propInfo = ResolveProperty(sourceType, basedOnOpenProp);
            if (propInfo != null)
            {
                priceTypeCandidate = propInfo.PropertyType;
                priceNullable = IsNullableType(propInfo.PropertyType);
            }
        }

        if (priceTypeCandidate == null && !string.IsNullOrWhiteSpace(basedOnCloseProp))
        {
            var propInfo = ResolveProperty(sourceType, basedOnCloseProp);
            if (propInfo != null)
            {
                priceTypeCandidate = propInfo.PropertyType;
                priceNullable = IsNullableType(propInfo.PropertyType);
            }
        }

        var priceType = priceTypeCandidate ?? typeof(double);
        if (priceTypeCandidate == null)
        {
            priceNullable = true;
        }

        var projectionNames = metadata.Projection.Names ?? Array.Empty<string>();
        var projectionTypes = metadata.Projection.Types ?? Array.Empty<Type?>();
        var projectionNulls = metadata.Projection.NullableFlags ?? Array.Empty<bool>();

        if (projectionMeta != null)
        {
            foreach (var member in projectionMeta.Members.Where(m => m.Kind != ProjectionMemberKind.Key))
            {
                var alias = string.IsNullOrWhiteSpace(member.Alias) ? member.ResolvedColumnName : member.Alias;
                if (string.IsNullOrWhiteSpace(alias))
                {
                    continue;
                }

                var memberType = member.ResultType ?? priceType;
                var memberNullable = member.IsNullable;
                EnsureValue(alias!, memberType, memberNullable);
            }

            var metaNames = projectionMeta.Members
                .Where(m => m.Kind != ProjectionMemberKind.Key)
                .Select(m => string.IsNullOrWhiteSpace(m.Alias) ? m.ResolvedColumnName : m.Alias)
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Select(n => n!)
                .ToArray();

            if (metaNames.Length > 0)
            {
                projectionNames = projectionNames
                    .Concat(metaNames)
                    .Where(n => !string.IsNullOrWhiteSpace(n))
                    .Select(n => n!)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();
            }
        }

        for (var i = 0; i < projectionNames.Length; i++)
        {
            var projName = projectionNames[i];
            if (string.IsNullOrWhiteSpace(projName))
            {
                continue;
            }

            var type = i < projectionTypes.Length && projectionTypes[i] != null
                ? projectionTypes[i]!
                : priceType;
            var nullable = i < projectionNulls.Length ? projectionNulls[i] : priceNullable;
            EnsureValue(projName!, type ?? priceType, nullable);
        }

        return new ValueDefinitionResult(valueDefs, timestampName, projectionNames);
    }

    private static IReadOnlyList<(string Name, Type Type, bool IsNullable)> OrderValueDefinitions(
        ValueDefinitionResult valueResult,
        ProjectionMetadata? projectionMeta)
    {
        var preferredOrder = new List<string>();
        if (!string.IsNullOrWhiteSpace(valueResult.TimestampName))
        {
            preferredOrder.Add(valueResult.TimestampName);
        }

        if (projectionMeta != null)
        {
            preferredOrder.AddRange(
                projectionMeta.Members
                    .Where(m => m.Kind != ProjectionMemberKind.Key)
                    .Select(m => string.IsNullOrWhiteSpace(m.Alias) ? m.ResolvedColumnName : m.Alias)
                    .Where(n => !string.IsNullOrWhiteSpace(n))
                    .Select(n => n!)
                    .Distinct(StringComparer.OrdinalIgnoreCase));
        }

        preferredOrder.AddRange(
            valueResult.ProjectionNames
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .Select(n => n!));

        return OrderValues(valueResult.Values, preferredOrder);
    }

    private static IReadOnlyList<(string Name, Type Type, bool IsNullable)> OrderValues(
        IReadOnlyList<(string Name, Type Type, bool IsNullable)> values,
        IEnumerable<string> preferredOrder)
    {
        var remaining = new List<(string Name, Type Type, bool IsNullable)>(values);
        var ordered = new List<(string Name, Type Type, bool IsNullable)>();

        foreach (var target in preferredOrder)
        {
            var idx = remaining.FindIndex(v => string.Equals(v.Name, target, StringComparison.OrdinalIgnoreCase));
            if (idx < 0)
            {
                continue;
            }

            ordered.Add(remaining[idx]);
            remaining.RemoveAt(idx);
        }

        ordered.AddRange(remaining);
        return ordered;
    }

    private static void ApplyKeyAndValueShapes(
        BuildContext context,
        IReadOnlyList<(string Name, Type Type, bool IsNullable)> keyDefinitions,
        IReadOnlyList<(string Name, Type Type, bool IsNullable)> valueDefinitions)
    {
        var keyNames = keyDefinitions.Select(k => k.Name).ToArray();
        var keyTypes = keyDefinitions.Select(k => k.Type).ToArray();
        var keyNulls = keyDefinitions.Select(k => k.IsNullable).ToArray();

        var valNames = valueDefinitions.Select(v => v.Name).ToArray();
        var valTypes = valueDefinitions.Select(v => v.Type).ToArray();
        var valNulls = valueDefinitions.Select(v => v.IsNullable).ToArray();

        var metadata = context.Metadata with
        {
            Keys = new QueryKeyShape(keyNames, keyTypes, keyNulls),
            Projection = new QueryProjectionShape(valNames, valTypes, valNulls)
        };

        context.UpdateMetadata(metadata);
    }

    private static StreamSettings ResolveStreamSettings(BuildContext context)
    {
        var model = context.Model;
        var metadata = context.Metadata;

        var partitions = model.Partitions > 0 ? model.Partitions : 1;
        var replicas = model.ReplicationFactor > 0 ? model.ReplicationFactor : (short)1;
        var retentionMs = ResolveRetention(model, metadata, context.DefaultRowsStreamRetentionMs);

        metadata = metadata with { RetentionMs = retentionMs };
        context.UpdateMetadata(metadata);

        return new StreamSettings(partitions, replicas, retentionMs);
    }

    private static Ksql.Linq.Core.Models.PropertyMeta[] BuildPropertyMetas(
        EntityModel model,
        Type? sourceType,
        IReadOnlyList<(string Name, Type Type, bool IsNullable)> definitions)
    {
        var result = new Ksql.Linq.Core.Models.PropertyMeta[definitions.Count];
        for (var i = 0; i < definitions.Count; i++)
        {
            var definition = definitions[i];
            var prop = ResolveProperty(sourceType, definition.Name) ?? ResolveProperty(model.EntityType, definition.Name);
            result[i] = CreateMeta(prop, definition.Name, definition.Type, definition.IsNullable);
        }

        return result;
    }

    private static PropertyInfo? ResolveProperty(Type? root, string name)
        => root?.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

    private static Ksql.Linq.Core.Models.PropertyMeta CreateMeta(
        PropertyInfo? property,
        string name,
        Type type,
        bool isNullable)
    {
        if (property != null)
        {
            return Ksql.Linq.Core.Models.PropertyMeta.FromProperty(property);
        }

        return new Ksql.Linq.Core.Models.PropertyMeta
        {
            Name = name,
            SourceName = name,
            PropertyType = type,
            IsNullable = isNullable,
            Attributes = Array.Empty<Attribute>()
        };
    }

    private static string GenerateCreateStreamDdl(
        BuildContext context,
        IReadOnlyList<Ksql.Linq.Core.Models.PropertyMeta> keyMetas,
        IReadOnlyList<Ksql.Linq.Core.Models.PropertyMeta> valueMetas,
        StreamSettings settings,
        string? timestampColumn)
    {
        static string MapTypeMeta(Ksql.Linq.Core.Models.PropertyMeta meta)
            => Ksql.Linq.Query.Schema.KsqlTypeMapping.MapToKsqlType(meta.PropertyType, meta.PropertyInfo, meta.Precision, meta.Scale);

        var cols = new List<string>();
        foreach (var meta in keyMetas)
        {
            cols.Add($"{meta.Name.ToUpperInvariant()} {MapTypeMeta(meta)} KEY");
        }

        var keyNames = keyMetas
            .Select(meta => meta.Name)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Select(name => name!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var meta in valueMetas)
        {
            if (keyNames.Contains(meta.Name))
            {
                continue;
            }

            cols.Add($"{meta.Name.ToUpperInvariant()} {MapTypeMeta(meta)}");
        }

        var withParts = WithClauseUtils.BuildWithParts(
            kafkaTopic: context.Name,
            hasKey: keyMetas.Any(k => !string.IsNullOrWhiteSpace(k.Name)),
            valueSchemaFullName: null,
            timestampColumn: timestampColumn,
            partitions: settings.Partitions,
            replicas: settings.Replicas,
            retentionMs: settings.RetentionMs);

        return $"CREATE STREAM IF NOT EXISTS {context.Name} ({string.Join(", ", cols)}) WITH ({string.Join(", ", withParts)});";
    }

    private static void TryRecordHubInput(string name, IReadOnlyList<string> valueNames)
    {
        try
        {
            HubInputIntrospector.Record(name, valueNames.ToArray());
        }
        catch
        {
        }
    }

    private static string BuildWindowedDdl(BuildContext context)
    {
        var qm = context.QueryModel;
        var metadata = context.Metadata;
        var inputOverride = context.InputOverride;
        var model = context.Model;

        var isHubInput = !string.IsNullOrWhiteSpace(inputOverride) &&
                         inputOverride.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase);

        if (isHubInput)
        {
            if (qm.SelectProjection != null)
            {
                qm.SelectProjection = HubRowsProjectionAdapter.Adapt(qm.SelectProjection);
            }

            var meta = ProjectionMetadataAnalyzer.Build(qm, isHubInput: true);
            qm.SelectProjectionMetadata = meta;
            qm.Extras["projectionMetadata"] = meta;

            System.Collections.Generic.ISet<string>? hubCols = null;
            if (!string.IsNullOrWhiteSpace(inputOverride))
            {
                hubCols = HubInputIntrospector.TryGetColumns(inputOverride);
                if (hubCols != null)
                {
                    qm.Extras["hub/availableColumns"] = hubCols;
                }
            }

            HubSelectPolicy.BuildOverridesAndExcludes(
                meta,
                out var selectOverrides,
                out var selectExclude,
                availableColumns: hubCols);

            try
            {
                var ovLog = selectOverrides?.Keys != null ? string.Join(",", selectOverrides.Keys) : "<null>";
                var exLog = selectExclude != null && selectExclude.Count > 0 ? string.Join(",", selectExclude) : "<null>";
                System.Console.WriteLine($"[hub] overrides={ovLog} exclude={exLog}");
            }
            catch { }

            qm.Extras["select/overrides"] = selectOverrides;
            qm.Extras["select/exclude"] = selectExclude;
        }

        if (context.Role != Role.Live && !qm.Extras.ContainsKey("valueSchemaFullName"))
        {
            var nsValue = metadata.Namespace;
            if (!string.IsNullOrWhiteSpace(nsValue))
            {
                qm.Extras["valueSchemaFullName"] = $"{nsValue}.{context.Name}_valueAvro";
            }
        }

        var sinkPartitions = model.Partitions > 0 ? model.Partitions : 1;
        var sinkReplicas = model.ReplicationFactor > 0 ? model.ReplicationFactor : (short)1;
        var sinkRetentionMs = ResolveRetention(model, metadata, context.DefaultRowsStreamRetentionMs);

        metadata = metadata with { RetentionMs = sinkRetentionMs };
        context.UpdateMetadata(metadata);

        int? graceSeconds = null;
        if (metadata.GraceSeconds.HasValue && metadata.GraceSeconds.Value > 0)
        {
            graceSeconds = metadata.GraceSeconds.Value;
        }

        // Live 縺ｧ繧よ兜蠖ｱ繝｡繧ｿ繧貞酔譛溘＠縺ｦ縲・寔險亥・繧貞性繧蛟､繧ｷ繧ｧ繧､繝励ｒ遒ｺ螳夲ｼ医・繝・ヴ繝ｳ繧ｰ貍上ｌ髦ｲ豁｢・・        try
        {
            var sourceType = (qm.SourceTypes?.Length ?? 0) > 0 ? qm.SourceTypes![0] : null;
            var projectionMetaForLive = qm.SelectProjectionMetadata ?? ProjectionMetadataAnalyzer.Build(qm, isHubInput: isHubInput);
            if (projectionMetaForLive != null)
            {
                var keyDefsLive = BuildKeyDefinitions(context, sourceType, projectionMetaForLive);
                var valResLive = BuildValueDefinitions(context, sourceType, projectionMetaForLive);
                var orderedValsLive = OrderValueDefinitions(valResLive, projectionMetaForLive);
                ApplyKeyAndValueShapes(context, keyDefsLive, orderedValsLive);
            }
        }
        catch { }

        var ddl = Ksql.Linq.Query.Planning.DdlPlanner.BuildWindowedCtas(
            name: context.Name,
            model: qm,
            timeframe: context.Timeframe,
            graceSeconds: graceSeconds,
            inputOverride: inputOverride,
            partitions: sinkPartitions,
            replicas: (int)sinkReplicas,
            retentionMs: sinkRetentionMs,
            emitOverride: context.EmitClause);

        Console.WriteLine($"LiveDDL[{context.Name}]: {ddl}");
        return ddl;
    }

    private static (string TimestampName, Type TimestampType, bool TimestampNullable) ResolveTimestamp(
        Type? sourceType,
        QueryMetadata metadata)
    {
        string? timestampName = null;
        Type? timestampType = null;
        var timestampNullable = false;

        if (sourceType != null)
        {
            var tsProp = sourceType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(p => p.GetCustomAttribute<KsqlTimestampAttribute>(inherit: true) != null);
            if (tsProp != null)
            {
                timestampName = tsProp.Name;
                timestampType = tsProp.PropertyType;
                timestampNullable = Nullable.GetUnderlyingType(tsProp.PropertyType) != null ||
                                    !tsProp.PropertyType.IsValueType;
            }
        }

        if (string.IsNullOrWhiteSpace(timestampName) && !string.IsNullOrWhiteSpace(metadata.TimestampColumn))
        {
            timestampName = metadata.TimestampColumn;
        }

        if (string.IsNullOrWhiteSpace(timestampName))
        {
            timestampName = "Timestamp";
        }

        timestampType ??= typeof(DateTime);
        return (timestampName, timestampType, timestampNullable);
    }

    private static long? ResolveRetention(EntityModel model, QueryMetadata metadata, long defaultMs)
    {
        if (metadata.RetentionMs.HasValue && metadata.RetentionMs.Value > 0)
        {
            return metadata.RetentionMs.Value;
        }

        if (WithClauseUtils.TryConvertRetention(model.AdditionalSettings, "retentionMs", out var camel))
        {
            return camel;
        }

        if (WithClauseUtils.TryConvertRetention(model.AdditionalSettings, "retention.ms", out var dotted))
        {
            return dotted;
        }

        return defaultMs > 0 ? defaultMs : (long?)null;
    }

    private sealed record ValueDefinitionResult(
        List<(string Name, Type Type, bool IsNullable)> Values,
        string TimestampName,
        string[] ProjectionNames);

    private sealed record StreamSettings(int Partitions, short Replicas, long? RetentionMs);

    private sealed class BuildContext
    {
        public BuildContext(
            string name,
            string timeframe,
            Role role,
            KsqlQueryModel queryModel,
            EntityModel model,
            QueryMetadata metadata,
            string? inputOverride,
            string? emitClause,
            Func<string, Type> resolveType,
            long defaultRowsStreamRetentionMs)
        {
            Name = name;
            Timeframe = timeframe;
            Role = role;
            QueryModel = queryModel;
            Model = model;
            Metadata = metadata;
            InputOverride = inputOverride;
            EmitClause = emitClause;
            ResolveType = resolveType;
            DefaultRowsStreamRetentionMs = defaultRowsStreamRetentionMs;
        }

        public string Name { get; }
        public string Timeframe { get; }
        public Role Role { get; }
        public KsqlQueryModel QueryModel { get; }
        public EntityModel Model { get; }
        public string? InputOverride { get; }
        public string? EmitClause { get; }
        public Func<string, Type> ResolveType { get; }
        public long DefaultRowsStreamRetentionMs { get; }
        public QueryMetadata Metadata { get; private set; }

        public void UpdateMetadata(QueryMetadata metadata)
        {
            Metadata = metadata;
            QueryMetadataWriter.Apply(Model, metadata);
        }
    }
}
//debug