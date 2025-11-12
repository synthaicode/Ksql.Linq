using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Adapters;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Core;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Mapping;
using Ksql.Linq;
using Ksql.Linq.Core.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using System.IO;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Ksql.Linq.Query.Metadata;

namespace Ksql.Linq.Query.Analysis;

internal static class DerivedTumblingPipeline
{
    private const long DefaultRowsStreamRetentionMs = 7L * 24 * 60 * 60 * 1000;
    public sealed record ExecutionResult(
        EntityModel Model,
        Role Role,
        string Statement,
        string? InputTopic,
        KsqlDbResponse Response,
        string? QueryId)
    {
        public string TargetTopic => Model.GetTopicName();
        public bool IsPersistentQuery => Statement.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    public static async Task<IReadOnlyList<ExecutionResult>> RunAsync(
        TumblingQao qao,
        EntityModel baseModel,
        KsqlQueryModel queryModel,
        Func<EntityModel, string, Task<KsqlDbResponse>> execute,
        Func<string, Type> resolveType,
        MappingRegistry mapping,
        ConcurrentDictionary<Type, EntityModel> registry,
        ILogger logger,
        Func<ExecutionResult, Task>? afterExecuteAsync = null,
        Action<EntityModel>? applyTopicSettings = null)
    {
        var executions = new List<ExecutionResult>();
        var baseAttr = baseModel.EntityType.GetCustomAttribute<KsqlTopicAttribute>();
        var baseName = ModelNaming.GetBaseId(baseModel);
        var entities = PlanDerivedEntities(qao, baseModel);
        var models = AdaptModels(entities);
        // Ensure deterministic and dependency-safe ordering:
        // - Create 1s TABLE before the hub STREAM to ensure topic exists
        // - Then other roles ordered by role priority and timeframe ascending
        static int RolePriority(Role role) => role switch
        {
            Role.Final1sStream => 0,
            Role.Live => 1,
            _ => 9
        };

        var passRows = new List<EntityModel>();
        var restCandidates = new List<(EntityModel Model, Role Role, string Timeframe)>();
        foreach (var model in models)
        {
            var metadata = model.GetOrCreateMetadata();
            var identifier = EnsureIdentifier(model, ref metadata);
            if (!string.IsNullOrWhiteSpace(identifier) && identifier.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase))
            {
                passRows.Add(model);
                continue;
            }

            var role = EnsureRoleOrDefault(model, ref metadata, Role.Live);
            var timeframe = EnsureTimeframeOrDefault(model, ref metadata, "1s");
            restCandidates.Add((model, role, timeframe));
        }

        var rest = restCandidates
            .OrderBy(x => RolePriority(x.Role))
            .ThenBy(x => TimeframeUtils.ToSeconds(x.Timeframe))
            .Select(x => x.Model)
            .ToList();

        var executionOrder = passRows.Concat(rest);
        foreach (var m in executionOrder)
        {
            var metadata = m.GetOrCreateMetadata();
            if (string.IsNullOrWhiteSpace(m.TopicName) && !string.IsNullOrWhiteSpace(metadata.Identifier))
                m.TopicName = metadata.Identifier;

            // Apply per-topic sizing from configuration (appsettings.json) if provided by caller
            applyTopicSettings?.Invoke(m);
            if (m.AdditionalSettings.Count > 0)
            {
                var refreshed = QueryMetadataFactory.FromAdditionalSettings(m.AdditionalSettings);
                m.SetMetadata(refreshed);
            }
            metadata = m.GetOrCreateMetadata();
            var role = EnsureRoleOrDefault(m, ref metadata, Role.Live);
            var tf = EnsureTimeframeOrDefault(m, ref metadata, "1s");
            var allow = role == Role.Final1sStream ? tf == "1s" : true;
            if (!allow)
                continue;
            var (ddl, dt, ns, inputOverride, shouldExecute) = DerivedEntityDdlPlanner.Build(
                baseName,
                queryModel,
                m,
                role,
                resolveType,
                DefaultRowsStreamRetentionMs);
            // Register TimeBucket read mapping so TimeBucket<T> resolves to the
            // concrete per-timeframe entity type instead of the base class.
            if (!shouldExecute || string.IsNullOrWhiteSpace(ddl))
            {
                registry[dt] = m;
                var skipResult = new ExecutionResult(m, role, ddl ?? string.Empty, inputOverride, new KsqlDbResponse(true, string.Empty), null);
                executions.Add(skipResult);
                if (afterExecuteAsync != null)
                    await afterExecuteAsync(skipResult).ConfigureAwait(false);
                continue;
            }
            try
            {
                Ksql.Linq.Runtime.Period period = TimeframeUtils.ToPeriod(tf);
                Ksql.Linq.Runtime.TimeBucketTypes.RegisterRead(baseModel.EntityType, period, dt);
            }
            catch { /* best-effort; do not block DDL */ }
            logger.LogInformation("KSQL DDL (derived {Entity}): {Sql}", m.TopicName, ddl);
            var response = await execute(m, ddl);
            var queryId = Ksql.Linq.Query.Builders.Utilities.QueryIdUtils.ExtractQueryId(response);
            LogDdlResponse(m, ddl, response, queryId);
            var result = new ExecutionResult(m, role, ddl, inputOverride, response, queryId);
            executions.Add(result);
            if (afterExecuteAsync != null)
                await afterExecuteAsync(result).ConfigureAwait(false);
            // Register mapping using explicit shapes captured in metadata
            if (role == Role.Final1sStream || role == Role.Live)
            {
                try
                {
                    // Skip when mapping already exists (e.g., rows injected as simple entity)
                    try { _ = mapping.GetMapping(dt); goto SkipRegister; } catch { }
                    var derivedMeta = m.GetOrCreateMetadata();
                    var keyNames = derivedMeta.Keys.Names ?? Array.Empty<string>();
                    var keyTypes = derivedMeta.Keys.Types ?? Array.Empty<Type>();
                    var valNames = derivedMeta.Projection.Names ?? Array.Empty<string>();
                    var valTypes = derivedMeta.Projection.Types ?? Array.Empty<Type>();
                    var keyMeta = new Ksql.Linq.Core.Models.PropertyMeta[Math.Min(keyNames.Length, keyTypes.Length)];
                    for (int i = 0; i < keyMeta.Length; i++)
                    {
                        var kname = keyNames[i];
                        var kprop = baseModel.EntityType.GetProperty(kname, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                        keyMeta[i] = kprop != null
                            ? Ksql.Linq.Core.Models.PropertyMeta.FromProperty(kprop)
                            : new Ksql.Linq.Core.Models.PropertyMeta
                            {
                                Name = kname,
                                SourceName = kname,
                                PropertyType = keyTypes[i],
                                IsNullable = false,
                                Attributes = Array.Empty<Attribute>()
                            };
                    }
                    // build value meta excluding key names
                    var keySet3 = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    for (int ki = 0; ki < keyMeta.Length; ki++) keySet3.Add(keyMeta[ki].Name);
                    var vals3 = new System.Collections.Generic.List<Ksql.Linq.Core.Models.PropertyMeta>();
                    for (int i = 0; i < valNames.Length && i < valTypes.Length; i++)
                    {
                        var vname = valNames[i];
                        if (keySet3.Contains(vname)) continue;
                        var vprop = baseModel.EntityType.GetProperty(vname, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                        if (vprop != null)
                            vals3.Add(Ksql.Linq.Core.Models.PropertyMeta.FromProperty(vprop));
                        else
                            vals3.Add(new Ksql.Linq.Core.Models.PropertyMeta
                            {
                                Name = vname,
                                SourceName = vname,
                                PropertyType = valTypes[i],
                                IsNullable = true,
                                Attributes = Array.Empty<Attribute>()
                            });
                    }
                    var valMeta = vals3.ToArray();
                    // CTASを構成するハブ(rows)はSR既存スキーマに合わせやすいGenericRecordを用いる
                    bool isRows = role == Role.Final1sStream;
                    bool isTimeframeTableValueGeneric = role == Role.Live;
                    // Use GenericRecord for keys across timeframe entities.
                    // Also unify value as GenericRecord for timeframe TABLEs (Live/Prev/Hb/Fill) to avoid SR fullname drift.
                    var kvMapping = mapping.RegisterMeta(dt, (keyMeta, valMeta), m.TopicName,
                        genericKey: true,
                        genericValue: isRows || isTimeframeTableValueGeneric,
                        overrideNamespace: ns);
                    if (kvMapping.AvroValueType == typeof(Avro.Generic.GenericRecord))
                    {
                        m.ValueSchemaFullName = null;
                    }
                    else if (string.IsNullOrWhiteSpace(m.ValueSchemaFullName))
                    {
                        m.ValueSchemaFullName = kvMapping.AvroValueRecordSchema?.Fullname;
                    }
                }
                catch { }
            SkipRegister: ;
            }
            registry[dt] = m;
        }

        return executions;
    }

    

    private static void LogDdlResponse(EntityModel model, string ddl, KsqlDbResponse response, string? queryId)
    {
        try
        {
            var path = ResolveDdlLogPath();
            if (string.IsNullOrWhiteSpace(path))
                return;

            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            var sb = new StringBuilder();
            sb.AppendLine("-----");
            sb.AppendLine($"UTC {DateTime.UtcNow:O}");
            sb.AppendLine($"Entity: {model.EntityType.FullName}");
            sb.AppendLine($"Topic: {model.TopicName}");
        var metadata = model.GetOrCreateMetadata();
        var role = EnsureRoleOrDefault(model, ref metadata, Role.Live);
        sb.AppendLine($"Role: {role}");
        var timeframe = EnsureTimeframeOrDefault(model, ref metadata, "1s");
            if (!string.IsNullOrWhiteSpace(timeframe))
                sb.AppendLine($"Timeframe: {timeframe}");
            sb.AppendLine($"QueryId: {queryId ?? "(none)"}");
            sb.AppendLine("DDL:");
            sb.AppendLine(ddl);
            sb.AppendLine("Response:");
            sb.AppendLine(response?.Message ?? string.Empty);

            File.AppendAllText(path, sb.ToString());
        }
        catch
        {
            // best-effort logging only
        }
    }

    private static string ResolveDdlLogPath()
    {
        var configured = Environment.GetEnvironmentVariable("KSQL_DDL_LOG_PATH");
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        try
        {
            var baseDir = AppContext.BaseDirectory ?? Directory.GetCurrentDirectory();
            return Path.Combine(baseDir, "reports", "physical", "ddl.log");
        }
        catch
        {
            return Path.Combine(Directory.GetCurrentDirectory(), "reports", "physical", "ddl.log");
        }
    }
    public static IReadOnlyList<DerivedEntity> PlanDerivedEntities(TumblingQao qao, EntityModel model)
        => DerivationPlanner.Plan(qao, model);
    public static IReadOnlyList<EntityModel> AdaptModels(IReadOnlyList<DerivedEntity> entities)
        => EntityModelAdapter.Adapt(entities);

    private static string? EnsureIdentifier(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.Identifier))
            return metadata.Identifier;
        return null;
    }

    private static Role EnsureRoleOrDefault(EntityModel model, ref QueryMetadata metadata, Role defaultRole)
    {
        var role = TryEnsureRole(model, ref metadata);
        if (role.HasValue)
            return role.Value;

        metadata = metadata with { Role = defaultRole.ToString() };
        QueryMetadataWriter.Apply(model, metadata);
        return defaultRole;
    }

    private static Role? TryEnsureRole(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.Role) && Enum.TryParse<Role>(metadata.Role, ignoreCase: true, out var parsedExisting))
            return parsedExisting;

        return null;
    }

    private static string EnsureTimeframeOrDefault(EntityModel model, ref QueryMetadata metadata, string defaultTimeframe)
    {
        var timeframe = TryEnsureTimeframe(model, ref metadata);
        if (!string.IsNullOrWhiteSpace(timeframe))
            return timeframe!;

        metadata = metadata with { TimeframeRaw = defaultTimeframe };
        QueryMetadataWriter.Apply(model, metadata);
        return defaultTimeframe;
    }

    private static string? TryEnsureTimeframe(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.TimeframeRaw))
            return metadata.TimeframeRaw;
        return null;
    }

    private static LambdaExpression BuildInputProjection(Type inputType)
    {
        // App-agnostic: select all columns (identity -> SELECT *)
        var p = Expression.Parameter(inputType, "x");
        return Expression.Lambda(p, p);
    }
    // Note: HubAggregationRewriter (expression-tree) was removed in favor of safe SELECT-clause adjustment above.

    

}


