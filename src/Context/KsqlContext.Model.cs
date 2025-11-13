using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Metadata;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Ksql.Linq;

public abstract partial class KsqlContext
{
    public IEntitySet<T> Set<T>() where T : class
    {
        var entityType = typeof(T);

        if (_entitySets.TryGetValue(entityType, out var existingSet))
        {
            return (IEntitySet<T>)existingSet;
        }

        var entityModel = GetOrCreateEntityModel<T>();
        var entitySet = CreateEntitySet<T>(entityModel);
        _entitySets[entityType] = entitySet;

        return entitySet;
    }

    public object GetEventSet(Type entityType)
    {
        if (_entitySets.TryGetValue(entityType, out var entitySet))
        {
            return entitySet;
        }

        var entityModel = GetOrCreateEntityModel(entityType);
        var createdSet = CreateEntitySet(entityType, entityModel);
        _entitySets[entityType] = createdSet;

        return createdSet;
    }

    public Dictionary<Type, EntityModel> GetEntityModels()
    {
        return new Dictionary<Type, EntityModel>(_entityModels);
    }

    public IReadOnlyDictionary<Type, Configuration.ResolvedEntityConfig> GetResolvedEntityConfigs()
    {
        return _resolvedConfigs;
    }

    private void InitializeEventSetProperties(ModelBuilder builder)
    {
        var contextType = GetType();
        var eventSetProps = contextType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.CanWrite
                && p.PropertyType.IsGenericType
                && p.PropertyType.GetGenericTypeDefinition() == typeof(EventSet<>));

        foreach (var prop in eventSetProps)
        {
            if (prop.GetValue(this) != null)
                continue;

            var entityType = prop.PropertyType.GetGenericArguments()[0];
            builder.AddEntityModel(entityType);
            var model = EnsureEntityModel(entityType);
            var set = CreateEntitySet(entityType, model);
            _entitySets[entityType] = set;
            prop.SetValue(this, set);
        }
    }

    protected virtual object CreateEntitySet(Type entityType, EntityModel entityModel)
    {
        var method = GetType()
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .FirstOrDefault(m =>
                m.Name == nameof(CreateEntitySet)
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == 1
                && m.GetParameters().Length == 1
                && m.GetParameters()[0].ParameterType == typeof(EntityModel)
            );

        if (method == null)
            throw new InvalidOperationException("Generic CreateEntitySet<T>(EntityModel) not found!");

        // After this
        var genericMethod = method.MakeGenericMethod(entityType);
        return genericMethod.Invoke(this, new object[] { entityModel })!;
    }

    protected void ConfigureModel()
    {
        var modelBuilder = new ModelBuilder(ValidationMode.Strict);
        InitializeEventSetProperties(modelBuilder);
        using (Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            OnModelCreating(modelBuilder);
        }
        ApplyModelBuilderSettings(modelBuilder);
    }

    private void ResolveEntityConfigurations()
    {
        _resolvedConfigs.Clear();

        foreach (var (type, model) in _entityModels)
        {
            var config = _dslOptions.Entities.FirstOrDefault(e => string.Equals(e.Entity, type.Name, StringComparison.OrdinalIgnoreCase));

            var defaultTopic = model.TopicName ?? type.GetKafkaTopicName();
            var sourceTopic = config?.SourceTopic ?? defaultTopic;

            var defaultCache = model.EnableCache;
            bool enableCache = false;
            if (model.StreamTableType == StreamTableType.Table)
            {
                enableCache = config?.EnableCache ?? defaultCache;
            }

            var metadata = model.GetOrCreateMetadata();

            if (!string.IsNullOrWhiteSpace(config?.StoreName))
            {
                metadata = metadata with { StoreName = config.StoreName };
                QueryMetadataWriter.Apply(model, metadata);
            }
            var storeName = PromoteStoreName(model, ref metadata);

            if (!string.IsNullOrWhiteSpace(config?.BaseDirectory))
            {
                metadata = metadata with { BaseDirectory = config.BaseDirectory };
                QueryMetadataWriter.Apply(model, metadata);
            }
            else
            {
                PromoteBaseDirectory(model, ref metadata);
            }

            string? groupId = null;
            if (_dslOptions.Topics.TryGetValue(sourceTopic, out var topicSection) && !string.IsNullOrEmpty(topicSection.Consumer.GroupId))
            {
                groupId = topicSection.Consumer.GroupId;
                if (!string.IsNullOrEmpty(model.GroupId) && model.GroupId != groupId)
                {
                    _logger.LogWarning("GroupId for {Entity} overridden by configuration: {Config} (was {Dsl})", type.Name, groupId, model.GroupId);
                }
            }
            else if (!string.IsNullOrEmpty(model.GroupId))
            {
                groupId = model.GroupId;
            }

            if (config != null && config.EnableCache != defaultCache)
            {
                _logger.LogInformation("EnableCache for {Entity} set to {Value} from configuration", type.Name, enableCache);
            }

            var resolved = new Configuration.ResolvedEntityConfig
            {
                Entity = type.Name,
                SourceTopic = sourceTopic,
                GroupId = groupId,
                EnableCache = enableCache,
                StoreName = storeName
            };

            foreach (var kv in model.AdditionalSettings)
            {
                resolved.AdditionalSettings[kv.Key] = kv.Value;
            }

            _resolvedConfigs[type] = resolved;
        }

        _dslOptions.Entities.Clear();
        foreach (var rc in _resolvedConfigs.Values)
        {
            _dslOptions.Entities.Add(new EntityConfiguration
            {
                Entity = rc.Entity,
                SourceTopic = rc.SourceTopic,
                EnableCache = rc.EnableCache,
                StoreName = rc.StoreName,
                BaseDirectory = rc.AdditionalSettings.TryGetValue("BaseDirectory", out var bd) ? bd?.ToString() : null
            });
        }
    }

    private void InitializeEntityModels()
    {
        using (Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var dlqModel = CreateEntityModelFromType(typeof(Messaging.DlqEnvelope));
            dlqModel.SetStreamTableType(Query.Abstractions.StreamTableType.Stream);
            dlqModel.TopicName = GetDlqTopicName();
            dlqModel.AccessMode = Core.Abstractions.EntityAccessMode.ReadOnly;
            _entityModels[typeof(Messaging.DlqEnvelope)] = dlqModel;
            var metadata = dlqModel.GetOrCreateMetadata();
            var ns = PromoteNamespace(dlqModel, ref metadata);
            _mappingRegistry.RegisterEntityModel(dlqModel, overrideNamespace: ns);
        }
    }

    private void ApplyModelBuilderSettings(ModelBuilder modelBuilder)
    {
        var models = modelBuilder.GetAllEntityModels();
        foreach (var (type, model) in models)
        {
            if (model.QueryModel != null)
            {
                RegisterQueryModelMapping(model);
                _entityModels[type] = model;
                continue;
            }

            if (_entityModels.TryGetValue(type, out var existing))
            {
                existing.SetStreamTableType(model.GetExplicitStreamTableType());
                existing.ErrorAction = model.ErrorAction;
                existing.DeserializationErrorPolicy = model.DeserializationErrorPolicy;
                existing.EnableCache = model.EnableCache;
                existing.BarTimeSelector = model.BarTimeSelector;
            }
            else
            {
                _entityModels[type] = model;
            }

            var metadata = model.GetOrCreateMetadata();
            var ns = PromoteNamespace(model, ref metadata);
            var isTable = model.StreamTableType == StreamTableType.Table;
            _mappingRegistry.RegisterEntityModel(
                model,
                genericKey: isTable,
                genericValue: isTable,
                    overrideNamespace: ns);
        }
    }

    private EntityModel GetOrCreateEntityModel<T>() where T : class
    {
        return GetOrCreateEntityModel(typeof(T));
    }

    private EntityModel GetOrCreateEntityModel(Type entityType)
    {
        if (_entityModels.TryGetValue(entityType, out var existingModel))
        {
            return existingModel;
        }

        var entityModel = CreateEntityModelFromType(entityType);
        _entityModels[entityType] = entityModel;
        return entityModel;
    }

    private EntityModel CreateEntityModelFromType(Type entityType)
    {
        var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var keyProperties = allProperties
            .Select(p => new { Property = p, Attr = p.GetCustomAttribute<KsqlKeyAttribute>() })
            .Where(x => x.Attr != null)
            .OrderBy(x => x.Attr!.Order)
            .Select(x => x.Property)
            .ToArray();

        var model = new EntityModel
        {
            EntityType = entityType,
            TopicName = entityType.GetKafkaTopicName(),
            Partitions = 1,
            ReplicationFactor = 1,
            AllProperties = allProperties,
            KeyProperties = keyProperties

        };

        if (entityType.GetCustomAttribute<KsqlTableAttribute>() != null)
        {
            model.SetStreamTableType(StreamTableType.Table);
        }

        if (model.StreamTableType == StreamTableType.Stream)
        {
            model.EnableCache = false;
        }
        else
        {
            model.EnableCache = true;
        }

        var topicAttr = entityType.GetCustomAttribute<KsqlTopicAttribute>();
        if (topicAttr != null)
        {
            model.TopicName = topicAttr.Name;
            model.Partitions = topicAttr.PartitionCount;
            model.ReplicationFactor = topicAttr.ReplicationFactor;
        }

        if (entityType == typeof(Messaging.DlqEnvelope))
        {
            model.TopicName = _dslOptions.DlqTopicName;
            model.Partitions = _dslOptions.DlqOptions.NumPartitions;
            model.ReplicationFactor = _dslOptions.DlqOptions.ReplicationFactor;
        }
        else
        {
            var config = _dslOptions.Entities.FirstOrDefault(e =>
                string.Equals(e.Entity, entityType.Name, StringComparison.OrdinalIgnoreCase));

            var metadata = model.GetOrCreateMetadata();
            if (config != null)
            {
                if (!string.IsNullOrEmpty(config.SourceTopic))
                    model.TopicName = config.SourceTopic;

                if (model.StreamTableType == StreamTableType.Table)
                    model.EnableCache = config.EnableCache;

                if (!string.IsNullOrWhiteSpace(config.StoreName))
                {
                    metadata = metadata with { StoreName = config.StoreName };
                    QueryMetadataWriter.Apply(model, metadata);
                }

                if (!string.IsNullOrWhiteSpace(config.BaseDirectory))
                {
                    metadata = metadata with { BaseDirectory = config.BaseDirectory };
                    QueryMetadataWriter.Apply(model, metadata);
                }
            }
            else
            {
                PromoteStoreName(model, ref metadata);
                PromoteBaseDirectory(model, ref metadata);
            }
        }

        var validation = new ValidationResult { IsValid = true };
        if (keyProperties.Length == 0)
        {
            validation.Warnings.Add($"No key properties defined for {entityType.Name}");
        }
        model.ValidationResult = validation;

        return model;
    }

    internal EntityModel EnsureEntityModel(Type entityType, EntityModel? model = null)
    {
        if (_entityModels.TryGetValue(entityType, out var existing))
        {
            if (existing.QueryModel == null && model?.QueryModel != null)
            {
                existing.QueryModel = model.QueryModel;
                foreach (var kv in model.AdditionalSettings)
                {
                    existing.AdditionalSettings[kv.Key] = kv.Value;
                }
                existing.QueryMetadata = QueryMetadataFactory.FromAdditionalSettings(existing.AdditionalSettings);
                existing.SetStreamTableType(model.GetExplicitStreamTableType());
                RegisterOrUpdateMapping(existing);
            }
            else
            {
                RegisterOrUpdateMapping(existing);
            }
            return existing;
        }

        if (model == null)
        {
            model = CreateEntityModelFromType(entityType);
        }
        else
        {
            if (!_entityModels.TryGetValue(entityType, out _))
            {
                _entityModels[entityType] = model;
            }
        }
        model.QueryMetadata ??= QueryMetadataFactory.FromAdditionalSettings(model.AdditionalSettings);
        RegisterOrUpdateMapping(model);
        return model;
    }

    private void RegisterOrUpdateMapping(EntityModel model)
    {
        if (model == null)
            return;

        try
        {
            if (model.QueryModel != null)
            {
                var metadata = model.GetOrCreateMetadata();
                // Guard: if a non-empty mapping is already registered, avoid overwriting it.
                try
                {
                    var existing = _mappingRegistry.GetMapping(model.EntityType);
                    var hasFields = existing?.AvroValueRecordSchema?.Fields != null && existing.AvroValueRecordSchema.Fields.Count > 0;
                    if (hasFields)
                        return;
                }
                catch { /* no existing mapping -> continue */ }

                var ns2 = PromoteNamespace(model, ref metadata);

                // Prefer explicit shapes captured in AdditionalSettings when available (derived entities path),
                // to prevent generating empty schemas from placeholder result types.
                // Extract shapes from metadata
                var keyNamesArr = metadata.Keys.Names ?? Array.Empty<string>();
                var keyTypesArr = metadata.Keys.Types ?? Array.Empty<Type>();
                var valNamesArr = metadata.Projection.Names ?? Array.Empty<string>();
                var valTypesArr = metadata.Projection.Types ?? Array.Empty<Type>();

                var hasKeyNames = keyNamesArr.Length > 0;
                var hasKeyTypes = keyTypesArr.Length > 0;
                var hasValNames = valNamesArr.Length > 0;
                var hasValTypes = valTypesArr.Length > 0;

                if (hasKeyNames && hasKeyTypes && hasValNames && hasValTypes)
                {
                    // Avoid role-name dependency: detect rows hub by id suffix *_1s_rows
                    var isRows = metadata.Identifier?.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase) == true;
                    // Build PropertyMeta arrays from the captured shapes
                    var keyLen = Math.Min(keyNamesArr!.Length, keyTypesArr!.Length);
                    var keyMeta = new Core.Models.PropertyMeta[keyLen];
                    for (int i = 0; i < keyLen; i++)
                    {
                        keyMeta[i] = new Core.Models.PropertyMeta
                        {
                            Name = keyNamesArr![i],
                            SourceName = keyNamesArr![i],
                            PropertyType = keyTypesArr![i],
                            IsNullable = false,
                            Attributes = Array.Empty<Attribute>()
                        };
                    }

                    // Build value meta excluding key columns (schema/value must not include key fields)
                    var keySet = new System.Collections.Generic.HashSet<string>(System.StringComparer.OrdinalIgnoreCase);
                    for (int ki = 0; ki < keyLen; ki++) keySet.Add(keyMeta[ki].Name);
                    var vals = new System.Collections.Generic.List<Core.Models.PropertyMeta>();
                    var valLen = Math.Min(valNamesArr!.Length, valTypesArr!.Length);
                    for (int i = 0; i < valLen; i++)
                    {
                        var vname = valNamesArr![i];
                        if (keySet.Contains(vname)) continue;
                        vals.Add(new Core.Models.PropertyMeta
                        {
                            Name = vname,
                            SourceName = vname,
                            PropertyType = valTypesArr![i],
                            IsNullable = true,
                            Attributes = System.Array.Empty<System.Attribute>()
                        });
                    }
                    var valMeta = vals.ToArray();

                    // rowsはGenericRecordでSRに合わせる。TABLEもGenericRecordでSRのスキーマに厳密追従させる。
                    var isTable = model.StreamTableType == StreamTableType.Table;
                    _mappingRegistry.RegisterMeta(
                        model.EntityType,
                        (keyMeta, valMeta),
                        model.GetTopicName(),
                        genericKey: isRows || isTable,
                        genericValue: isRows || isTable,
                        overrideNamespace: ns2);
                }
                else
                {
                    // Fallback: derive mapping from the query model (tables typically)
                    var isTable = model.StreamTableType == StreamTableType.Table;
                    var isRows2 = metadata.Identifier?.EndsWith("_1s_rows", StringComparison.OrdinalIgnoreCase) == true;
                    _mappingRegistry.RegisterQueryModel(
                        model.EntityType,
                        model.QueryModel!,
                        model.KeyProperties,
                        model.GetTopicName(),
                        genericKey: isRows2 || isTable,
                        genericValue: isRows2 || isTable,
                        overrideNamespace: ns2);
                }
            }
            else
            {
                EnsureMappingRegistered(model);
            }
        }
        catch (InvalidOperationException)
        {
            EnsureMappingRegistered(model);
        }
    }

    private void EnsureMappingRegistered(EntityModel model)
    {
        if (model == null)
            return;

        try
        {
            _ = _mappingRegistry.GetMapping(model.EntityType);
        }
        catch (InvalidOperationException)
        {
            var metadata = model.GetOrCreateMetadata();
            var ns2 = PromoteNamespace(model, ref metadata);
            var isTable = model.StreamTableType == StreamTableType.Table;
            var forceGenericKey = PromoteForceGenericKey(model, ref metadata);
            var forceGenericValue = PromoteForceGenericValue(model, ref metadata);
            if (model.QueryModel != null)
            {
                _mappingRegistry.RegisterQueryModel(
                    model.EntityType,
                    model.QueryModel!,
                    model.KeyProperties,
                    model.GetTopicName(),
                    genericKey: forceGenericKey || isTable,
                    genericValue: forceGenericValue || isTable,
                    overrideNamespace: ns2);
            }
            else
            {
                _mappingRegistry.RegisterEntityModel(
                    model,
                    genericKey: forceGenericKey,
                    genericValue: forceGenericValue || isTable,
                    overrideNamespace: ns2);
            }
        }
    }

    private static string? PromoteNamespace(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.Namespace))
            return metadata.Namespace;
        if (model.AdditionalSettings.TryGetValue("namespace", out var nsObj))
        {
            var value = nsObj?.ToString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                metadata = metadata with { Namespace = value };
                QueryMetadataWriter.Apply(model, metadata);
                return value;
            }
        }
        return metadata.Namespace;
    }

    private static string? PromoteStoreName(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.StoreName))
            return metadata.StoreName;
        if (model.AdditionalSettings.TryGetValue("StoreName", out var sObj))
        {
            var value = sObj?.ToString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                metadata = metadata with { StoreName = value };
                QueryMetadataWriter.Apply(model, metadata);
                return value;
            }
        }
        return metadata.StoreName;
    }

    private static string? PromoteBaseDirectory(EntityModel model, ref QueryMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(metadata.BaseDirectory))
            return metadata.BaseDirectory;
        if (model.AdditionalSettings.TryGetValue("BaseDirectory", out var bdObj))
        {
            var value = bdObj?.ToString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                metadata = metadata with { BaseDirectory = value };
                QueryMetadataWriter.Apply(model, metadata);
                return value;
            }
        }
        return metadata.BaseDirectory;
    }

    private static bool PromoteForceGenericKey(EntityModel model, ref QueryMetadata metadata)
    {
        if (metadata.ForceGenericKey.HasValue)
            return metadata.ForceGenericKey.Value;
        if (model.AdditionalSettings.TryGetValue("forceGenericKey", out var fgkObj))
        {
            var parsed = TryCoerceBool(fgkObj);
            if (parsed.HasValue)
            {
                metadata = metadata with { ForceGenericKey = parsed.Value };
                QueryMetadataWriter.Apply(model, metadata);
                return parsed.Value;
            }
        }
        return metadata.ForceGenericKey ?? false;
    }

    private static bool PromoteForceGenericValue(EntityModel model, ref QueryMetadata metadata)
    {
        if (metadata.ForceGenericValue.HasValue)
            return metadata.ForceGenericValue.Value;
        if (model.AdditionalSettings.TryGetValue("forceGenericValue", out var fgvObj))
        {
            var parsed = TryCoerceBool(fgvObj);
            if (parsed.HasValue)
            {
                metadata = metadata with { ForceGenericValue = parsed.Value };
                QueryMetadataWriter.Apply(model, metadata);
                return parsed.Value;
            }
        }
        return metadata.ForceGenericValue ?? false;
    }

    private static bool? TryCoerceBool(object? value)
        => value switch
        {
            bool b => b,
            string s when bool.TryParse(s, out var parsed) => parsed,
            _ => (bool?)null
        };

}
