
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Extensions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.RegularExpressions;

namespace Ksql.Linq.Mapping;

/// <summary>
/// Provides registration and lookup of dynamically generated key/value types
/// based on PropertyMeta information.
/// </summary>
internal class MappingRegistry
{
    private readonly ConcurrentDictionary<Type, KeyValueTypeMapping> _mappings = new();
    private readonly ModuleBuilder _moduleBuilder;
    private Type? _lastRegisteredType;
    private readonly ConcurrentDictionary<string, byte> _generatedTypeNames = new(StringComparer.OrdinalIgnoreCase);

    private static string AvroSanitizeName(string name)
    {
        var sanitized = Regex.Replace(name, @"[^A-Za-z0-9_]", "_");
        if (string.IsNullOrEmpty(sanitized))
            sanitized = "_";
        if (!Regex.IsMatch(sanitized[0].ToString(), "[A-Za-z_]"))
            sanitized = "_" + sanitized;
        return sanitized;
    }

    public MappingRegistry()
    {
        var asmName = new AssemblyName("KafkaKsqlDynamicMappings");
        var asmBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        _moduleBuilder = asmBuilder.DefineDynamicModule("Main");
    }

    public KeyValueTypeMapping Register(
        Type pocoType,
        PropertyMeta[] keyProperties,
        PropertyMeta[] valueProperties,
        string? topicName = null,
        bool genericKey = false,
        bool genericValue = false,
        string? overrideNamespace = null)
    {
        if (_mappings.TryGetValue(pocoType, out var existing))
        {
            bool keyMatches = genericKey
                ? existing.AvroKeyType == typeof(Avro.Generic.GenericRecord)
                : existing.AvroKeyType != typeof(Avro.Generic.GenericRecord);
            bool valueMatches = genericValue
                ? existing.AvroValueType == typeof(Avro.Generic.GenericRecord)
                : existing.AvroValueType != typeof(Avro.Generic.GenericRecord);

            if (keyMatches && valueMatches)
                return existing;

            _mappings.TryRemove(pocoType, out _);
        }

        string ns = AvroSanitizeName((overrideNamespace ?? pocoType.Namespace)?.ToLower() ?? string.Empty);

        var baseName = AvroSanitizeName((topicName ?? pocoType.Name).ToLower());

        var keyType = CreateType(ns, $"{baseName}-key", keyProperties);
        var valueType = CreateType(ns, $"{baseName}-value", valueProperties);

        // Stable Avro record names (prevent _1 suffix drift on re-registration)
        var keyAvroName = AvroSanitizeName($"{baseName}-key");
        var valueAvroName = AvroSanitizeName($"{baseName}-value");

        // Generate Avro types or schemas
        Type? avroKeyType = null;
        string? avroKeySchema = null;
        Avro.RecordSchema? avroKeyRecordSchema = null;
        if (genericKey && keyProperties.Length > 0)
        {
            var tmpKey = SpecificRecordGenerator.Generate(keyType, keyAvroName, ns);
            avroKeySchema = ((Avro.Specific.ISpecificRecord)Activator.CreateInstance(tmpKey)!).Schema.ToString();
            avroKeyType = typeof(Avro.Generic.GenericRecord);
            avroKeyRecordSchema = (Avro.RecordSchema)Avro.Schema.Parse(avroKeySchema);
        }
        else if (!genericKey && keyProperties.Length > 0)
        {
            avroKeyType = SpecificRecordGenerator.Generate(keyType, keyAvroName, ns);
            avroKeySchema = ((Avro.Specific.ISpecificRecord)Activator.CreateInstance(avroKeyType)!).Schema.ToString();
        }

        Type avroValueType;
        string avroValueSchema;
        Avro.RecordSchema? avroValueRecordSchema = null;
        if (genericValue)
        {
            var tmp = SpecificRecordGenerator.Generate(valueType, valueAvroName, ns);
            avroValueSchema = ((Avro.Specific.ISpecificRecord)Activator.CreateInstance(tmp)!).Schema.ToString();
            avroValueType = typeof(Avro.Generic.GenericRecord);
            avroValueRecordSchema = (Avro.RecordSchema)Avro.Schema.Parse(avroValueSchema);
        }
        else
        {
            avroValueType = SpecificRecordGenerator.Generate(valueType, valueAvroName, ns);
            avroValueSchema = ((Avro.Specific.ISpecificRecord)Activator.CreateInstance(avroValueType)!).Schema.ToString();
        }

        var keyTypeProps = keyProperties
            .Select(p => keyType.GetProperty(p.Name)!)
            .ToArray();
        var valueTypeProps = valueProperties
            .Select(p => valueType.GetProperty(p.Name)!)
            .ToArray();

        var mapping = new KeyValueTypeMapping
        {
            KeyType = keyType,
            KeyProperties = keyProperties,
            KeyTypeProperties = keyTypeProps,
            ValueType = valueType,
            ValueProperties = valueProperties,
            ValueTypeProperties = valueTypeProps,
            AvroKeyType = avroKeyType,
            AvroValueType = avroValueType,
            AvroKeySchema = avroKeySchema,
            AvroValueSchema = avroValueSchema,
            AvroKeyRecordSchema = avroKeyRecordSchema,
            AvroValueRecordSchema = avroValueRecordSchema
        };
        _mappings[pocoType] = mapping;
        _lastRegisteredType = pocoType;
        return mapping;
    }

    /// <summary>
    /// Register mapping using pre-generated PropertyMeta information.
    /// </summary>
    public KeyValueTypeMapping RegisterMeta(
        Type pocoType,
        (PropertyMeta[] KeyProperties, PropertyMeta[] ValueProperties) meta,
        string? topicName = null,
        bool genericKey = false,
        bool genericValue = false,
        string? overrideNamespace = null)
    {
        return Register(pocoType, meta.KeyProperties, meta.ValueProperties, topicName, genericKey, genericValue, overrideNamespace);
    }

    /// <summary>
    /// Register mapping using an EntityModel's property information.
    /// Convenience wrapper so callers don't need to manually convert
    /// PropertyInfo to <see cref="PropertyMeta"/> arrays.
    /// </summary>
    public KeyValueTypeMapping RegisterEntityModel(EntityModel model, bool genericKey = false, bool genericValue = false, string? overrideNamespace = null)
    {
        if (model == null) throw new ArgumentNullException(nameof(model));

        var keyMeta = model.KeyProperties
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();
        var valueMeta = model.AllProperties
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();

        return Register(model.EntityType, keyMeta, valueMeta, model.GetTopicName(), genericKey, genericValue, overrideNamespace);
    }

    /// <summary>
    /// Register mapping information based on a KsqlQueryModel.
    /// This extracts projection property order so the generated
    /// Avro schema matches the SELECT column ordering.
    /// </summary>
    public KeyValueTypeMapping RegisterQueryModel(
        Type resultType,
        KsqlQueryModel model,
        PropertyInfo[] keyProperties,
        string? topicName = null,
        bool genericKey = false,
        bool genericValue = false,
        string? overrideNamespace = null)
    {
        if (resultType == null) throw new ArgumentNullException(nameof(resultType));
        if (model == null) throw new ArgumentNullException(nameof(model));
        if (keyProperties == null) throw new ArgumentNullException(nameof(keyProperties));

        // For query-defined entities (int), exclude key properties from value schema
        var keyNames = new HashSet<string>(keyProperties.Select(k => k.Name), StringComparer.Ordinal);
        var projected = ExtractProjectionProperties(model.SelectProjection, resultType);
        if (projected.Count == 0)
        {
            projected = resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance).ToList();
        }
        var valueProps = projected
            .Where(p => !keyNames.Contains(p.Name))
            .Select(p => PropertyMeta.FromProperty(p))
            .ToArray();
        var keyMeta = keyProperties.Select(p => PropertyMeta.FromProperty(p)).ToArray();

        return Register(resultType, keyMeta, valueProps, topicName ?? resultType.GetKafkaTopicName(), genericKey, genericValue, overrideNamespace);
    }

    private static List<PropertyInfo> ExtractProjectionProperties(LambdaExpression? projection, Type resultType)
    {
        if (projection == null)
            return resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance).ToList();

        var props = new List<PropertyInfo>();
        switch (projection.Body)
        {
            case NewExpression newExpr when newExpr.Members != null:
                foreach (var mem in newExpr.Members.OfType<PropertyInfo>())
                {
                    var p = resultType.GetProperty(mem.Name);
                    if (p != null) props.Add(p);
                }
                break;
            case MemberInitExpression initExpr:
                foreach (var binding in initExpr.Bindings.OfType<MemberAssignment>())
                {
                    var p = resultType.GetProperty(binding.Member.Name);
                    if (p != null) props.Add(p);
                }
                break;
            case ParameterExpression:
                props.AddRange(resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance));
                break;
            case MemberExpression me when me.Member is PropertyInfo pi:
                var prop = resultType.GetProperty(pi.Name);
                if (prop != null) props.Add(prop);
                break;
        }
        return props;
    }

    public KeyValueTypeMapping GetMapping(Type pocoType)
    {
        if (_mappings.TryGetValue(pocoType, out var mapping))
        {
            return mapping;
        }
        throw new InvalidOperationException($"Mapping for {pocoType.FullName} is not registered.");
    }

    /// <summary>
    /// Returns the most recently registered POCO type, or null if none.
    /// </summary>
    public Type? GetLastRegistered()
    {
        return _lastRegisteredType;
    }

    private Type CreateType(string ns, string name, PropertyMeta[] properties)
    {
        var safeName = AvroSanitizeName(name);
        var prefix = string.IsNullOrWhiteSpace(ns) ? safeName : $"{ns}.{safeName}";
        var uniqueName = prefix;
        var suffix = 1;
        while (!_generatedTypeNames.TryAdd(uniqueName, 0))
        {
            uniqueName = $"{prefix}_{suffix++}";
        }
        var typeBuilder = _moduleBuilder.DefineType(uniqueName, TypeAttributes.Public | TypeAttributes.Class);
        foreach (var meta in properties)
        {
            var field = typeBuilder.DefineField($"_{meta.Name}", meta.PropertyType, FieldAttributes.Private);
            var property = typeBuilder.DefineProperty(meta.Name, PropertyAttributes.None, meta.PropertyType, null);
            var getMethod = typeBuilder.DefineMethod(
                $"get_{meta.Name}",
                MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName,
                meta.PropertyType,
                Type.EmptyTypes);
            var ilGet = getMethod.GetILGenerator();
            ilGet.Emit(OpCodes.Ldarg_0);
            ilGet.Emit(OpCodes.Ldfld, field);
            ilGet.Emit(OpCodes.Ret);
            var setMethod = typeBuilder.DefineMethod(
                $"set_{meta.Name}",
                MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName,
                null,
                new[] { meta.PropertyType });
            var ilSet = setMethod.GetILGenerator();
            ilSet.Emit(OpCodes.Ldarg_0);
            ilSet.Emit(OpCodes.Ldarg_1);
            ilSet.Emit(OpCodes.Stfld, field);
            ilSet.Emit(OpCodes.Ret);
            property.SetGetMethod(getMethod);
            property.SetSetMethod(setMethod);
            if (((meta.PropertyType == typeof(decimal)) || (Nullable.GetUnderlyingType(meta.PropertyType) == typeof(decimal)))
                && meta.Precision.HasValue && meta.Scale.HasValue)
            {
                var ctor = typeof(KsqlDecimalAttribute).GetConstructor(new[] { typeof(int), typeof(int) });
                var attr = new CustomAttributeBuilder(ctor!, new object[] { meta.Precision.Value, meta.Scale.Value });
                property.SetCustomAttribute(attr);
            }
        }
        return typeBuilder.CreateType()!;
    }
}
