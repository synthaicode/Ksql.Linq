using System.Reflection;
using System.Text;
using System.Text.Json;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Configuration;

namespace Ksql.Linq.Cli.Services;

/// <summary>
/// Generates Avro schema files (.avsc) from entity models.
/// </summary>
public class AvroSchemaGenerator
{
    /// <summary>
    /// Generates Avro schemas for all entities in the context.
    /// </summary>
    /// <returns>Dictionary of topic name to schema JSON content.</returns>
    public Dictionary<string, string> Generate(KsqlContext context)
    {
        var result = new Dictionary<string, string>();
        var models = context.GetEntityModels();

        foreach (var model in models.Values)
        {
            var topicName = model.GetTopicName();

            // Generate value schema
            var valueSchema = GenerateSchema(
                model.EntityType,
                model.ValueSchemaFullName,
                isKey: false);
            result[$"{topicName}-value"] = valueSchema;

            // Generate key schema if there are key properties
            if (model.KeyProperties.Length > 0)
            {
                var keySchema = GenerateKeySchema(model);
                if (!string.IsNullOrEmpty(keySchema))
                {
                    result[$"{topicName}-key"] = keySchema;
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Generates a single Avro schema JSON for a type.
    /// </summary>
    public string GenerateSchema(Type entityType, string? schemaFullName = null, bool isKey = false)
    {
        var schemaName = entityType.Name;
        var schemaNamespace = entityType.Namespace ?? string.Empty;

        // Parse schema full name if provided
        if (!string.IsNullOrWhiteSpace(schemaFullName))
        {
            var lastDot = schemaFullName.LastIndexOf('.');
            if (lastDot > 0)
            {
                schemaNamespace = schemaFullName.Substring(0, lastDot);
                schemaName = schemaFullName.Substring(lastDot + 1);
            }
            else
            {
                schemaName = schemaFullName;
            }
        }

        var props = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        var fields = new List<object>();
        foreach (var prop in props)
        {
            // Skip ignored properties
            if (prop.GetCustomAttribute<KsqlIgnoreAttribute>() != null)
                continue;

            var decAttr = prop.GetCustomAttribute<KsqlDecimalAttribute>();
            var field = CreateFieldDefinition(prop, decAttr);
            fields.Add(field);
        }

        var schema = new Dictionary<string, object>
        {
            ["type"] = "record",
            ["name"] = schemaName,
            ["namespace"] = schemaNamespace,
            ["fields"] = fields
        };

        return JsonSerializer.Serialize(schema, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = null
        });
    }

    private string GenerateKeySchema(EntityModel model)
    {
        var keyProps = model.KeyProperties;
        if (keyProps.Length == 0)
            return string.Empty;

        // For single primitive key, return simple schema
        if (keyProps.Length == 1)
        {
            var keyProp = keyProps[0];
            var keyType = keyProp.PropertyType;

            // Simple types don't need a record wrapper
            if (keyType == typeof(string) || keyType == typeof(int) || keyType == typeof(long))
            {
                var simpleType = MapToAvroType(keyType, null, keyProp);
                return simpleType;
            }
        }

        // For composite keys, create a record
        var keySchemaName = model.KeySchemaFullName;
        if (string.IsNullOrWhiteSpace(keySchemaName))
        {
            keySchemaName = $"{model.EntityType.Namespace}.{model.EntityType.Name}Key";
        }

        var lastDot = keySchemaName.LastIndexOf('.');
        var schemaNamespace = lastDot > 0 ? keySchemaName.Substring(0, lastDot) : "";
        var schemaName = lastDot > 0 ? keySchemaName.Substring(lastDot + 1) : keySchemaName;

        var fields = new List<object>();
        foreach (var prop in keyProps)
        {
            var decAttr = prop.GetCustomAttribute<KsqlDecimalAttribute>();
            var field = CreateFieldDefinition(prop, decAttr);
            fields.Add(field);
        }

        var schema = new Dictionary<string, object>
        {
            ["type"] = "record",
            ["name"] = schemaName,
            ["namespace"] = schemaNamespace,
            ["fields"] = fields
        };

        return JsonSerializer.Serialize(schema, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = null
        });
    }

    private object CreateFieldDefinition(PropertyInfo prop, KsqlDecimalAttribute? decAttr)
    {
        var avroType = MapToAvroTypeObject(prop.PropertyType, decAttr, prop);
        var field = new Dictionary<string, object>
        {
            ["name"] = prop.Name,
            ["type"] = avroType
        };

        // Add default values
        var defaultValue = GetDefaultValue(prop.PropertyType);
        if (defaultValue != null)
        {
            field["default"] = defaultValue;
        }

        return field;
    }

    private object MapToAvroTypeObject(Type t, KsqlDecimalAttribute? decAttr, PropertyInfo? prop)
    {
        // Handle Nullable<T> as union ["null", <T>]
        var underlying = Nullable.GetUnderlyingType(t);
        if (underlying != null)
        {
            var inner = MapToAvroTypeObject(underlying, decAttr, prop);
            return new object[] { "null", inner };
        }

        // Handle Dictionary<string, string>
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var args = t.GetGenericArguments();
            if (args[0] == typeof(string) && args[1] == typeof(string))
            {
                return new Dictionary<string, object>
                {
                    ["type"] = "map",
                    ["values"] = "string"
                };
            }
            throw new NotSupportedException("Only Dictionary<string,string> is supported.");
        }

        // Handle arrays/lists
        if (t.IsArray && t.GetElementType() != typeof(byte))
        {
            return new Dictionary<string, object>
            {
                ["type"] = "array",
                ["items"] = MapToAvroTypeObject(t.GetElementType()!, null, null)
            };
        }

        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>))
        {
            return new Dictionary<string, object>
            {
                ["type"] = "array",
                ["items"] = MapToAvroTypeObject(t.GetGenericArguments()[0], null, null)
            };
        }

        // Primitive types
        if (t == typeof(int)) return "int";
        if (t == typeof(long)) return "long";
        if (t == typeof(float)) return "double"; // Align with ksqlDB AVRO
        if (t == typeof(double)) return "double";
        if (t == typeof(bool)) return "boolean";
        if (t == typeof(string)) return new object[] { "null", "string" };
        if (t == typeof(byte[])) return "bytes";

        if (t == typeof(decimal))
        {
            var precision = decAttr?.Precision ?? DecimalPrecisionConfig.ResolvePrecision(null, prop);
            var scale = decAttr?.Scale ?? DecimalPrecisionConfig.ResolveScale(null, prop);
            return new Dictionary<string, object>
            {
                ["type"] = "bytes",
                ["logicalType"] = "decimal",
                ["precision"] = precision,
                ["scale"] = scale
            };
        }

        if (t == typeof(DateTime))
        {
            return new Dictionary<string, object>
            {
                ["type"] = "long",
                ["logicalType"] = "timestamp-millis"
            };
        }

        if (t == typeof(DateTimeOffset))
        {
            return new Dictionary<string, object>
            {
                ["type"] = "long",
                ["logicalType"] = "timestamp-millis"
            };
        }

        if (t == typeof(TimeSpan))
        {
            return new Dictionary<string, object>
            {
                ["type"] = "long",
                ["logicalType"] = "time-millis"
            };
        }

        if (t == typeof(Guid)) return "string";

        // Default to string
        return "string";
    }

    private string MapToAvroType(Type t, KsqlDecimalAttribute? decAttr, PropertyInfo? prop)
    {
        var obj = MapToAvroTypeObject(t, decAttr, prop);
        return JsonSerializer.Serialize(obj);
    }

    private object? GetDefaultValue(Type t)
    {
        if (t == typeof(string)) return null;
        if (t == typeof(int) || t == typeof(long) || t == typeof(float) || t == typeof(double)) return 0;
        if (t == typeof(bool)) return false;
        if (t == typeof(byte[])) return null;
        if (t == typeof(DateTime)) return 0;
        if (t == typeof(Guid)) return "00000000-0000-0000-0000-000000000000";
        if (Nullable.GetUnderlyingType(t) != null) return null;

        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            return new Dictionary<string, object>();
        }

        return null;
    }
}
