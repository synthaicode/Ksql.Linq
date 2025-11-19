using System;
using System.Collections.Generic;
using System.Linq;
using Ksql.Linq.Mapping;

namespace Ksql.Linq.Query.Script;

/// <summary>
/// Default implementation that inspects the MappingRegistry to obtain
/// Avro value schemas for entity models.
/// </summary>
public sealed class DefaultAvroSchemaExporter : IAvroSchemaExporter
{
    public IReadOnlyDictionary<string, string> ExportValueSchemas(KsqlContext context)
    {
        if (context == null) throw new ArgumentNullException(nameof(context));

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var models = context.GetEntityModels();
        var registry = context.GetMappingRegistry();
        foreach (var (clrType, model) in models.OrderBy(kvp => kvp.Key.Name))
        {
            // Skip internal entities like DLQ
            if (clrType.Namespace != null && clrType.Namespace.StartsWith("Ksql.Linq.Messaging", StringComparison.Ordinal))
                continue;

            // MappingRegistry already contains AvroValueRecordSchema once mapping is registered
            var mapping = registry.GetMapping(clrType);
            var valueSchema = mapping?.AvroValueSchema;
            if (valueSchema == null)
                continue;

            var key = clrType.FullName ?? clrType.Name;
            result[key] = valueSchema;
        }

        return result;
    }
}
