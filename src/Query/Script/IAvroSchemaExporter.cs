using System.Collections.Generic;

namespace Ksql.Linq.Query.Script;

/// <summary>
/// Exports Avro schemas for entity value types from a configured KsqlContext.
/// Intended for design-time inspection (e.g., generating .avsc files).
/// </summary>
public interface IAvroSchemaExporter
{
    IReadOnlyDictionary<string, string> ExportValueSchemas(KsqlContext context);
}
