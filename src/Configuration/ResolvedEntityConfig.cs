using System.Collections.Generic;

namespace Ksql.Linq.Configuration;

/// <summary>
/// Final merged configuration for an entity after combining defaults,
/// DSL/POCO metadata and external configuration.
/// </summary>
public class ResolvedEntityConfig
{
    public string Entity { get; set; } = string.Empty;
    public string SourceTopic { get; set; } = string.Empty;
    public string? GroupId { get; set; }
    public bool EnableCache { get; set; }
    public string? StoreName { get; set; }
    /// <summary>
    /// Derived value schema subject for this entity. This is not user-configurable;
    /// it is computed from the effective topic name using SchemaSubjects.ValueFor(...).
    /// Design-time tooling can use this to correlate KSQL statements with Schema Registry.
    /// </summary>
    public string? ValueSchemaSubject { get; set; }
    /// <summary>
    /// Container for additional configuration values that may be introduced
    /// in future extensions. Keys follow the same naming as configuration
    /// properties.
    /// </summary>
    public Dictionary<string, object> AdditionalSettings { get; } = new();
}
