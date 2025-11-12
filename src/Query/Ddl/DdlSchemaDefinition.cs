using System.Collections.Generic;

namespace Ksql.Linq.Query.Ddl;

internal record DdlSchemaDefinition(
    string ObjectName,
    string TopicName,
    DdlObjectType ObjectType,
    int Partitions,
    short Replicas,
    string? KeySchemaFullName,
    string? ValueSchemaFullName,
    IReadOnlyList<ColumnDefinition> Columns,
    string? TimestampColumn = null);
