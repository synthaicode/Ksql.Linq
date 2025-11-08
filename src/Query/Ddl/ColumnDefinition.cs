namespace Ksql.Linq.Query.Ddl;

internal record ColumnDefinition(string Name, string Type, bool IsKey);