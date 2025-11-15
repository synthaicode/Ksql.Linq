namespace Ksql.Linq.Query.Builders.Schema;

internal record KsqlColumn(string Name, string KsqlType, bool IsKey = false);
