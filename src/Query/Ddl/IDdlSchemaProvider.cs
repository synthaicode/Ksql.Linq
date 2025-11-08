namespace Ksql.Linq.Query.Ddl;

internal interface IDdlSchemaProvider
{
    DdlSchemaDefinition GetSchema();
}
