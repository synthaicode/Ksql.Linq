using Ksql.Linq.Query.Script;

namespace Ksql.Linq;

/// <summary>
/// Builds KSQL scripts from a configured KsqlContext instance.
/// Intended for design-time use (e.g., offline DDL inspection).
/// </summary>
public interface IKsqlScriptBuilder
{
    KsqlScript Build(KsqlContext context);
}

