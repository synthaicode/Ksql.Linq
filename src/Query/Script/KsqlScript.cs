using System;
using System.Collections.Generic;

namespace Ksql.Linq.Query.Script;

/// <summary>
/// Represents a collection of KSQL statements produced from a KsqlContext.
/// </summary>
public sealed class KsqlScript
{
    public IReadOnlyList<string> Statements { get; }

    public KsqlScript(IReadOnlyList<string> statements)
    {
        Statements = statements ?? throw new ArgumentNullException(nameof(statements));
    }

    public override string ToString() => ToSql();

    /// <summary>
    /// Concatenates all statements into a single KSQL script string.
    /// </summary>
    public string ToSql()
    {
        return string.Join(Environment.NewLine + Environment.NewLine, Statements);
    }
}

