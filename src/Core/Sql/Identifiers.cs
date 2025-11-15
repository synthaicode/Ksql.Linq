namespace Ksql.Linq.Core.Sql;

internal static class Identifiers
{
    public static string Normalize(string? name)
        => string.IsNullOrWhiteSpace(name) ? string.Empty : name.Trim().ToUpperInvariant();
}

