namespace Ksql.Linq.Mapping;

using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// Joins representable key components without changing the existing NUL-delimited format.
/// </summary>
internal static class CompositeKeyEncoding
{
    public static string Join(IEnumerable<string?> components)
    {
        var parts = components.ToArray();
        foreach (var part in parts)
        {
            if (part != null && part.IndexOf(KeyValueTypeMapping.KeySep) >= 0)
                throw new ArgumentException("Key components must not contain NUL characters.", nameof(components));
        }
        return string.Join(KeyValueTypeMapping.KeySep, parts);
    }
}
