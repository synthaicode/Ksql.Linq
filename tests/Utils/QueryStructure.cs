using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Ksql.Linq.Tests.Utils;

public class QueryStructure
{
    public string CreateType { get; set; } = string.Empty; // STREAM/TABLE
    public string TargetName { get; set; } = string.Empty;
    public Dictionary<string, string> WithParts { get; } = new(StringComparer.OrdinalIgnoreCase);
    public List<(string Expression, string Alias)> Projections { get; } = new();
    public (string Name, string Alias) From { get; set; }
    public string WindowRaw { get; set; } = string.Empty;
    public List<string> GroupByColumns { get; } = new();
    public bool HasEmitChanges { get; set; }

    public bool TryGetProjection(string alias, out string expression)
    {
        var p = Projections.FirstOrDefault(x => string.Equals(x.Alias, alias, StringComparison.OrdinalIgnoreCase));
        if (!string.IsNullOrWhiteSpace(p.Alias))
        {
            expression = p.Expression;
            return true;
        }
        expression = string.Empty;
        return false;
    }

    public static QueryStructure Parse(string sql)
    {
        var s = sql ?? string.Empty;
        var qs = new QueryStructure();

        // CREATE line
        var createMatch = Regex.Match(s, @"\bCREATE\s+(STREAM|TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][\w]*)", RegexOptions.IgnoreCase);
        if (createMatch.Success)
        {
            qs.CreateType = createMatch.Groups[1].Value.ToUpperInvariant();
            qs.TargetName = createMatch.Groups[2].Value;
        }

        // WITH parts
        var withMatch = Regex.Match(s, @"WITH\s*\((?<with>[^\)]*)\)", RegexOptions.IgnoreCase);
        if (withMatch.Success)
        {
            var body = withMatch.Groups["with"].Value;
            foreach (var part in body.Split(','))
            {
                var kv = part.Split('=', 2);
                if (kv.Length == 2)
                {
                    qs.WithParts[kv[0].Trim()] = kv[1].Trim().Trim('\'', '"');
                }
            }
        }

        // SELECT columns: between SELECT and the first top-level FROM
        var selectIndex = s.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
        if (selectIndex >= 0)
        {
            var start = selectIndex + "SELECT".Length;
            var depth = 0;
            var inQuote = false;
            var fromIndex = -1;
            for (int i = start; i < s.Length - 3; i++)
            {
                var ch = s[i];
                if (ch == '\'')
                {
                    inQuote = !inQuote;
                    continue;
                }
                if (inQuote)
                    continue;

                if (ch == '(')
                    depth++;
                else if (ch == ')' && depth > 0)
                    depth--;

                if (depth == 0 &&
                    (ch == 'F' || ch == 'f') &&
                    char.IsWhiteSpace(i > 0 ? s[i - 1] : ' ') &&
                    string.Compare(s, i, "FROM", 0, 4, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    fromIndex = i;
                    break;
                }
            }

            if (fromIndex > start)
            {
                var sel = s[start..fromIndex].Trim();
            foreach (var col in SplitTopLevel(sel))
            {
                var trimmed = col.Trim();
                if (TrySplitProjection(trimmed, out var expr, out var alias))
                {
                    qs.Projections.Add((expr, alias));
                }
                else
                {
                    // no alias; use expression as alias fallback
                    qs.Projections.Add((trimmed, trimmed));
                }
            }
            }
        }

        // FROM and alias
        var fromMatch = Regex.Match(s, @"\bFROM\s+([A-Za-z_][\w]*)\s+([A-Za-z_][\w]*)", RegexOptions.IgnoreCase);
        if (fromMatch.Success)
        {
            qs.From = (fromMatch.Groups[1].Value, fromMatch.Groups[2].Value);
        }

        // WINDOW clause (raw)
        var winMatch = Regex.Match(s, @"\bWINDOW\s+([A-Z]+)\s*\(([^\)]*)\)", RegexOptions.IgnoreCase);
        if (winMatch.Success)
        {
            qs.WindowRaw = winMatch.Value.ToUpperInvariant();
        }

        // GROUP BY columns (allow WINDOW clause between GROUP and BY)
        var gbMatch = Regex.Match(
            s,
            @"\bGROUP\s+(?:WINDOW\s+[^\)]*\)\s+)?BY\s+(?<gb>.+?)(\bHAVING\b|\bEMIT\b|;|$)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (gbMatch.Success)
        {
            var gb = gbMatch.Groups["gb"].Value;
            foreach (var c in SplitTopLevel(gb))
            {
                qs.GroupByColumns.Add(c.Trim().Trim('`').ToUpperInvariant());
            }
        }

        qs.HasEmitChanges = s.IndexOf("EMIT CHANGES", StringComparison.OrdinalIgnoreCase) >= 0;
        return qs;
    }

    private static IEnumerable<string> SplitTopLevel(string list)
    {
        var items = new List<string>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < list.Length; i++)
        {
            var ch = list[i];
            if (ch == '(') depth++;
            else if (ch == ')') depth = Math.Max(0, depth - 1);
            else if (ch == ',' && depth == 0)
            {
                items.Add(list.Substring(start, i - start));
                start = i + 1;
            }
        }
        if (start < list.Length)
            items.Add(list[start..]);
        return items;
    }

    private static bool TrySplitProjection(string column, out string expression, out string alias)
    {
        expression = string.Empty;
        alias = string.Empty;
        int depth = 0;
        bool inQuote = false;
        int aliasIndex = -1;

        for (int i = 0; i < column.Length; i++)
        {
            var ch = column[i];
            if (ch == '\'')
            {
                inQuote = !inQuote;
                continue;
            }
            if (inQuote)
                continue;

            if (ch == '(')
            {
                depth++;
                continue;
            }
            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 &&
                (ch == 'A' || ch == 'a') &&
                i + 1 < column.Length &&
                (column[i + 1] == 'S' || column[i + 1] == 's'))
            {
                int prev = i - 1;
                int next = i + 2;
                var prevChar = prev >= 0 ? column[prev] : ' ';
                if ((prev < 0 || char.IsWhiteSpace(prevChar) || prevChar == ')' || prevChar == ']') &&
                    (next >= column.Length || char.IsWhiteSpace(column[next])))
                {
                    aliasIndex = i;
                }
            }
        }

        if (aliasIndex < 0)
            return false;

        int exprEnd = aliasIndex - 1;
        while (exprEnd >= 0 && char.IsWhiteSpace(column[exprEnd]))
            exprEnd--;
        if (exprEnd < 0)
            return false;

        int aliasStart = aliasIndex + 2;
        while (aliasStart < column.Length && char.IsWhiteSpace(column[aliasStart]))
            aliasStart++;
        if (aliasStart >= column.Length)
            return false;

        int aliasEnd = aliasStart;
        while (aliasEnd < column.Length)
        {
            var ch = column[aliasEnd];
            if (char.IsWhiteSpace(ch) || ch == ',' || ch == ')')
                break;
            aliasEnd++;
        }

        expression = column[..(exprEnd + 1)].Trim();
        alias = column[aliasStart..aliasEnd].Trim();
        return true;
    }
}
