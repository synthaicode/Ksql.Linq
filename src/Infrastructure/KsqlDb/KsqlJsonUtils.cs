using System;
using System.Collections.Generic;
using System.Text.Json;

namespace Ksql.Linq.Infrastructure.KsqlDb;

internal static class KsqlJsonUtils
{
    public static int CountRowsInArrayBody(string body)
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            var cnt = 0;
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out _)) cnt++;
                }
            }
            return cnt;
        }
        catch
        {
            var idx = 0; var count = 0;
            while ((idx = body.IndexOf("\"row\"", idx, StringComparison.OrdinalIgnoreCase)) >= 0)
            { count++; idx += 5; }
            return count;
        }
    }

    public static bool TryGetRowColumns(JsonElement element, out object?[]? columns)
    {
        columns = null;
        if (element.ValueKind != JsonValueKind.Object) return false;
        if (!element.TryGetProperty("row", out var rowEl)) return false;
        if (!rowEl.TryGetProperty("columns", out var cols) || cols.ValueKind != JsonValueKind.Array) return false;
        columns = ParseColumns(cols);
        return true;
    }

    public static bool TryParseRowLine(string line, out object?[]? columns)
    {
        columns = null;
        if (string.IsNullOrWhiteSpace(line)) return false;
        try
        {
            using var doc = JsonDocument.Parse(line);
            return TryGetRowColumns(doc.RootElement, out columns);
        }
        catch { return false; }
    }

    public static List<object?[]> ParseRowsFromBody(string body)
    {
        var rows = new List<object?[]>();
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return rows;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (TryGetRowColumns(el, out var cols) && cols is not null) rows.Add(cols);
            }
        }
        catch { }
        return rows;
    }

    public static object?[] ParseColumns(JsonElement cols)
    {
        var list = new object?[cols.GetArrayLength()];
        var i = 0;
        foreach (var c in cols.EnumerateArray())
        {
            list[i++] = c.ValueKind switch
            {
                JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                JsonValueKind.String => c.GetString(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => c.ToString()
            };
        }
        return list;
    }

    public static HashSet<string> ExtractLowercasedFields(string body, string collectionProperty, params string[] fields)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return set;
            foreach (var item in doc.RootElement.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object) continue;
                if (!item.TryGetProperty(collectionProperty, out var arr) || arr.ValueKind != JsonValueKind.Array) continue;
                foreach (var element in arr.EnumerateArray())
                {
                    foreach (var f in fields)
                    {
                        if (element.TryGetProperty(f, out var val) && val.ValueKind == JsonValueKind.String)
                        {
                            var s = val.GetString();
                            if (!string.IsNullOrEmpty(s)) set.Add(s.ToLowerInvariant());
                        }
                    }
                }
            }
        }
        catch { }
        return set;
    }
}

