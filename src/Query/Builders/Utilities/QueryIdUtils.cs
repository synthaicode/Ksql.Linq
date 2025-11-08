using System;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Ksql.Linq.Query.Builders.Utilities;

internal static class QueryIdUtils
{
    public static string? ExtractQueryId(KsqlDbResponse response)
    {
        if (response?.Message == null)
            return null;

        var message = response.Message;
        try
        {
            using var doc = JsonDocument.Parse(message);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in doc.RootElement.EnumerateArray())
                {
                    if (TryReadQueryId(element, out var id))
                        return id;
                }
            }
            else if (doc.RootElement.ValueKind == JsonValueKind.Object)
            {
                if (TryReadQueryId(doc.RootElement, out var id))
                    return id;
            }
        }
        catch
        {
            // fall back to regex parsing
        }

        var regex = new Regex(@"(CTAS|CSAS)_[A-Za-z0-9_\-]+", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        var match = regex.Match(message);
        if (match.Success)
            return match.Value;

        return null;
    }

    private static bool TryReadQueryId(JsonElement element, out string? queryId)
    {
        queryId = null;
        if (element.ValueKind != JsonValueKind.Object)
            return false;

        if (element.TryGetProperty("commandStatus", out var status) && status.ValueKind == JsonValueKind.Object)
        {
            if (status.TryGetProperty("queryId", out var statusQueryId) && statusQueryId.ValueKind == JsonValueKind.String)
            {
                queryId = statusQueryId.GetString();
                return !string.IsNullOrWhiteSpace(queryId);
            }
        }

        if (element.TryGetProperty("queryId", out var queryIdElement) && queryIdElement.ValueKind == JsonValueKind.String)
        {
            queryId = queryIdElement.GetString();
            return !string.IsNullOrWhiteSpace(queryId);
        }

        if (element.TryGetProperty("commandId", out var commandId) && commandId.ValueKind == JsonValueKind.String)
        {
            var commandValue = commandId.GetString();
            if (!string.IsNullOrWhiteSpace(commandValue) && commandValue.StartsWith("CT", StringComparison.OrdinalIgnoreCase))
            {
                queryId = commandValue;
                return true;
            }
        }

        return false;
    }
}
