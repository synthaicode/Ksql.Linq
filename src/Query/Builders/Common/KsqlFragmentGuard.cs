using System;

namespace Ksql.Linq.Query.Builders.Common;

/// <summary>
/// Checks generated clause fragment integrity outside KSQL literals and quoted
/// identifiers. Raw comments and statement separators are unsupported in these
/// builder fragments, although they are valid in complete KSQL statements.
/// This is not a complete KSQL parser or a general security validator.
/// </summary>
internal static class KsqlFragmentGuard
{
    internal static string? FindRejectedPattern(string sql)
    {
        for (var index = 0; index < sql.Length;)
        {
            var current = sql[index];
            if (current == '\'' || current == '"' || current == '`')
            {
                var delimiter = current;
                var closed = false;
                index++;
                while (index < sql.Length)
                {
                    if (sql[index++] != delimiter)
                        continue;
                    if (index < sql.Length && sql[index] == delimiter)
                    {
                        index++;
                        continue;
                    }
                    closed = true;
                    break;
                }
                if (!closed)
                    return "unterminated quote";
                continue;
            }

            if (current == ';')
                return ";";
            if (index + 1 < sql.Length)
            {
                var next = sql[index + 1];
                if ((current == '-' && next == '-') ||
                    (current == '/' && next == '*') ||
                    (current == '*' && next == '/'))
                    return sql.Substring(index, 2);
            }

            if (IsIdentifierCharacter(current))
            {
                var start = index++;
                while (index < sql.Length && IsIdentifierCharacter(sql[index]))
                    index++;
                var token = sql.Substring(start, index - start);
                if (IsRejectedToken(token))
                    return token;
                continue;
            }
            index++;
        }
        return null;
    }

    private static bool IsIdentifierCharacter(char value) =>
        char.IsLetterOrDigit(value) || value == '_' || value == '@';

    private static bool IsRejectedToken(string token) =>
        token.Equals("DROP", StringComparison.OrdinalIgnoreCase) ||
        token.Equals("DELETE", StringComparison.OrdinalIgnoreCase) ||
        token.Equals("INSERT", StringComparison.OrdinalIgnoreCase) ||
        token.Equals("SCRIPT", StringComparison.OrdinalIgnoreCase);
}
