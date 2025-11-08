using System;
using System.IO;
using Xunit;

namespace Ksql.Linq.Tests.Query.Golden;

internal static class GoldenSqlHelpers
{
    public static string Normalize(string s) => Ksql.Linq.Tests.Utils.SqlAssert.Normalize(s);

    public static void AssertEqualsOrUpdate(string goldenPath, string actualSql)
    {
        var normalized = Normalize(actualSql);
        // Resolve to repository-root absolute path to avoid writing under bin directory
        string ResolvePath(string p)
        {
            if (Path.IsPathRooted(p)) return p;
            var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../"));
            var full = Path.Combine(repoRoot, p.Replace('/', Path.DirectorySeparatorChar));
            return full;
        }
        var fullGoldenPath = ResolvePath(goldenPath);
        var update = Environment.GetEnvironmentVariable("UPDATE_GOLDEN");
        if (!File.Exists(fullGoldenPath))
        {
            if (string.Equals(update, "1", StringComparison.Ordinal))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(fullGoldenPath)!);
                File.WriteAllText(fullGoldenPath, normalized);
                return; // updated
            }
            throw new Xunit.Sdk.XunitException($"Golden file not found: {goldenPath}. Set UPDATE_GOLDEN=1 to create it.");
        }
        if (string.Equals(update, "1", StringComparison.Ordinal))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(fullGoldenPath)!);
            File.WriteAllText(fullGoldenPath, normalized);
        }
        var expected = File.ReadAllText(fullGoldenPath);
        Assert.Equal(expected.Trim(), normalized.Trim());
    }
}