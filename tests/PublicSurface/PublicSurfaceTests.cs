using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Ksql.Linq.Tests.PublicSurface;

public class PublicSurfaceTests
{
    private static string ResolveTestFile(string relative)
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.Combine(baseDir, relative);
        if (File.Exists(path)) return path;
        // Fallback to project root resolution
        var dir = new DirectoryInfo(baseDir);
        for (int i = 0; i < 6 && dir != null; i++, dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate)) return candidate;
        }
        throw new FileNotFoundException($"Cannot find file: {relative}");
    }

    [Fact]
    public void ExportedTypes_match_explicit_whitelist()
    {
        var asm = typeof(KsqlContext).Assembly;
        var exported = asm.GetExportedTypes().Select(t => t.FullName!).OrderBy(n => n).ToList();

        var allowed = File.ReadAllLines(ResolveTestFile("PublicSurface/AllowedTypes.txt"))
            .Select(l => l.Trim())
            .Where(l => !string.IsNullOrWhiteSpace(l) && !l.StartsWith("#") && !l.StartsWith("//"))
            .ToHashSet(StringComparer.Ordinal);

        var missing = allowed.Where(a => !exported.Contains(a)).ToList();
        Assert.True(missing.Count == 0, "Missing expected public types:\n" + string.Join("\n", missing));
    }

    // No wildcard helpers needed for explicit list
}
