using System;
using System.IO;
using Ksql.Linq.Cli.Services;
using Xunit;

namespace Ksql.Linq.Cli.Tests;

public class DesignTimeContextLoaderTests
{
    [Fact]
    public void Load_Throws_WhenAssemblyDoesNotExist()
    {
        var loader = new DesignTimeContextLoader();
        var missingPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".dll");

        var ex = Assert.Throws<FileNotFoundException>(() =>
            loader.Load(missingPath, contextTypeName: null, args: Array.Empty<string>()));

        Assert.Contains("Assembly not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
