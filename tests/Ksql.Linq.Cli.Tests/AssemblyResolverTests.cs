using System;
using System.IO;
using Ksql.Linq.Cli.Services;
using Xunit;

namespace Ksql.Linq.Cli.Tests;

public class AssemblyResolverTests
{
    [Fact]
    public void Resolve_DllPath_ReturnsFullPath()
    {
        var tempDll = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".dll");
        File.WriteAllBytes(tempDll, Array.Empty<byte>());

        try
        {
            var resolved = AssemblyResolver.Resolve(tempDll, verbose: false);
            Assert.Equal(Path.GetFullPath(tempDll), resolved);
        }
        finally
        {
            if (File.Exists(tempDll))
            {
                File.Delete(tempDll);
            }
        }
    }
}

