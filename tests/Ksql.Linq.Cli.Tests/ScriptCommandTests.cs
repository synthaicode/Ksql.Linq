using System.CommandLine;
using System.CommandLine.Invocation;
using Ksql.Linq.Cli.Commands;
using Xunit;

namespace Ksql.Linq.Cli.Tests;

public class ScriptCommandTests
{
    [Fact]
    public void Create_HasExpectedNameAndOptions()
    {
        var cmd = ScriptCommand.Create();

        Assert.Equal("script", cmd.Name);
        Assert.Contains(cmd.Options, o => o.HasAlias("--project"));
        Assert.Contains(cmd.Options, o => o.HasAlias("--output"));
        Assert.Contains(cmd.Options, o => o.HasAlias("--no-header"));
    }
}

