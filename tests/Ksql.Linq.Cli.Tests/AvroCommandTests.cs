using Ksql.Linq.Cli.Commands;
using Xunit;

namespace Ksql.Linq.Cli.Tests;

public class AvroCommandTests
{
    [Fact]
    public void Create_HasExpectedNameAndOptions()
    {
        var cmd = AvroCommand.Create();

        Assert.Equal("avro", cmd.Name);
        Assert.Contains(cmd.Options, o => o.HasAlias("--project"));
        Assert.Contains(cmd.Options, o => o.HasAlias("--output"));
    }
}

