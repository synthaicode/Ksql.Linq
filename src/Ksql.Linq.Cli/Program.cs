using System.CommandLine;
using Ksql.Linq.Cli.Commands;

namespace Ksql.Linq.Cli;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var rootCommand = new RootCommand("Ksql.Linq design-time tools for KSQL script and Avro schema generation");

        rootCommand.AddCommand(ScriptCommand.Create());
        rootCommand.AddCommand(AvroCommand.Create());

        return await rootCommand.InvokeAsync(args);
    }
}

