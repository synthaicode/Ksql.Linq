using System.CommandLine;
using Ksql.Linq.Cli.Commands;

namespace Ksql.Linq.Cli;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var rootCommand = new RootCommand("Ksql.Linq design-time tools and AI assistant guide commands");

        rootCommand.AddCommand(ScriptCommand.Create());
        rootCommand.AddCommand(AvroCommand.Create());
        rootCommand.AddCommand(AiAssistCommand.Create());

        return await rootCommand.InvokeAsync(args);
    }
}

