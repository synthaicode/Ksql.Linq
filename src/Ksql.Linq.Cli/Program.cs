using System.CommandLine;
using Ksql.Linq.Cli.Commands;

namespace Ksql.Linq.Cli;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        var rootCommand = new RootCommand("""
            Ksql.Linq design-time tools and AI assistant guide commands.

            Quick start (AI Assist):
              dotnet ksql ai-assist --copy

            Then ask your AI assistant:
              "Read this guide and help me design and sanity-check my KsqlContext, entities, and windowing strategy."

            Tip (GitHub Copilot / agent mode):
              Paste the output into Copilot Chat and ask it to follow the guide when reviewing your KsqlContext or generated KSQL.
            """);

        rootCommand.AddCommand(ScriptCommand.Create());
        rootCommand.AddCommand(AvroCommand.Create());
        rootCommand.AddCommand(AiAssistCommand.Create());

        return await rootCommand.InvokeAsync(args);
    }
}
