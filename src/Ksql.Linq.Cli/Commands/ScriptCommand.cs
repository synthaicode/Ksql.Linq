using System.CommandLine;
using System.CommandLine.Invocation;
using Ksql.Linq.Cli.Services;
using Ksql.Linq.Query.Script;

namespace Ksql.Linq.Cli.Commands;

/// <summary>
/// Command to generate KSQL scripts from a compiled assembly.
/// </summary>
public static class ScriptCommand
{
    public static Command Create()
    {
        var command = new Command("script", "Generate KSQL script from compiled assembly");

        // Options
        var projectOption = new Option<string>(
            aliases: new[] { "--project", "-p" },
            description: "Path to the project file (.csproj) or assembly (.dll)")
        {
            IsRequired = true
        };

        var contextOption = new Option<string?>(
            aliases: new[] { "--context", "-c" },
            description: "Name of the KsqlContext class (required if multiple factories exist)");

        var outputOption = new Option<string?>(
            aliases: new[] { "--output", "-o" },
            description: "Output file path for the generated KSQL script");

        var configOption = new Option<string?>(
            aliases: new[] { "--config" },
            description: "Path to appsettings.json (passed to factory)");

        var noHeaderOption = new Option<bool>(
            aliases: new[] { "--no-header" },
            description: "Exclude header comment from output");

        var verboseOption = new Option<bool>(
            aliases: new[] { "--verbose", "-v" },
            description: "Show detailed output");

        command.AddOption(projectOption);
        command.AddOption(contextOption);
        command.AddOption(outputOption);
        command.AddOption(configOption);
        command.AddOption(noHeaderOption);
        command.AddOption(verboseOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var project = context.ParseResult.GetValueForOption(projectOption)!;
            var contextName = context.ParseResult.GetValueForOption(contextOption);
            var output = context.ParseResult.GetValueForOption(outputOption);
            var config = context.ParseResult.GetValueForOption(configOption);
            var noHeader = context.ParseResult.GetValueForOption(noHeaderOption);
            var verbose = context.ParseResult.GetValueForOption(verboseOption);

            var exitCode = await Execute(project, contextName, output, config, noHeader, verbose);
            context.ExitCode = exitCode;
        });

        return command;
    }

    private static async Task<int> Execute(
        string projectPath,
        string? contextName,
        string? outputPath,
        string? configPath,
        bool noHeader,
        bool verbose)
    {
        try
        {
            var assemblyPath = AssemblyResolver.Resolve(projectPath, verbose);
            if (assemblyPath == null)
            {
                Console.Error.WriteLine($"Error: Could not resolve assembly from '{projectPath}'");
                return 1;
            }

            if (verbose)
            {
                Console.WriteLine($"Loading assembly: {assemblyPath}");
            }

            var factoryArgs = new List<string>();
            if (!string.IsNullOrEmpty(configPath))
            {
                factoryArgs.Add(configPath);
            }

            var loader = new DesignTimeContextLoader();
            var loadResult = loader.Load(assemblyPath, contextName, factoryArgs.ToArray());

            if (verbose)
            {
                Console.WriteLine($"Loaded context: {loadResult.ContextTypeName}");
                Console.WriteLine($"Assembly: {loadResult.AssemblyName} v{loadResult.AssemblyVersion}");
            }

            string scriptText;
            using (loadResult.Context)
            {
                var entityCount = loadResult.Context.GetEntityModels().Count;
                if (verbose)
                {
                    Console.WriteLine($"Found {entityCount} entity model(s)");
                }

                var builder = new DefaultKsqlScriptBuilder();
                var script = builder.Build(loadResult.Context);
                scriptText = script.ToSql();

                if (noHeader)
                {
                    // ヘッダー行を削除したい場合は、先頭のコメントブロックを取り除く簡易処理を行う
                    var lines = scriptText.Split(Environment.NewLine);
                    var trimmed = lines.SkipWhile(l => l.StartsWith("--")).SkipWhile(string.IsNullOrWhiteSpace);
                    scriptText = string.Join(Environment.NewLine, trimmed);
                }
            }

            if (!string.IsNullOrEmpty(outputPath))
            {
                var dir = Path.GetDirectoryName(outputPath);
                if (!string.IsNullOrEmpty(dir))
                {
                    Directory.CreateDirectory(dir);
                }

                await File.WriteAllTextAsync(outputPath, scriptText);
                Console.WriteLine($"Script written to: {outputPath}");
            }
            else
            {
                Console.WriteLine(scriptText);
            }

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            if (verbose)
            {
                Console.Error.WriteLine(ex.StackTrace);
            }
            return 1;
        }
    }
}

