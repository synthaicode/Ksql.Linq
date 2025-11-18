using System.CommandLine;
using System.CommandLine.Invocation;
using Ksql.Linq.Cli.Services;

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
            // Resolve assembly path
            var assemblyPath = ResolveAssemblyPath(projectPath, verbose);
            if (assemblyPath == null)
            {
                Console.Error.WriteLine($"Error: Could not resolve assembly from '{projectPath}'");
                return 1;
            }

            if (verbose)
            {
                Console.WriteLine($"Loading assembly: {assemblyPath}");
            }

            // Build args for factory
            var factoryArgs = new List<string>();
            if (!string.IsNullOrEmpty(configPath))
            {
                factoryArgs.Add(configPath);
            }

            // Load context
            var loader = new DesignTimeContextLoader();
            var loadResult = loader.Load(assemblyPath, contextName, factoryArgs.ToArray());

            if (verbose)
            {
                Console.WriteLine($"Loaded context: {loadResult.ContextTypeName}");
                Console.WriteLine($"Assembly: {loadResult.AssemblyName} v{loadResult.AssemblyVersion}");
            }

            // Build script
            var builder = new ScriptBuilder();
            var options = new ScriptBuilder.ScriptOptions
            {
                IncludeHeader = !noHeader,
                AssemblyName = loadResult.AssemblyName,
                AssemblyVersion = loadResult.AssemblyVersion,
                ContextTypeName = loadResult.ContextTypeName
            };

            string script;
            using (loadResult.Context)
            {
                var entityCount = loadResult.Context.GetEntityModels().Count;
                if (verbose)
                {
                    Console.WriteLine($"Found {entityCount} entity model(s)");
                }

                script = builder.Build(loadResult.Context, options);
            }

            // Output script
            if (!string.IsNullOrEmpty(outputPath))
            {
                // Ensure directory exists
                var dir = Path.GetDirectoryName(outputPath);
                if (!string.IsNullOrEmpty(dir))
                {
                    Directory.CreateDirectory(dir);
                }

                await File.WriteAllTextAsync(outputPath, script);
                Console.WriteLine($"Script written to: {outputPath}");
            }
            else
            {
                // Write to stdout
                Console.WriteLine(script);
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

    private static string? ResolveAssemblyPath(string projectPath, bool verbose)
    {
        // If it's already a DLL, return as-is
        if (projectPath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
        {
            return Path.GetFullPath(projectPath);
        }

        // If it's a csproj, find the output assembly
        if (projectPath.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase))
        {
            var projectDir = Path.GetDirectoryName(Path.GetFullPath(projectPath));
            var projectName = Path.GetFileNameWithoutExtension(projectPath);

            // Check common output paths
            var searchPaths = new[]
            {
                Path.Combine(projectDir!, "bin", "Debug", "net8.0", $"{projectName}.dll"),
                Path.Combine(projectDir!, "bin", "Release", "net8.0", $"{projectName}.dll"),
                Path.Combine(projectDir!, "bin", "Debug", "net7.0", $"{projectName}.dll"),
                Path.Combine(projectDir!, "bin", "Release", "net7.0", $"{projectName}.dll"),
            };

            foreach (var path in searchPaths)
            {
                if (File.Exists(path))
                {
                    if (verbose)
                    {
                        Console.WriteLine($"Found assembly: {path}");
                    }
                    return path;
                }
            }

            // Suggest building
            Console.Error.WriteLine($"Assembly not found. Please build the project first: dotnet build {projectPath}");
            return null;
        }

        // Unknown file type
        Console.Error.WriteLine($"Unknown file type: {projectPath}. Expected .csproj or .dll");
        return null;
    }
}
