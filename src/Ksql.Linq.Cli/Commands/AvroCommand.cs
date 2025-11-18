using System.CommandLine;
using System.CommandLine.Invocation;
using Ksql.Linq.Cli.Services;

namespace Ksql.Linq.Cli.Commands;

/// <summary>
/// Command to generate Avro schema files (.avsc) from a compiled assembly.
/// </summary>
public static class AvroCommand
{
    public static Command Create()
    {
        var command = new Command("avro", "Generate Avro schema files (.avsc) from compiled assembly");

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

        var outputOption = new Option<string>(
            aliases: new[] { "--output", "-o" },
            description: "Output directory for Avro schema files")
        {
            IsRequired = true
        };

        var configOption = new Option<string?>(
            aliases: new[] { "--config" },
            description: "Path to appsettings.json (passed to factory)");

        var verboseOption = new Option<bool>(
            aliases: new[] { "--verbose", "-v" },
            description: "Show detailed output");

        command.AddOption(projectOption);
        command.AddOption(contextOption);
        command.AddOption(outputOption);
        command.AddOption(configOption);
        command.AddOption(verboseOption);

        command.SetHandler(async (InvocationContext context) =>
        {
            var project = context.ParseResult.GetValueForOption(projectOption)!;
            var contextName = context.ParseResult.GetValueForOption(contextOption);
            var output = context.ParseResult.GetValueForOption(outputOption)!;
            var config = context.ParseResult.GetValueForOption(configOption);
            var verbose = context.ParseResult.GetValueForOption(verboseOption);

            var exitCode = await Execute(project, contextName, output, config, verbose);
            context.ExitCode = exitCode;
        });

        return command;
    }

    private static async Task<int> Execute(
        string projectPath,
        string? contextName,
        string outputDir,
        string? configPath,
        bool verbose)
    {
        try
        {
            // Resolve assembly path
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

            // Generate schemas
            Dictionary<string, string> schemas;
            using (loadResult.Context)
            {
                var entityCount = loadResult.Context.GetEntityModels().Count;
                if (verbose)
                {
                    Console.WriteLine($"Found {entityCount} entity model(s)");
                }

                var schemaGenerator = new AvroSchemaGenerator();
                schemas = schemaGenerator.Generate(loadResult.Context);

                if (verbose)
                {
                    Console.WriteLine($"Generated {schemas.Count} Avro schema(s)");
                }
            }

            // Output schemas
            Directory.CreateDirectory(outputDir);

            foreach (var (name, schemaJson) in schemas)
            {
                var schemaPath = Path.Combine(outputDir, $"{name}.avsc");
                await File.WriteAllTextAsync(schemaPath, schemaJson);

                if (verbose)
                {
                    Console.WriteLine($"Schema written to: {schemaPath}");
                }
            }

            Console.WriteLine($"Avro schemas written to: {outputDir}");
            Console.WriteLine($"Total schemas: {schemas.Count}");

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
