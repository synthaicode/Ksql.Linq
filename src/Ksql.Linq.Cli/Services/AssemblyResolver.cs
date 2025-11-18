namespace Ksql.Linq.Cli.Services;

/// <summary>
/// Resolves assembly paths from project files or DLL paths.
/// </summary>
public static class AssemblyResolver
{
    /// <summary>
    /// Resolves the assembly path from a project file or DLL path.
    /// </summary>
    public static string? Resolve(string projectPath, bool verbose)
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
