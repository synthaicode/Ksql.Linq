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
        if (projectPath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
        {
            return Path.GetFullPath(projectPath);
        }

        if (projectPath.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase))
        {
            var projectDir = Path.GetDirectoryName(Path.GetFullPath(projectPath));
            var projectName = Path.GetFileNameWithoutExtension(projectPath);

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

            Console.Error.WriteLine($"Assembly not found. Please build the project first: dotnet build {projectPath}");
            return null;
        }

        Console.Error.WriteLine($"Unknown file type: {projectPath}. Expected .csproj or .dll");
        return null;
    }
}

