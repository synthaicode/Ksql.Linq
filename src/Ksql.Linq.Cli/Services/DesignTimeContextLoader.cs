using System.Reflection;

namespace Ksql.Linq.Cli.Services;

/// <summary>
/// Loads KsqlContext from a compiled assembly using IDesignTimeKsqlContextFactory.
/// </summary>
public class DesignTimeContextLoader
{
    public LoadResult Load(string assemblyPath, string? contextTypeName, string[] args)
    {
        if (!File.Exists(assemblyPath))
        {
            throw new FileNotFoundException($"Assembly not found: {assemblyPath}");
        }

        var assembly = Assembly.LoadFrom(assemblyPath);
        var assemblyName = assembly.GetName();

        var factoryInterface = typeof(IDesignTimeKsqlContextFactory);
        var factories = assembly.GetTypes()
            .Where(t => !t.IsAbstract && !t.IsInterface)
            .Where(t => factoryInterface.IsAssignableFrom(t))
            .ToList();

        if (factories.Count == 0)
        {
            throw new InvalidOperationException(
                $"No IDesignTimeKsqlContextFactory implementation found in assembly: {assemblyPath}");
        }

        var created = factories
            .Select(f => Activator.CreateInstance(f) as IDesignTimeKsqlContextFactory)
            .Where(f => f != null)
            .Select(f => (Factory: f!, Context: f!.CreateDesignTimeContext()
                ?? throw new InvalidOperationException($"CreateDesignTimeContext returned null in {f.GetType().Name}")))
            .ToList();

        if (created.Count == 0)
            throw new InvalidOperationException("Failed to create any IDesignTimeKsqlContextFactory instances.");

        (IDesignTimeKsqlContextFactory Factory, KsqlContext Context) selected;

        if (!string.IsNullOrEmpty(contextTypeName))
        {
            selected = created.FirstOrDefault(c =>
                string.Equals(c.Context.GetType().Name, contextTypeName, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(c.Context.GetType().FullName, contextTypeName, StringComparison.OrdinalIgnoreCase));

            if (selected.Factory == null)
                throw new InvalidOperationException(
                    $"No factory found for context type '{contextTypeName}'. Available contexts: " +
                    string.Join(", ", created.Select(c => c.Context.GetType().Name)));
        }
        else
        {
            if (created.Count > 1)
            {
                throw new InvalidOperationException(
                    $"Multiple factories found. Please specify --context. Available: " +
                    string.Join(", ", created.Select(c => c.Context.GetType().Name)));
            }
            selected = created[0];
        }

        var factory = selected.Factory;
        var context = selected.Context;
        var contextType = context.GetType();

        return new LoadResult(
            Context: context,
            AssemblyName: assemblyName.Name ?? "Unknown",
            AssemblyVersion: assemblyName.Version?.ToString() ?? "0.0.0",
            ContextTypeName: contextType.FullName ?? contextType.Name);
    }

    public record LoadResult(
        KsqlContext Context,
        string AssemblyName,
        string AssemblyVersion,
        string ContextTypeName);
}
