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

        Type factoryType;
        Type contextType;

        if (!string.IsNullOrEmpty(contextTypeName))
        {
            factoryType = factories.FirstOrDefault(f =>
            {
                var ctxType = GetContextTypeFromFactory(f);
                return ctxType.Name == contextTypeName || ctxType.FullName == contextTypeName;
            }) ?? throw new InvalidOperationException(
                $"No factory found for context type '{contextTypeName}'. Available factories: " +
                string.Join(", ", factories.Select(GetFactoryContextTypeName)));

            contextType = GetContextTypeFromFactory(factoryType);
        }
        else
        {
            if (factories.Count > 1)
            {
                throw new InvalidOperationException(
                    $"Multiple factories found. Please specify --context. Available: " +
                    string.Join(", ", factories.Select(GetFactoryContextTypeName)));
            }

            factoryType = factories[0];
            contextType = GetContextTypeFromFactory(factoryType);
        }

        var factory = Activator.CreateInstance(factoryType) as IDesignTimeKsqlContextFactory
            ?? throw new InvalidOperationException($"Failed to create factory instance: {factoryType.Name}");

        var context = factory.CreateDesignTimeContext()
            ?? throw new InvalidOperationException("CreateDesignTimeContext returned null");

        return new LoadResult(
            Context: context,
            AssemblyName: assemblyName.Name ?? "Unknown",
            AssemblyVersion: assemblyName.Version?.ToString() ?? "0.0.0",
            ContextTypeName: contextType.FullName ?? contextType.Name);
    }

    private static Type GetContextTypeFromFactory(Type factoryType)
    {
        var method = factoryType.GetMethod("CreateDesignTimeContext",
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"CreateDesignTimeContext method not found on {factoryType.Name}");

        return method.ReturnType;
    }

    private static string GetFactoryContextTypeName(Type factoryType)
    {
        return GetContextTypeFromFactory(factoryType).Name;
    }

    public record LoadResult(
        KsqlContext Context,
        string AssemblyName,
        string AssemblyVersion,
        string ContextTypeName);
}
