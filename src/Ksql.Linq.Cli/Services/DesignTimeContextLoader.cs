using System.Reflection;
using Ksql.Linq.Core.Abstractions;

namespace Ksql.Linq.Cli.Services;

/// <summary>
/// Loads KsqlContext from a compiled assembly using IDesignTimeKsqlContextFactory.
/// </summary>
public class DesignTimeContextLoader
{
    /// <summary>
    /// Loads a KsqlContext from the specified assembly using the factory pattern.
    /// </summary>
    /// <param name="assemblyPath">Path to the compiled assembly (.dll)</param>
    /// <param name="contextTypeName">Optional specific context type name. If null, finds first factory.</param>
    /// <param name="args">Arguments to pass to the factory's CreateContext method.</param>
    /// <returns>Loaded KsqlContext instance and assembly metadata.</returns>
    public LoadResult Load(string assemblyPath, string? contextTypeName, string[] args)
    {
        if (!File.Exists(assemblyPath))
        {
            throw new FileNotFoundException($"Assembly not found: {assemblyPath}");
        }

        // Load the assembly
        var assembly = Assembly.LoadFrom(assemblyPath);
        var assemblyName = assembly.GetName();

        // Find all factory implementations
        var factoryInterface = typeof(IDesignTimeKsqlContextFactory<>);
        var factories = assembly.GetTypes()
            .Where(t => !t.IsAbstract && !t.IsInterface)
            .Where(t => t.GetInterfaces().Any(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == factoryInterface))
            .ToList();

        if (factories.Count == 0)
        {
            throw new InvalidOperationException(
                $"No IDesignTimeKsqlContextFactory<T> implementation found in assembly: {assemblyPath}");
        }

        Type factoryType;
        Type contextType;

        if (!string.IsNullOrEmpty(contextTypeName))
        {
            // Find factory that creates the specified context type
            factoryType = factories.FirstOrDefault(f =>
            {
                var iface = f.GetInterfaces()
                    .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == factoryInterface);
                var ctxType = iface.GetGenericArguments()[0];
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

        // Create factory instance
        var factory = Activator.CreateInstance(factoryType)
            ?? throw new InvalidOperationException($"Failed to create factory instance: {factoryType.Name}");

        // Get CreateContext method
        var createMethod = factoryType.GetMethod("CreateContext")
            ?? throw new InvalidOperationException($"CreateContext method not found on {factoryType.Name}");

        // Invoke CreateContext
        var context = createMethod.Invoke(factory, new object[] { args }) as KsqlContext
            ?? throw new InvalidOperationException($"CreateContext returned null or non-KsqlContext");

        return new LoadResult(
            Context: context,
            AssemblyName: assemblyName.Name ?? "Unknown",
            AssemblyVersion: assemblyName.Version?.ToString() ?? "0.0.0",
            ContextTypeName: contextType.FullName ?? contextType.Name);
    }

    private static Type GetContextTypeFromFactory(Type factoryType)
    {
        var iface = factoryType.GetInterfaces()
            .First(i => i.IsGenericType &&
                i.GetGenericTypeDefinition() == typeof(IDesignTimeKsqlContextFactory<>));
        return iface.GetGenericArguments()[0];
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
