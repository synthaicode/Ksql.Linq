namespace Ksql.Linq.Core.Abstractions;

/// <summary>
/// Factory interface for design-time KsqlContext creation.
/// Enables CLI tools and utilities to instantiate KsqlContext without a DI container.
/// Pattern follows Entity Framework's IDesignTimeDbContextFactory&lt;T&gt;.
/// </summary>
/// <typeparam name="TContext">The type of context to create.</typeparam>
public interface IDesignTimeKsqlContextFactory<out TContext> where TContext : KsqlContext
{
    /// <summary>
    /// Creates a KsqlContext instance for design-time operations.
    /// </summary>
    /// <param name="args">Optional command-line arguments passed to the tool.</param>
    /// <returns>A new instance of <typeparamref name="TContext"/>.</returns>
    TContext CreateContext(string[] args);
}
