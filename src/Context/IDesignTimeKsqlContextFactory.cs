using System;

namespace Ksql.Linq;

/// <summary>
/// Design-time factory for creating a KsqlContext without starting the full application.
/// This mirrors Entity Framework's IDesignTimeDbContextFactory pattern and is intended
/// for tooling scenarios such as offline KSQL script generation.
/// </summary>
public interface IDesignTimeKsqlContextFactory
{
    /// <summary>
    /// Creates a KsqlContext instance for design-time usage.
    /// Implementations should avoid performing external side effects such as
    /// connecting to Kafka or ksqlDB, and focus on making the model available.
    /// </summary>
    /// <returns>A configured KsqlContext instance.</returns>
    KsqlContext CreateDesignTimeContext();
}

