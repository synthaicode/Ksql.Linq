using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Script;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace Examples.DesigntimeKsqlScript;

[KsqlTopic("orders")]
public class OrderEvent
{
    public int Id { get; set; }

    [KsqlTimestamp]
    public DateTime CreatedAt { get; set; }

    public string Status { get; set; } = string.Empty;
}

/// <summary>
/// View entity that demonstrates the ToQuery projection from OrderEvent.
/// </summary>
public class OrderSummary
{
    public int Id { get; set; }
    public DateTime CreatedDate { get; set; }
}

/// <summary>
/// Sample KsqlContext used for design-time KSQL script generation.
/// Runtime usage is optional; the primary goal is to expose the model.
/// </summary>
public sealed class OrdersKsqlContext : KsqlContext
{
    public OrdersKsqlContext(IConfiguration configuration, ILoggerFactory? loggerFactory = null)
        : base(configuration, loggerFactory)
    {
    }

    public EventSet<OrderEvent> Orders { get; set; } = null!;
    public EventSet<OrderSummary> DailySummaries { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OrderEvent>();
        modelBuilder.Entity<OrderSummary>().ToQuery(q => q
            .From<OrderEvent>()
            .Where(o => o.Status == "Completed")
            .Select(o => new OrderSummary
            {
                Id = o.Id,
                CreatedDate = o.CreatedAt.Date
            }));
    }

    protected override bool IsDesignTime => true;
    protected override bool SkipSchemaRegistration => true;
}

/// <summary>
/// Design-time factory that tooling (e.g. a future `dotnet ksql` CLI)
/// can use to create an OrdersKsqlContext without running the full app.
/// </summary>
public sealed class OrdersDesignTimeKsqlContextFactory : IDesignTimeKsqlContextFactory
{
    public KsqlContext CreateDesignTimeContext()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        // Design-time path: avoid any explicit calls that require running Kafka/ksqlDB.
        return new OrdersKsqlContext(configuration);
    }
}

internal sealed class Program
{
    private static void Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        var factory = new OrdersDesignTimeKsqlContextFactory();
        using var context = factory.CreateDesignTimeContext();

        var builder = new DefaultKsqlScriptBuilder();
        var script = builder.Build(context);

        Console.WriteLine("-- Design-time KSQL script for OrdersKsqlContext");
        Console.WriteLine(script.ToSql());

        var avroExporter = new DefaultAvroSchemaExporter();
        var schemas = avroExporter.ExportValueSchemas(context);

        Console.WriteLine();
        Console.WriteLine("-- Avro value schemas (.avsc) for entities");
        foreach (var kv in schemas)
        {
            Console.WriteLine($"-- Entity: {kv.Key}");
            Console.WriteLine(kv.Value);
            Console.WriteLine();
        }
    }
}
