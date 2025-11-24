using System;
using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Script;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Examples.DesigntimeKsqlTumbling;

[KsqlTopic("ticks")]
public class Tick
{
    [KsqlKey(1)]
    public string Symbol { get; set; } = string.Empty;

    [KsqlTimestamp]
    public DateTime TimestampUtc { get; set; }

    public decimal Price { get; set; }
}

public class MinuteBar
{
    public string Symbol { get; set; } = string.Empty;
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
}

/// <summary>
/// Sample context that defines a Tumbling-window aggregation via ToQuery.
/// Intended for design-time KSQL generation (no runtime execution required).
/// </summary>
public sealed class TumblingKsqlContext : KsqlContext
{
    public TumblingKsqlContext(IConfiguration configuration, ILoggerFactory? loggerFactory = null)
        : base(configuration, loggerFactory)
    {
    }

    public EventSet<Tick> Ticks { get; set; } = null!;
    public EventSet<MinuteBar> MinuteBars { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Tick>();

        modelBuilder.Entity<MinuteBar>().ToQuery(q => q
            .From<Tick>()
            .Tumbling(t => t.TimestampUtc, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
            .GroupBy(t => t.Symbol)
            .Select(g => new MinuteBar
            {
                Symbol = g.Key,
                Open = g.EarliestByOffset(x => x.Price),
                High = g.Max(x => x.Price),
                Low = g.Min(x => x.Price),
                Close = g.LatestByOffset(x => x.Price)
            }));
    }

    protected override bool IsDesignTime => true;
    protected override bool SkipSchemaRegistration => true;
}

/// <summary>
/// Design-time factory for TumblingKsqlContext.
/// Tooling can use this to inspect the Tumbling-window KSQL without starting the app.
/// </summary>
public sealed class TumblingDesignTimeKsqlContextFactory : IDesignTimeKsqlContextFactory
{
    public KsqlContext CreateDesignTimeContext()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        return new TumblingKsqlContext(configuration);
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

        var factory = new TumblingDesignTimeKsqlContextFactory();
        using var context = factory.CreateDesignTimeContext();

        var builder = new DefaultKsqlScriptBuilder();
        var script = builder.Build(context);

        Console.WriteLine("-- Design-time KSQL script for TumblingKsqlContext");
        Console.WriteLine(script.ToSql());
    }
}
