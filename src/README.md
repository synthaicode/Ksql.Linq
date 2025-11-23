
## Overview

Ksql.Linq is a C# library that unifies Kafka/ksqlDB and Avro/Schema Registry usage. It lets you control Kafka Streams and ksqlDB in a LINQ style and offers the following capabilities.

- Operate Kafka and ksqlDB through a LINQ-based DSL.
- Design type-safe schemas with Avro and Schema Registry.
- Detect Streams/Tables and Pull/Push modes automatically.
- Support operations with DLQ, retry, and commit helpers.
- **Self-healing persistent queries:** automatically stabilizes CTAS/CSAS queries
  by retrying, pre-creating internal topics, and recovering from transient errors.
- **Market-schedule–aware OHLC bars (support feature):**
   Generate OHLC bars (e.g., 1s/1m/5m/15m/1h) strictly aligned to exchange trading sessions.
   The engine skips closed hours and holidays, handles DST correctly, and offers gap policies
   (skip, carry-forward close, or emit sentinel). Pre-/post-market can be toggled per schedule.

## Release Notes

Version-specific changes (including v0.9.7 and later) are documented in the **Release notes** section of this NuGet page.

---

## Documentation

For full documentation, advanced usage, and design notes, see the project wiki:

➡ **Ksql.Linq Wiki**  
https://github.com/synthaicode/Ksql.Linq/wiki

## Minimal Quick Start

> NOTE: In this repo's docker-compose test environment, use  
> `127.0.0.1:39092` (Kafka) / `18081` (Schema Registry) / `18088` (ksqlDB).  
> Samples below align to these ports. Adjust URLs when using external services.

This document is a minimal quick start guide for **Ksql.Linq** NuGet consumers.

---

### Prerequisites

- .NET 8 SDK
- Kafka / Schema Registry / ksqlDB running

---

### Minimal `appsettings.json`

```json
{
  "KsqlDsl": {
    "Common": {
      "BootstrapServers": "127.0.0.1:39092",
      "ClientId": "my-app"
    },
    "SchemaRegistry": {
      "Url": "http://127.0.0.1:18081"
    },
    "KsqlDbUrl": "http://127.0.0.1:18088"
  }
}
```

### Minimal code (produce / consume)
``` CSharp
using Ksql.Linq;
using Ksql.Linq.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

[KsqlTopic("quickstart-basic")]
public class Hello
{
    public int Id { get; set; }
    public string Text { get; set; } = "";
}

public class AppCtx : KsqlContext
{
    public AppCtx(IConfiguration cfg, ILoggerFactory? lf = null)
        : base(cfg, lf) { }

    public EventSet<Hello> Hellos { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder b)
        => b.Entity<Hello>();
}

var cfg = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .Build();

await using var ctx = new AppCtx(
    cfg,
    LoggerFactory.Create(b => b.AddConsole())
);

await ctx.Hellos.AddAsync(new Hello
{
    Id = 1,
    Text = "Hello Ksql.Linq"
});

await ctx.Hellos.ForEachAsync(m =>
{
    Console.WriteLine(m.Text);
    return Task.CompletedTask;
});
```
Notes

Use KsqlDsl:Topics.{name}.Creation.* to control partitions / retention per topic.

For secured clusters, configure SecurityProtocol / Sasl* under KsqlDsl:Common.
