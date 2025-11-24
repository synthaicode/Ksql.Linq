
---

## 📋 Table of Contents

1. [AI Profile (This Section)](#-ksqllinq-design-support-ai-profile)
   - [Conversation Flow](#2-conversation-flow)
   - [Output Format](#3-output-format)
   - [Knowledge Base](#4-knowledge-base)
   - [Amagi Protocol](#5-relationship-to-amagi-protocol)
   - [Tone & Communication](#6-tone--communication-style)
   - [Example Interaction](#7-example-interaction)
   - [Anti-Patterns](#8-anti-patterns-to-avoid)
   - [Feedback & Reporting](#9-feedback--issue-reporting-protocol)
2. [Library Overview](#library-overview)
3. [Core Architecture](#core-architecture)
4. [Design Patterns](#design-patterns)
5. [Common Use Cases](#common-use-cases)
6. [API Reference Quick Start](#api-reference-quick-start)
7. [Examples Index](#examples-index)
8. [Decision Trees](#decision-trees)
9. [Best Practices](#best-practices)

---

## Library Overview

### What is Ksql.Linq?

Ksql.Linq is a **LINQ-based DSL** for Kafka/ksqlDB stream processing in C#/.NET. It provides:

- **Type-safe Kafka operations** via C# entities and LINQ expressions
- **Automatic Avro schema management** with Schema Registry integration
- **Streamiz.Kafka.Net backend** for materialized views and state stores
- **Push/Pull query support** with automatic detection
- **Self-healing persistent queries** (CTAS/CSAS) with retry logic
- **Market-schedule-aware OHLC bar generation** for financial data

### Key Value Propositions

1. **No raw KSQL strings**: Define queries in C# LINQ, generate KSQL automatically
2. **Type safety**: Compile-time checking of schemas and queries
3. **Design-time tooling**: Generate KSQL scripts and Avro schemas without running Kafka
4. **Production-ready**: Built-in DLQ, retry, error handling, and monitoring

### What the AI and the Library Can Do

When a developer asks "What can you do?", treat it as two questions:

- **As an AI assistant**, you can:
  - Help design and review stream/table topologies based on existing POCOs and LINQ.
  - Suggest how to express a data flow (input → processing → output) in Ksql.Linq.
  - Analyze lag/errors/schema changes and narrow down which docs or options to check.

- **As a library**, Ksql.Linq can:
  - Define STREAM/TABLE/VIEW mappings over Kafka topics using POCOs and attributes.
  - Express joins, windowed aggregations, enrichments, and error-handling patterns in LINQ.
  - Generate KSQL and Avro schemas at design time, and run self-healing persistent queries at runtime.

---

## Core Architecture

### Component Hierarchy

```
KsqlContext (DbContext-like)
  ├── EventSet<T> (DbSet-like)
  │     ├── Producer operations (Add, AddRange)
  │     ├── Consumer operations (ForEach, Subscribe)
  │     └── Query operations (Where, Select, GroupBy, Join)
  ├── Schema Registry Client
  ├── Streamiz Topology Builder
  └── Configuration Management
```

### Main Components

| Component | Purpose | Location |
|-----------|---------|----------|
| `KsqlContext` | Central orchestrator, DbContext equivalent | `src/Context/` |
| `EventSet<T>` | Entity collection for streams/tables | `src/EntitySets/` |
| `IModelBuilder` | Fluent API for entity configuration | `src/Mapping/` |
| `IKsqlExecutor` | Executes KSQL commands | `src/Messaging/` |
| `QueryBuilder` | Converts LINQ to KSQL | `src/Query/` |
| `RuntimeMonitor` | Observability and diagnostics | `src/Runtime/Monitor/` |
| `ScheduleEngine` | Market-aware time handling | `src/Runtime/Scheduling/` |

### Data Flow

```
Producer Flow:
  Entity → EventSet.Add() → Avro Serialization → Kafka Topic → ksqlDB Stream

Consumer Flow:
  Kafka Topic → ksqlDB Query (Push/Pull) → Avro Deserialization → EventSet → Consumer Handler

Query Flow:
  LINQ Expression → QueryBuilder → KSQL Statement → ksqlDB Server → Results
```

---

## Design Patterns

> **Status:** Mixed (some patterns are fully implemented and documented in the Wiki; others describe planned / future directions.  
> When in doubt, prefer patterns that clearly match the APIs and behavior described in the Ksql.Linq Wiki.)

### Pattern 1: Basic Entity Definition

```csharp
using Ksql.Linq.Core.Attributes;

// Stream-backed entity (default)
[KsqlTopic("user-events")]
public class UserEvent
{
    [KsqlKey]
    public string UserId { get; set; } = "";

    [KsqlTimestamp]
    public long EventTime { get; set; }

    public string EventType { get; set; } = "";
    public string Payload { get; set; } = "";
}

// Table-backed entity (materialized view)
[KsqlTopic("user-profiles")]
[KsqlTable]  // Marks as TABLE instead of STREAM
public class UserProfile
{
    [KsqlKey]
    public string UserId { get; set; } = "";

    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
}

// Decimal precision control
public class PriceData
{
    [KsqlKey]
    public string Symbol { get; set; } = "";

    [KsqlDecimal(precision: 18, scale: 8)]
    public decimal Price { get; set; }
}
```

**Key Attributes:**
- `[KsqlTopic("name")]`: Maps entity to Kafka topic/ksqlDB stream
- `[KsqlKey]`: Marks message key field(s)
- `[KsqlTable]`: Declares entity as TABLE (default: STREAM)
- `[KsqlTimestamp]`: Custom timestamp field (Unix epoch ms)
- `[KsqlDecimal(p, s)]`: Precision/scale for decimal types

---

## Filling Missing Information Together

Some design decisions are unsafe to guess (see **AI MUST NOT GUESS** in the conversation patterns). When key information is missing, use targeted questions to fill the gap before recommending a pattern.

### Time Semantics (Event-time vs Processing-time)

- When unclear, ask:
  - “Which property on your POCO represents the *business event time* (when the event actually happened)?"
  - “Is there a difference between when the event happens and when it is ingested into Kafka?"
- If the user is unsure, explain:
  - Event-time is usually the domain timestamp (e.g., trade execution time).
  - Processing-time is the ingestion/processing timestamp.
- Once clarified:
  - Map the chosen property with `[KsqlTimestamp]`.
  - Then choose windowing patterns in Design Patterns (e.g., tumbling/hopping) based on that column.

### Key Selection

- When the key is ambiguous, ask:
  - “Which field(s) uniquely identify this entity for updates or lookups?"
  - “Do you expect multiple records per key, or only the latest state per key?"
- If no clear key exists:
  - Explain that dedup, TABLEs, and idempotent updates become difficult without a stable key.
  - Suggest options: introduce a synthetic key, or change the modeling to keep events append-only.
- Once clarified:
  - Apply `[KsqlKey]` to the chosen field(s).
  - Decide whether the entity is better modeled as STREAM or TABLE.

### TABLE vs STREAM

- When the modeling is unclear, ask:
  - “Does this data represent *events over time* or the *latest state per key*?"
  - “Will consumers mostly react to each event, or read the current snapshot?"
- Guidance:
  - **STREAM**: append-only events, order matters, often many records per key.
  - **TABLE**: materialized latest state per key, good for enrichment and pull-style reads.
- Once clarified:
  - Use `[KsqlTable]` for TABLE entities and leave it off for STREAMs.
  - Pick design patterns accordingly (stream enrichment, windowed aggregation, snapshot reads, etc.).

---

### Pattern 2: Context Definition

```csharp
public class TradingContext : KsqlContext
{
    public TradingContext(IConfiguration config, ILoggerFactory? loggerFactory = null)
        : base(config, loggerFactory)
    {
    }

    // EventSet properties (DbSet equivalent)
    public EventSet<Trade> Trades { get; set; } = null!;
    public EventSet<Quote> Quotes { get; set; } = null!;
    public EventSet<OHLCV> Bars { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder builder)
    {
        // Register entities so that attributes ([KsqlTopic], [KsqlKey], etc.) are inspected
        builder.Entity<Trade>();
        builder.Entity<Quote>();
        builder.Entity<OHLCV>();
    }
}
```

**Design Guidelines:**
1. Inherit from `KsqlContext`
2. Declare `EventSet<T>` properties for each entity
3. Use `OnModelCreating` for fluent configuration
4. Pass `IConfiguration` for settings (appsettings.json)
5. Optional: `ILoggerFactory` for diagnostics

---

### Pattern 3: Configuration (appsettings.json)

```json
{
  "KsqlDsl": {
    "Common": {
      "BootstrapServers": "localhost:9092",
      "ClientId": "my-app",
      "SecurityProtocol": "SaslSsl",  // Optional: SASL_SSL, etc.
      "SaslMechanism": "Plain",
      "SaslUsername": "user",
      "SaslPassword": "pass"
    },
    "SchemaRegistry": {
      "Url": "http://localhost:8081",
      "BasicAuthUserInfo": "user:pass"  // Optional
    },
    "KsqlDbUrl": "http://localhost:8088",
    "Topics": {
      "user-events": {
        "Creation": {
          "Partitions": 6,
          "ReplicationFactor": 3,
          "RetentionMs": 604800000  // 7 days
        }
      }
    },
    "Consumer": {
      "GroupId": "my-consumer-group",
      "AutoOffsetReset": "Earliest",
      "EnableAutoCommit": true
    },
    "Producer": {
      "Acks": "All",
      "EnableIdempotence": true
    }
  }
}
```

---

### Pattern 4: Producing Messages

```csharp
await using var ctx = new TradingContext(config, loggerFactory);

// Single message
await ctx.Trades.AddAsync(new Trade
{
    TradeId = "T001",
    Symbol = "AAPL",
    Price = 150.25m,
    Quantity = 100,
    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
});

// Batch messages
var trades = new List<Trade> { /* ... */ };
await ctx.Trades.AddRangeAsync(trades);

// With headers
await ctx.Trades.AddAsync(trade, headers: new Dictionary<string, byte[]>
{
    ["correlation-id"] = Encoding.UTF8.GetBytes(correlationId)
});
```

---

### Pattern 5: Consuming Messages (Push Query)

```csharp
// Simple consumption
await ctx.Trades.ForEachAsync(async trade =>
{
    Console.WriteLine($"{trade.Symbol}: {trade.Price}");
});

// With DLQ (Dead Letter Queue)
await ctx.Trades
    .OnError(ErrorAction.DLQ)
    .ForEachAsync(async trade =>
    {
        await ProcessTradeAsync(trade);
    });

// Manual commit control
await ctx.Trades.ForEachAsync(
    (trade, headers, meta) =>
    {
        Console.WriteLine($"{meta.Topic}:{meta.Offset}");
        ctx.Trades.Commit(trade);
        return Task.CompletedTask;
    },
    timeout: TimeSpan.FromSeconds(10),
    autoCommit: false);
```

---

### Pattern 6: LINQ Queries (Push)

```csharp
// Filter
var view = ctx.Trades
    .Where(t => t.Symbol == "AAPL" && t.Price > 100)
    ;

await view.ForEachAsync(async trade =>
{
    Console.WriteLine($"AAPL trade: {trade.Price}");
});

// Projection
var view = ctx.Trades
    .Select(t => new { t.Symbol, t.Price, t.Timestamp })
    ;

// Join (Stream-Table)
var enriched = ctx.Trades
    .Join(
        ctx.Quotes,
        trade => trade.Symbol,
        quote => quote.Symbol,
        (trade, quote) => new
        {
            trade.TradeId,
            trade.Symbol,
            trade.Price,
            Spread = quote.AskPrice - quote.BidPrice
        }
    )
    ;

await enriched.ForEachAsync(async e =>
{
    Console.WriteLine($"{e.Symbol} spread: {e.Spread}");
});
```

---

### Pattern 7: Windowed Aggregation

```csharp
// Tumbling window (non-overlapping)
var bars = ctx.Trades
    .GroupBy(t => t.Symbol)
    .Tumbling(r => r.TimestampUtc, new Windows { Minutes = new[] { 1 } })
    .Select(g => new OHLCV
    {
        Symbol = g.Key,
        Open = g.First().Price,
        High = g.Max(t => t.Price),
        Low = g.Min(t => t.Price),
        Close = g.Last().Price,
        Volume = g.Sum(t => t.Quantity),
        WindowStart = g.WindowStart(),  // Include for continuation mode
        WindowEnd = g.WindowEnd()
    })
    ;

```

**Window Types (current):**
- **Tumbling**: Fixed-size, non-overlapping (e.g., 1-minute bars) – **implemented**

---

### Pattern 8: Pull-style Queries over TABLEs

```csharp
// Snapshot current state of a TABLE-backed entity set
var topUsers = await ctx.UserStats
    .Where(s => s.Score > 1000)
    .OrderByDescending(s => s.Score)
    .Take(10)
    .ToListAsync();  // Pull-style query over a TABLE
```

**Push vs Pull (conceptual):**
- **Push Query**: Continuous stream of updates (`.ForEachAsync()`, `.Subscribe()`)
- **Pull-style Query**: One-time snapshot over a TABLE (`.ToListAsync()` on `IEntitySet<T>` / table-backed sets)

---

### Pattern 9: Error Handling Strategies

```csharp
// 1. Dead Letter Queue (DLQ): send failures to DLQ topic
await ctx.Trades
    .OnError(ErrorAction.DLQ)
    .ForEachAsync(trade => ProcessTrade(trade));

// 2. Retry policy (configured via appsettings)
await ctx.Trades
    .OnError(ErrorAction.Retry)
    .ForEachAsync(trade => ProcessTrade(trade));

// 3. Custom error handler
await ctx.Trades.ForEachAsync(
    async trade =>
    {
        try
        {
            await ProcessTrade(trade);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process trade {TradeId}", trade.TradeId);
            // Custom logic: alert, metrics, etc.
        }
    }
);

// 4. Manual commit with error handling
await ctx.Trades.ForEachAsync(
    (trade, headers, meta) =>
    {
        try
        {
            ProcessTrade(trade);
            ctx.Trades.Commit(trade);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process trade {TradeId}", trade.TradeId);
        }

        return Task.CompletedTask;
    },
    timeout: TimeSpan.FromSeconds(10),
    autoCommit: false);
```

---

### Pattern 10: Design-Time Code Generation

```csharp
// IDesignTimeKsqlContextFactory for CLI tooling
public class TradingContextFactory : IDesignTimeKsqlContextFactory
{
    public KsqlContext CreateDbContext(string[] args)
    {
        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        return new TradingContext(config);
    }
}
```

**CLI Usage:**
```bash
# Generate KSQL scripts from entities
dotnet ksql script --project MyProject.csproj --output schema.sql

# Generate Avro schemas
dotnet ksql avro --project MyProject.csproj --output-dir ./schemas

# From compiled DLL
dotnet ksql script --assembly bin/Debug/net8.0/MyApp.dll
```

---

## Common Use Cases

> **Status:** Guidance-oriented (scenario descriptions may combine current capabilities with roadmap directions.  
> Validate concrete steps against the relevant Wiki pages, examples, and your installed Ksql.Linq version.)

### Use Case 1: Real-Time Event Processing

**Scenario**: Process user clickstream events in real-time

```csharp
[KsqlTopic("clickstream")]
public class ClickEvent
{
    [KsqlKey] public string SessionId { get; set; } = "";
    public string UserId { get; set; } = "";
    public string PageUrl { get; set; } = "";
    public long Timestamp { get; set; }
}

public class ClickstreamContext : KsqlContext
{
    public EventSet<ClickEvent> Clicks { get; set; } = null!;
    // ... constructor ...
}

// Consumer
await ctx.Clicks
    .Where(c => c.PageUrl.Contains("/checkout"))
    .ForEachAsync(async click =>
    {
        await analyticsService.TrackCheckoutView(click.UserId);
    });
```

---

### Use Case 2: Stream Enrichment (Join)

**Scenario**: Enrich order events with customer data

```csharp
[KsqlTopic("orders")] public class Order { /* ... */ }
[KsqlTopic("customers")] [KsqlTable] public class Customer { /* ... */ }

var enrichedOrders = ctx.Orders
    .Join(
        ctx.Customers,
        order => order.CustomerId,
        customer => customer.CustomerId,
        (order, customer) => new
        {
            order.OrderId,
            order.Amount,
            customer.Name,
            customer.Tier
        }
    )
    ;

await enrichedOrders.ForEachAsync(async e =>
{
    if (e.Tier == "Premium" && e.Amount > 1000)
        await notificationService.SendVIPAlert(e);
});
```

---

### Use Case 3: Windowed Aggregation (Time-Series)

**Scenario**: Calculate 1-minute OHLCV bars from trades

```csharp
[KsqlTopic("trades")]
public class Trade
{
    [KsqlKey] public string Symbol { get; set; } = "";
    public decimal Price { get; set; }
    public long Quantity { get; set; }
    public long Timestamp { get; set; }
}

[KsqlTopic("bars_1m")]
public class OHLCV
{
    [KsqlKey] public string Symbol { get; set; } = "";
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
    public long Volume { get; set; }
}

var bars = ctx.Trades
    .GroupBy(t => t.Symbol)
    .Tumbling(r => r.TimestampUtc, new Windows { Minutes = new[] { 1 } })
    .Select(g => new OHLCV
    {
        Symbol = g.Key,
        Open = g.First().Price,
        High = g.Max(t => t.Price),
        Low = g.Min(t => t.Price),
        Close = g.Last().Price,
        Volume = g.Sum(t => t.Quantity)
    })
    ;

// Persist to topic (conceptual)
// In the current runtime, materialization is managed via KsqlContext and SchemaRegistrar
// (e.g., RegisterAndMaterializeAsync) rather than a direct `bars.MaterializeAsync(...)` API.
```

---

### Use Case 5: Data Pipeline with DLQ

**Scenario**: Process IoT sensor data with fault tolerance

```csharp
[KsqlTopic("sensor-readings")]
public class SensorReading
{
    [KsqlKey] public string DeviceId { get; set; } = "";
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public long Timestamp { get; set; }
}

// 1. Main pipeline with DLQ
await ctx.SensorReadings
    .OnError(ErrorAction.DLQ)  // Failed messages → DLQ topic
    .ForEachAsync(async reading =>
    {
        // Validate
        if (reading.Temperature < -50 || reading.Temperature > 150)
            throw new ValidationException("Temperature out of range");

        // Process
        await timeseriesDb.WriteAsync(reading);
    });

// 2. DLQ inspection / replay lane
await ctx.Dlq.ForEachAsync(record =>
{
    Console.WriteLine($"DLQ: {record.RawText}");
    // Optional: parse, fix, and route to a repair topic
    return Task.CompletedTask;
});
```

---

## API Reference Quick Start

> **Status:** Stable summary of released APIs.  
> For the canonical surface area and signatures, always refer to the Ksql.Linq Wiki (`API-Reference.md`, `Public-API.md`) and the actual NuGet package.

### KsqlContext Methods

| Method | Purpose | Example |
|--------|---------|---------|

### EventSet<T> Methods

| Method | Purpose | Type |
|--------|---------|------|
| `AddAsync(entity)` | Produce single message | Producer |
| `AddRangeAsync(entities)` | Produce batch | Producer |
| `ForEachAsync(handler)` | Consume with callback | Consumer |
| `Subscribe(observer)` | Rx.NET observable pattern | Consumer |
| `Where(predicate)` | Filter query | Query |
| `Select(projection)` | Transform query | Query |
| `Join(...)` | Join streams/tables | Query |
| `GroupBy(key).Tumbling(...)` | Windowed aggregation | Query |
| `ToListAsync()` | Pull-style snapshot over TABLE | Pull |

### LINQ Window Functions

| Function | Returns | Usage |
|----------|---------|-------|
| `g.WindowStart()` | `long` | Unix timestamp (ms) of window start |
| `g.WindowEnd()` | `long` | Unix timestamp (ms) of window end |
| `g.First()` | `T` | First element in window |
| `g.Last()` | `T` | Last element in window |
| `g.Count()` | `int` | Count of elements |
| `g.Sum(selector)` | `TResult` | Sum of projected values |
| `g.Average(selector)` | `double` | Average of projected values |
| `g.Min(selector)` | `TResult` | Minimum value |
| `g.Max(selector)` | `TResult` | Maximum value |

---

## Examples Index

Ksql.Linq includes 30+ working examples. Key categories:

### Basics
- `hello-world`: Minimal producer/consumer
- `basic-produce-consume`: Fundamental patterns
- `configuration`: appsettings.json setup

### Queries
- `query-basics`: LINQ → KSQL fundamentals
- `query-filter`: `.Where()` filtering
- `table-cache-lookup`: Table joins
- `pull-query`: Materialized view queries

### Windowing
- `windowing`: Tumbling/hopping aggregation
- `bar-1m-live-consumer`: OHLCV bar consumer
- `continuation-schedule`: Continuation-based windowing

### Error Handling
- `error-handling`: Retry strategies
- `error-handling-dlq`: Dead Letter Queue pattern
- `manual-commit`: Manual offset management

### Advanced
- `daily-comparison`: Multi-timeframe aggregation
- `runtime-events`: Monitoring and diagnostics
- `designtime-ksql-script`: Design-time code generation

**Full index**: See `examples/README.md` and `examples/index.md`

---

## Decision Trees

> **Status:** Design guidance; some branches may assume planned improvements or optional components.  
> Use these trees as a conversation aid, and cross-check chosen options with the Wiki and current version before implementation.

### Should I use STREAM or TABLE?

```
Is the data a changelog (updates/deletes)?
├─ Yes → Use [KsqlTable]
│   Example: User profiles, product catalog
│
└─ No → Use STREAM (default)
    Example: Clickstream, trades, logs
```

### Should I use Push or Pull query?

```
Do I need continuous updates?
├─ Yes → Push query (.ForEachAsync(), .Subscribe())
│   Example: Real-time alerts, dashboards
│
└─ No → Pull query (.FirstOrDefaultAsync(), .ToListAsync())
    Example: REST API lookups, batch reports
```

### Which window type?

```
What's the aggregation pattern?
├─ Fixed-size, non-overlapping → Tumbling
│   Example: 1-minute bars, hourly summaries
│
├─ Fixed-size, overlapping → Hopping
│   Example: 5-min moving average
│
└─ Variable-size, gap-based → Session
    Example: User sessions, burst detection
```

### How to handle errors?

```
What should happen on failure?
├─ Retry automatically → .OnError(ErrorAction.Retry, maxRetries: N)
│
├─ Park in DLQ for manual review → .OnError(ErrorAction.DLQ)
│
└─ Custom logic → try/catch in ForEachAsync handler
```

---

## Best Practices

> **Status:** Stable operational guidance, aligned with runtime and operations documentation.  
> For detailed configuration values and tuning knobs, refer to `Runtime-Tuning-Plan-v0-9-6.md`, `Lag-Monitoring-and-Tuning.md`, and related Wiki pages.

### 1. Entity Design

✅ **DO:**
- Use `[KsqlKey]` on key field(s)
- Use `[KsqlTimestamp]` for custom event time
- Use `[KsqlDecimal(p, s)]` for precise decimal values
- Keep entities simple (POCOs)
- Use meaningful topic names

❌ **DON'T:**
- Mix streams and tables without `[KsqlTable]` attribute
- Omit key fields (causes null-key messages)
- Use `DateTime` (use `long` Unix epoch instead)

---

### 2. Context Design

✅ **DO:**
- Inherit from `KsqlContext`
- Use `OnModelCreating` for configuration
- Dispose context properly (`await using`)
- Configure topics in `appsettings.json`

❌ **DON'T:**
- Create multiple contexts for same topic (use DI/singleton pattern)
- Hardcode connection strings (use `IConfiguration`)

---

### 3. Performance

✅ **DO:**
- Use `AddRangeAsync()` for batch produce
- Enable producer idempotence (`EnableIdempotence: true`)
- Set appropriate partitions (6-12 per broker)
- Use compression (`CompressionType: "gzip"`)
- Enable auto-commit for read-only consumers

❌ **DON'T:**
- Send one message at a time in tight loop (use batching)
- Use `AutoOffsetReset: "Earliest"` in production without reason
- Create unbounded windows (causes memory issues)

---

### 4. Error Handling

✅ **DO:**
- Use DLQ for unrecoverable errors
- Log errors with correlation IDs
- Set reasonable retry limits (3-5)
- Monitor DLQ topics

❌ **DON'T:**
- Retry indefinitely (causes backpressure)
- Swallow exceptions silently
- Mix error handling strategies

---

### 5. Schema Management

✅ **DO:**
- Use Schema Registry for production
- Version schemas properly
- Test schema compatibility
- Use design-time CLI to generate schemas

❌ **DON'T:**
- Change field types without migration
- Delete fields (mark as optional instead)
- Deploy incompatible schema changes

---

### 6. Windowing

✅ **DO:**
- Include `g.WindowStart()` in SELECT for continuation mode
- Use appropriate grace periods
- Set retention for windowed topics
- Test with historical data

❌ **DON'T:**
- Forget to emit window boundaries
- Use session windows for high-cardinality keys (memory leak)
- Mix windowing types without clear reason

---

### 7. Testing

✅ **DO:**
- Use Testcontainers for integration tests
- Test schema evolution scenarios
- Verify DLQ behavior
- Use design-time factory for unit tests

❌ **DON'T:**
- Test against production Kafka
- Skip schema compatibility tests
- Ignore edge cases (late arrivals, duplicates)

---

### 8. Monitoring

✅ **DO:**
- Use `RuntimeMonitor` for diagnostics
- Emit custom metrics
- Monitor consumer lag
- Track DLQ topic sizes
- Log correlation IDs

❌ **DON'T:**
- Deploy without observability
- Ignore consumer lag alerts
- Skip health checks

---

## Design-Time Workflow

### Recommended Development Flow

1. **Define entities** (POCOs with attributes)
2. **Create context** (inherit `KsqlContext`)
3. **Configure `OnModelCreating`** (fluent API)
4. **Generate KSQL scripts** (`dotnet ksql script`)
5. **Generate Avro schemas** (`dotnet ksql avro`)
6. **Review and apply** to ksqlDB cluster
7. **Implement producers/consumers**
8. **Test with Testcontainers**
9. **Deploy with monitoring**

### CLI Commands

```bash
# Install CLI tool
dotnet tool install -g Ksql.Linq.Cli

# Generate KSQL
dotnet ksql script --project MyApp.csproj --output schema.sql

# Generate Avro schemas
dotnet ksql avro --project MyApp.csproj --output-dir ./schemas

# From DLL
dotnet ksql script --assembly bin/Debug/net8.0/MyApp.dll
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `Topic not found` | Topic not auto-created | Set `auto.topic.create.enable: true` or pre-create |
| `Schema not compatible` | Breaking schema change | Use compatible evolution or new topic |
| `Consumer lag growing` | Processing too slow | Scale consumers, optimize handler |
| `Null key messages` | Missing `[KsqlKey]` | Add attribute to key property |
| `Timestamp out of order` | Late arrivals | Configure grace period |
| `DLQ not receiving` | No `.OnError(DLQ)` | Add error handler |

---

## Additional Resources

- **Wiki**: https://github.com/synthaicode/Ksql.Linq/wiki
- **Examples**: `examples/` directory (30+ samples)
- **API Docs**: XML documentation in NuGet package
- **Issue Tracker**: https://github.com/synthaicode/Ksql.Linq/issues

---

## Changelog

### Version 0.9.5
- Design-time KSQL/Avro generation
- `Ksql.Linq.Cli` .NET tool
- Improved error handling and DLQ

### Version 0.9.3
- Self-healing persistent queries
- Market-schedule-aware OHLC bars
- Streamiz backend improvements

---

**License**: MIT
**Maintained by**: SynthAICode with AI-human collaboration

---

*This document is designed for AI agents to understand and leverage Ksql.Linq effectively. For human-readable documentation, see the main README.md and Wiki.*
