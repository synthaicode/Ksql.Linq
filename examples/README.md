# Examples (Unified Guide)

- Start with `examples/index.md` for "Shared Prerequisites" and "How to Run".
- Then follow the category links from `examples/index.md`.

Quick start:
```
docker-compose -f tools/docker-compose.kafka.yml up -d
cd examples/basic-produce-consume
dotnet run
```

# Examples Index (Unified)

This folder lines up sample projects so ordinary developers can jump straight to what they need. Meet the prerequisites, then follow the trail that speaks to you. Keep the baton. Follow every rule. Confirm shared context or expect chaos.

## Shared Prerequisites (run once)
- Install the .NET 8 SDK
- Start local Kafka + Schema Registry + ksqlDB
  - `docker-compose -f tools/docker-compose.kafka.yml up -d`

## How to Run (common)
- Example: `basic-produce-consume`
  - `cd examples/basic-produce-consume`
  - `dotnet run`
- Each example ships with its own `appsettings.json` (or see its README)

---

## Basics (first touch)
- `basic-produce-consume`: Producer/Consumer fundamentals. Send `BasicMessage` and receive with `ForEachAsync`.
- `hello-world`: Minimal setup—define a POCO, send, wait, and receive (all in `Program.cs`).

## Configuration (settings & attributes)
- `configuration`: Minimal appsettings.json and Builder setup (connection/Topic/Consumer/Producer).
- `configuration-mapping`: Switch logging settings between Development and Production.
- `schema-attributes`: How to use `[KsqlKey]`, `[KsqlDecimal]`, and `[KsqlTimestamp]`.
- `headers-meta`: Handling message headers and metadata.

## Query Basics (LINQ → KSQL)
- `query-basics`: Core LINQ → KSQL flow (introduces View/ToQuery).
- `query-filter`: Filtering with `.Where(...)`.
- `view-toquery`: Fundamentals of View/ToQuery.
- `table-cache-lookup`: Refer to a local cache via `[KsqlTable]`.
- `pull-query`: Issue Pull queries against materialized views.

## Windowing (time windows & aggregation | unified)
- `windowing`: Adds live aggregation (Push) and a 1-minute → 5-minute roll-up atop TUMBLING basics.
  - Unified from `examples/tumbling-live-consumer` and `examples/rollup-1m-5m-verify`.
- `continuation-schedule`: Continuation-based scheduling sample (use Tumbling(..., continuation: true) over a market schedule).
 - `bar-1m-live-consumer`: Monitor `bar_1m_live` directly with a dedicated POCO.

## Error Handling (operations & reprocessing)
- `error-handling`: Basics of OnError/Retry (introduces retry strategies).
- `error-handling-dlq`: Park invalid messages in a DLQ (`.OnError(ErrorAction.DLQ)`).
- `manual-commit`: Manage commits manually (autoCommit: false).
- `retry-onerror`: Retry patterns.

## Operations & maintenance
- `streamiz-clear`: Clear Streamiz local state caches (RocksDB). One-off maintenance tool. Run: `dotnet run --project examples/streamiz-clear/StreamizClear.csproj`.
- `runtime-events`: Observe runtime/diagnostic events and counters.
- `startup-warmup`: Read-only warmup of ksqlDB paths at app startup.
- `options-validation-timeouts`: Configure validation behavior and schema registration timeouts.
- `ksql-response-inspect`: Execute KSQL and inspect `KsqlDbResponse`.
- `custom-executor`: Implement and use a custom `IKsqlExecutor` (direct call demo).

## Advanced (verification & application)
- `daily-comparison`: Daily aggregation—import rates, roll up 1/5/60-minute data, then verify daily totals.
- `oss-bars-verify`: Verify OSS around bars.
- `deduprates-producer`: Produce de-duplicated rates.
 - `rows-last-assignment`: Deterministic last-value assignment patterns with traces.

---

## Reference Docs (click to open)
- OnModelCreating samples: `../docs/onmodelcreating_samples.md`
- Function/type mapping: `../docs/ksql-function-type-mapping.md`
- SQLServer → ksqlDB guide: `../docs/sqlserver-to-kafka-guide.md`
