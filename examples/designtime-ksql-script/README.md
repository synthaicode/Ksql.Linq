# Designtime KSQL Script (sample)

Purpose: define a `KsqlContext` and design-time factory so tooling can generate KSQL scripts from the model without starting the full app.

## What this covers
- `OrdersKsqlContext` : a minimal `KsqlContext` with `OrderEvent` and a ToQuery-based `OrderSummary` view.
- `OrdersDesignTimeKsqlContextFactory` : an `IDesignTimeKsqlContextFactory` implementation for design-time use.
- `appsettings.json` : `KsqlDsl` configuration that controls topics per entity (`Entities` セクション)。

## How this is intended to be used
- Tooling (e.g. a future `dotnet ksql script` CLI) will:
  - Build this project (or your app project).
  - Load the resulting DLL.
  - Locate `OrdersDesignTimeKsqlContextFactory`.
  - Call `CreateDesignTimeContext()` to obtain an `OrdersKsqlContext`.
  - Inspect the model and emit KSQL scripts (CREATE STREAM/TABLE, CSAS/CTAS, etc.).

At design time:
- Kafka/ksqlDB do not need to be running.
- The focus is on the model and KSQL generation, not on executing the queries.

## Run (optional)
```bash
cd examples/designtime-ksql-script
dotnet run
```

You will see a design-time KSQL script printed, including `CREATE STREAM/TABLE` and `WITH` clauses.

### See how DDL changes with appsettings.json

`appsettings.json` contains per-entity configuration:

```json
"KsqlDsl": {
  "Entities": [
    { "Entity": "OrderEvent", "SourceTopic": "orders_v1" },
    { "Entity": "OrderSummary", "SourceTopic": "orders_summary_v1" }
  ]
}
```

Run `dotnet run` and note the `WITH (KAFKA_TOPIC='orders_v1', ...)` and `orders_summary_v1` entries.
Then change `SourceTopic` (for example `orders_v2`) and run again to see the DDL update before touching any runtime.

## Related docs
- Design-time KSQL plan: `../../docs/designtime_ksql_script_plan_v0_9_5.md`
- appsettings/retention plan: `../../docs/designtime_ksql_script_plan_v0_9_5_appsettings_retention.md`
- Workflow: `../../docs/workflows/designtime-ksql-script-workflow.md`
