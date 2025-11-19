# Designtime Tumbling KSQL (sample)

Purpose: show how to define a Tumbling-window aggregation via ToQuery on a `KsqlContext` that is intended for design-time KSQL script generation.

## What this covers
- `TumblingKsqlContext` : a `KsqlContext` with `Tick` (source) and `MinuteBar` (Tumbling view).
- Tumbling-window aggregation using `Tumbling(...)`, `GroupBy`, and window helpers like `WindowStart()`.
- `TumblingDesignTimeKsqlContextFactory` : an `IDesignTimeKsqlContextFactory` implementation for offline tooling.

## How this is intended to be used
- Tooling (e.g. a future `dotnet ksql script` CLI) will:
  - Build this project (or your app project).
  - Load the resulting DLL.
  - Locate `TumblingDesignTimeKsqlContextFactory`.
  - Call `CreateDesignTimeContext()` to obtain a `TumblingKsqlContext`.
  - Inspect the model and emit KSQL scripts, including the Tumbling-window view.

At design time:
- Kafka/ksqlDB do not need to be running.
- Focus is on the model and KSQL generation, especially the Tumbling-window query structure.

## Run (optional)

```bash
cd examples/designtime-ksql-tumbling
dotnet run
```

You should see a short message describing the Tumbling design-time context.

## Related samples
- `examples/designtime-ksql-script`: basic design-time KSQL from a simple context.
- `examples/continuation-schedule`: runtime sample for continuation-aware Tumbling windows.

