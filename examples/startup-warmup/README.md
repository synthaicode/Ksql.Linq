## startup-warmup (read-only)

- Purpose: Warm up ksqlDB read paths at startup without emitting any records.
- Actions: SHOW STREAMS check, Pull count on tables, Push count (LIMIT 1) on streams.

## Run
```
cd examples/startup-warmup
# optional: edit appsettings.json (ksqlDB URL); default is http://localhost:8088
dotnet run
```

## Configure targets
- Edit `Program.cs` and set `tables` / `streams` arrays.
- Or wire your own discovery and pass them into `WarmupStartupFillService`.

## Integrate
- You can call `WarmupStartupFillService` during your host startup.
- It respects cancellation and logs at Information/Debug levels.

