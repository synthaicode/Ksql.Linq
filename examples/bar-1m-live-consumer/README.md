# bar-1m-live-consumer

Monitor `bar_1m_live` with a dedicated POCO, and optionally enable app-side startup backfill to fill missing 1-minute buckets.

How to run
- Start local stack: `docker-compose -f tools/docker-compose.kafka.yml up -d`
- Run: `dotnet run --project examples/bar-1m-live-consumer`

Backfill (optional)
- appsettings.json contains `KsqlDsl.Fill`:
  - `EnableAppSide`: true (turn on startup backfill)
  - `MaxBackfillBuckets`: 10 (per key)
  - `BackfillHorizonMinutes`: 60
- See `docs/wiki/appside-fill.md` for the policy and conditions.

Notes
- Fill writes synthesized rows to `<entity>_1s_rows` with header `x-fill:true` (design). ksqlDB re-aggregates `bar_1m_live`.
- If you don’t need backfill, set `EnableAppSide` to false.

rows_last (Pull) quick tips
- Latest across all keys:
  - `SELECT BROKER, SYMBOL, BUCKETSTART, CLOSE FROM BAR_1S_ROWS_LAST ORDER BY BUCKETSTART DESC LIMIT 1;`
- Existence for a specific key+bucket:
  - `SELECT 1 FROM BAR_1S_ROWS_LAST WHERE BROKER='X' AND SYMBOL='Y' AND BUCKETSTART='2025-10-17T12:34:00Z' LIMIT 1;`
- Program.cs includes small demos; customize key via env vars `DEMO_BROKER` / `DEMO_SYMBOL`.
