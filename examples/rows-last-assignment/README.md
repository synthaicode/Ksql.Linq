# rows-last-assignment (Assigned/Revoked-driven ingestion)

## Purpose

This sample demonstrates how **consumer assignment and revocation** drive ingestion into a `rows_last` table and 1-minute bars.

- When partitions are **Assigned**, the row monitor starts ingesting source rows into `rows_last` and downstream bar topics.  
- When partitions are **Revoked**, the row monitor stops ingestion (Flush-None style), leaving the last known snapshot for each key.

---

## Prerequisites

- .NET 8 SDK  
- Docker with Kafka / ksqlDB / Schema Registry running
  - You can reuse the `physicalTests` docker-compose environment:
    - `curl -s http://127.0.0.1:18088/info` → ksqlDB `RUNNING`
    - `curl -s http://127.0.0.1:18081/subjects` → returns JSON

---

## Model Overview

- **Source stream**
  - `Rate` (`deduprates`): 1-second tumbling window + `GroupBy(BROKER, SYMBOL)` feeding 1s rows.
- **Bars**
  - `Bar` (`bar_1m_live`): 1-minute OHLC bars (`OPEN/HIGH/LOW/KSQLTIMEFRAMECLOSE`).
- **Row monitor / rows_last**
  - Row monitor listens to assignment/revocation events and controls ingestion.  
  - `rows_last` CTAS uses `LATEST_BY_OFFSET` + `GROUP BY` to materialize a **compact TABLE** of the latest 1s row per key.  
  - Schema Registry key/value schemas are aligned so it is easy to debug KEY/VALUE subjects.

---

## How to Run

1. Start the sample:
   ```bash
   dotnet run --project examples/rows-last-assignment -c Release
   ```
2. You should see logs similar to:
   - `Row monitor assignment observed for topic deduprates`
   - `Row monitor runId=... consuming sourceTopic=deduprates targetTopic=bar_1s_rows ...`
   - `Aligned KEY schema ... fields=[...]` / `Aligned VALUE schema ... fields=[...]`
3. To simulate assignment/revocation:
   - Start a second instance (or another consumer with the same group) to trigger a rebalance.  
   - On the previously active instance you should see:  
     - `revocation ... stopping consumption` (ingestion stops)  
   - On the new active instance you should see:  
     - `assignment observed ... consuming ...` (ingestion resumes)

---

## ksqlDB Queries (CLI / REST)

- Check `rows_last` presence (T1):
  ```sql
  SELECT 1
  FROM BAR_1S_ROWS_LAST
  WHERE BROKER='B1' AND SYMBOL='S1'
  LIMIT 1;
  ```

- Check 1-minute bars (T2):
  ```sql
  SELECT BROKER,
         SYMBOL,
         BUCKETSTART,
         OPEN,
         HIGH,
         LOW,
         KSQLTIMEFRAMECLOSE
  FROM BAR_1M_LIVE
  WHERE BROKER='B1' AND SYMBOL='S1'
  LIMIT 5;
  ```

---

## Notes

- When partitions are **revoked**, ingestion stops but the `rows_last` snapshot remains. This models **Flush-None** behavior and is useful for deterministic last-value assignment patterns.  
- `rows_last` is a **compact TABLE**; it is intended as the canonical “latest tick” view per key and a stable input for comparisons or downstream analytics.

