# Physical Tests - Environment Readiness

This test harness runs Kafka, Schema Registry, and ksqlDB via Docker Compose and then executes integration tests.

Key points about readiness and waiting:

- Why waits are in tests (not OSS):
  - ksqlDB can return HTTP 500 from /ksql or be not fully ready immediately after container start.
  - To keep the OSS library free of environment-specific waits, readiness waits live in the test harness only.

- ksqlDB healthcheck:
  - docker-compose declares a healthcheck for ksqldb-server using /info. Downstream services (cli, runner) depend on service_healthy.

- Runner waiting sequence:
  - Wait until both Schema Registry (/subjects) and ksqlDB (/info) return HTTP 200, three times consecutively.
  - Warm up ksqlDB by calling SHOW QUERIES until responses show RUNNING three times consecutively.
  - Sleep an extra 180s to absorb initial churn.

- Streamiz/State Store notes:
  - Tests query local state stores via Streamiz. If a query hits during thread transitions (e.g., PENDING_SHUTDOWN), the harness relies on built-in retries/timeouts.
  - Data absence is a valid application scenario; tests should not attempt to “fill” data—WhenEmpty or application logic governs that.

If tests are flaky on constrained machines, increase the number of consecutive successes or the settle wait window in docker-compose runner entrypoint.
## Running Tests

The intent of the physical test suite is to validate ksqlDB behavior against a clean single-node stack. Because some tests modify persistent tables/streams, always recycle the stack before each run.

1. From physicalTests/, reset the environment:
   `powershell
   pwsh -NoLogo -File .\reset.ps1
   `
   This issues docker compose down -v followed by docker compose up -d and waits for Kafka, Schema Registry, and ksqlDB to report healthy.
2. Run the desired tests (examples):
   - Single test class:
     `powershell
     dotnet test Kafka.Ksql.Linq.Tests.Integration.csproj -c Release --filter "FullyQualifiedName~BarScheduleDataTests"
     `
   - Selected OssSamples set:
     `powershell
     dotnet test Kafka.Ksql.Linq.Tests.Integration.csproj -c Release --filter "FullyQualifiedName~(BarDslExplainTests|BarDslMultiTierTests)" --logger "trx;LogFileName=ossamples.trx" --results-directory ..\reports\physical
     `
3. Before running another test set, repeat step 1 to avoid CREATE OR REPLACE conflicts on _live tables.

Logs (docker, ksqlDB, test TRX) are written under physicalTests/ and eports/physical/. Include the TRX and relevant container logs when reporting failures.

