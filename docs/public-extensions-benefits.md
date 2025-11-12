## Public Extensions: Benefits Overview

Purpose: Summarize why and when to use each public extension point/API not fully covered by examples.

### IKsqlExecutor (WithKsqlExecutor)
- Cross-cutting control: centralize retries, circuit breakers, backoff, and logging around KSQL calls.
- Observability: inject correlation IDs, structured logs, metrics without touching call sites.
- Resilience policies per environment: A/B test executor strategies (dev/stage/prod).

### ITopicAdmin (WithTopicAdmin)
- Governance: enforce topic naming, partition/replication policies, and configs.
- Safety: dry-run or guard-rails before destructive ops (delete/alter).
- Audit: centralize admin actions with change logs.

### IProducerFactory / IConsumerFactory
- Security: plug SASL/OAUTH credentials and rotation hooks.
- Performance tuning: standardize linger/batch/acks/auto.offset.reset per workload.
- Testing: inject fakes for deterministic unit/integration tests.

### IDlqService (WithDlqService)
- Unified failure handling: route poison messages to centralized DLQ with metadata.
- Operability: add dedupe, retention, and routing (by entity/tenant/severity).
- Compliance: attach incident IDs, PII scrubbing, and evidence links.

### IRowMonitorCoordinator (WithRowMonitorCoordinator)
- Live readiness: coordinate background checks for rows_last/hub streams.
- Alerting: push runtime events to sinks (console, Prometheus, APM) consistently.
- Throttling: gate workloads until critical feeds are ready.

### IMarketScheduleService (WithMarketScheduleService)
- Business alignment: inject trading/market calendars to gate processing windows.
- Safety: avoid processing during maintenance/holidays automatically.
- Flexibility: swap schedules by region/venue without code changes.

### ITableCacheManager (WithTableCacheManager)
- Latency: local lookup caches for hot tables to avoid ksqlDB roundtrips.
- Resource control: explicit cache lifecycle (clear, rebuild, warmup).
- Isolation: partition caches per tenant/app domain.

### IStartupFillService (WithStartupFillService)
- Predictable startups: read-only warmup to stabilize first access.
- Custom checks: verify dependencies without emitting synthetic records.
- Extensibility: compose environment-specific probes (tables/streams).

### KsqlContextBuilder.ConfigureValidation / WithTimeouts
- Fast feedback: toggle auto-registration and failure policy during bootstrap.
- Safe defaults: lengthen schema registration timeouts in slow environments.
- Parity: match CI/local vs. prod posture via configuration, not code.

### KsqlDbResponse (direct)
- Diagnostics: access raw KSQL body/status for triage.
- Tooling: log/pretty-print/attach to incident reports.
- Extensibility: branch on response content when building orchestration flows.

### IWaitDiagnosticsSink
- Traceability: persist SHOW QUERIES raw outputs for postmortems.
- Reproducibility: capture evidence when matching fails on mixed formats.
- Hygiene: rotate/store logs under app-controlled paths.

