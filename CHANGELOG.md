# Changelog

## Unreleased
- Interface change: `KsqlContext.StartAsync` now executes deferred startup I/O instead of being a no-op. Behavior is unchanged for existing code: by default the constructor still runs schema registration/materialization, table-cache wiring, and startup fill (fail-fast preserved).
- Added `KsqlDslOptions.DeferStartup` (default `false`). When `true`, the constructor performs no startup I/O; call `await context.StartAsync(ct)` explicitly. Startup failures then surface from `StartAsync` (cancellable, retry-able, and the context remains disposable), which avoids sync-over-async blocking in the constructor for SynchronizationContext-bound hosts and DI composition.
- `Set<T>()` / `GetEventSet()` now throw `InvalidOperationException` with guidance when called before `StartAsync` completes on a deferred-startup context.

## v1.1.0
- Adds Hopping window support and clarifies the Tumbling vs Hopping boundary (hub rows path, function compatibility handling).
- Strengthens AI Assist onboarding: request rules (Must/Should), Copilot agent-mode prompt template, and full/focused loading strategy guidance.
- Improves CLI help/README guidance so first-time users can start with `dotnet ksql ai-assist --copy` and understand version alignment expectations.

## v1.0.0
- Packaged the AI Assistant Guide alongside the library/CLI and pointed to `docs/releases/release_v1_0_0.md` for the release story.
- Added the CLI `dotnet ksql ai-assist` command (with `--copy`) so users can immediately send the guide text to assistants.
- Localized the `ai-assist` header/footer text across the supported regions to match the UI culture.
- CI now generates `AI_ASSISTANT_GUIDE.md` for both the library and CLI, verifies it lands in the package, and publishes matching versions.

## v0.9.8
- Hardened DLQ defaults so `GetDlqTopicName()` and `KafkaAdminService.EnsureDlqTopicExistsAsync()` fall back to `dead_letter_queue` whenever `DlqTopicName` is unset, preventing startup `ArgumentException`s.
- Aligned the physical Tumbling/TimeBucket surface with the WindowStart policy: `TranslationsTimeBucketTests` treats `BucketStart` as a logical column without `[KsqlTimestamp]`, while the examples now avoid projecting `WindowStart()` as a data column and focus on OHLC/continuation semantics.
- Tidied `physicalTests/OssSamples` by moving inactive tests into `Archive/`, documenting the active trio, and keeping only the compiled tests at the root so the physical suite stays obvious.

## v0.9.7
- Simplified KsqlDslOptions defaults: dropped DefaultValue attributes, rely on initializers.
- Wait settings now use KsqlDslOptions; removed KSQL_QUERY_RUNNING_* env fallbacks.
- Made DeserializationErrorPolicy and ReadFromFinalTopicByDefault init-only.
- PublicAPI baseline rolled into Shipped; Unshipped reset.

## v0.9.6
- Introduced runtime tuning options for query stabilization/warmup/http timeouts.
- Added comment/label driven release automation (tag promotion and publish workflows).
- Updated release documentation and PublicAPI baseline for the tuning changes.

## v0.9.5
- Design-time KSQL generation: create full KSQL scripts (CREATE STREAM/TABLE, CSAS/CTAS, SELECT …) from your KsqlContext without running Kafka/ksqlDB.
- Design-time Avro export: export value Avro schemas for all entities from the mapping model (no live Schema Registry required).
- Design-time factory: implement `IDesignTimeKsqlContextFactory` to create a special KsqlContext that skips runtime connections and focuses on the model.
- Ksql.Linq.Cli .NET tool: `dotnet ksql script` / `dotnet ksql avro` against a compiled DLL or project to generate SQL scripts and `.avsc` files. Published on NuGet as Ksql.Linq.Cli.
- Traceable scripts: generated KSQL includes comments with the Ksql.Linq version, target assembly name/version, generation timestamp, Schema Registry subject, and CLR namespace.
