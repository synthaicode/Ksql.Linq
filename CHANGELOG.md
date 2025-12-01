# Changelog

## v1.0.0
- Packaged the AI Assistant Guide alongside the library/CLI and pointed to `docs/releases/release_v1_0_0.md` for the release story.
- Added the CLI `dotnet ksql ai-assist` command (with `--copy`) so users can immediately send the guide text to assistants.
- Localized the `ai-assist` header/footer text across the supported regions to match the UI culture.
- CI now generates `AI_ASSISTANT_GUIDE.md` for both the library and CLI, verifies it lands in the package, and publishes matching versions.

## v0.9.8
- Hardened DLQ defaults so minimal configurations fall back to `dead_letter_queue` and avoid startup failures.
- Aligned the Tumbling/TimeBucket physical tests and examples with the Wiki's WindowStart policy so bucket timestamps stay logical and clear.
- Tidied `physicalTests/OssSamples` by keeping only the compiled tests at the root, archiving the rest, and documenting the active suite in `physicalTests/OssSamples/README.md`.

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
