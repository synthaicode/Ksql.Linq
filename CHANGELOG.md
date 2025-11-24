# Changelog

## v0.9.8
- Fixed DLQ defaults for appsettings-lite scenarios: `GetDlqTopicName()` and `KafkaAdminService.EnsureDlqTopicExistsAsync()` now fall back to `dead_letter_queue` when `DlqTopicName` is unset (e.g., when `appsettings.json` is not loaded or does not define `DlqTopicName`).
- Corrected physical Tumbling+TimeBucket tests to avoid double timestamp semantics: `TranslationsTimeBucketTests` now uses `BucketStart` as a logical column without `[KsqlTimestamp]`.
- Cleaned up examples to avoid recommending `WindowStart()` as a projected value column for Tumbling windows; examples now focus on OHLC and continuation semantics instead.
- Clarified OssSamples physical test scope by moving non-active tests into `OssSamples/Archive` and documenting the three active physical tests.

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
