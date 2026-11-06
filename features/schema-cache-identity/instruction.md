<!-- xid: 3E7E37ADC4CF -->
<a id="xid-3E7E37ADC4CF"></a>

# K-C04: include schema identity in generated-record cache

Approved basis: user request on 2026-11-06 to select the next simple remaining finding, reproduce/test and commit/push if valid. Selected K-C04 (major): same source type with different schema name/namespace currently returns the first cached schema. Classification implementation_local, no contract or dependency tradeoff.

Difference: identify cache entries by actual source Type and effective Avro name/namespace, normalized using the existing null/whitespace override defaults. Preserve reuse for identical/equivalent input; distinct requested identities must return matching independent ISpecificRecord implementations. Avoid duplicate emitted CLR names when multiple variants are generated; names are internal, retain existing default naming where practical. Keep Lazy single generation per entry; guard shared module emission if needed for variants. Do not change Avro wire schema/default rules or other mapping caches.

Targets: src/Mapping/SpecificRecordGenerator.cs; tests/Mapping/SpecificRecordCacheIdentityRegressionTests.cs; this instruction; docs/diff_log/diff_schema_cache_identity_20261106.md.

Test design: before/after real Generate and ISpecificRecord.Schema reproduction; independent name-only/namespace-only/both changes, default->override and override->default, same-input reuse, null/empty/whitespace equivalent defaults, distinct source Type identity and parallel equivalent/different generation controls where practical. Assert schema identity/fields and usable Put/Get. Run red before source edit then green unchanged cases, full unit/CLI/cache suites, L3 guard and Release net8/net10 builds serially. Independent QA required before publication.

Excluded: other findings, decimal global configuration policy, mapping registry shape changes, schema field escaping, cache eviction, Kafka/profiler integration, API/dependency/version, main merge and NuGet release. Prior K-C02/K-R07/K-C05 fixes remain in branch ancestry. Original review unknowns remain held. External artifacts retain traceability; no temporary trace comments needed for this autonomous remediation.
