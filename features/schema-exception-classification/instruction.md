<!-- xid: E402AC4CD783 -->
<a id="xid-E402AC4CD783"></a>

# K-R01: require typed schema-error evidence for recovery

Approved basis: user instruction2026-11-06 to select next simple remaining finding, reproduce/test, commit/push if valid. Selected major K-R01, implementation_local: existing source comment requires contained SchemaRegistryException but implementation searches arbitrary exception.ToString.

Difference: search exception/InnerException chain (and AggregateException children if supported) for actual Confluent.SchemaRegistry.SchemaRegistryException; apply existing case-insensitive schema mismatch message markers to that typed exception's own Message, not unrelated outer text or stack traces. Preserve typed direct/wrapped mismatch recovery; non-schema exceptions containing identical words must retain original failure without recovery lookup/resend. Preserve exact typed ErrorCode==409 as the semantic equivalent of the existing error code:409 ToString marker; do not add any other code or409xx range, broaden recovery semantics, or change retry/delivery policy. Typed errors with other codes and without existing markers remain nonmatches.

Targets: src/Messaging/Producers/KafkaProducerManager.cs classifier; tests/Messaging/SchemaMismatchClassificationRegressionTests.cs (or matching tests/Producers placement); this instruction; docs/diff_log/diff_schema_exception_classification_20261106.md.

Test design: test actual exception type from installed package; same marker text in schema and unrelated exception controls, wrapped matches, irrelevant wrapper wording surrounding nonmatching typed exception, aggregate siblings if traversed; test SendAsync with controllable ProducerHolder and fake Registry to assert unrelated failure preserves same exception with no recovery lookup/resend versus typed mismatch recovery path. First tests red against unchanged source, then green unchanged cases. Full main/CLI/cache, L3, Release net8/net10 builds serially. Independent quality acceptance before commit/push.

Excluded: K-R02 cancellation, producer concurrency, SQL safety, key/wire formats, all other findings/held unknowns, API/dependencies/version, Kafka/profiler integration, main merge and release. Prior verified fixes remain in ancestry. Traceability external; no temporary source comments required for autonomous remediation.
