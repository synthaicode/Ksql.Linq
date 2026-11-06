# K-C05: reject malformed Guid string keys

Approved basis: user instruction of 2026-11-06 to fix K-C05, reproduce, test, commit and push if valid. Classification: implementation_local; no conflicting finding or dependency decision.

Difference: FromKeyString must throw FormatException for a nonempty malformed Guid instead of silently returning Guid.Empty. Preserve Guid.Parse-compatible valid formats, a genuine zero Guid, nullable Guid, and existing null/empty-key value fallback and tombstones. The parse call is outside the setter catch in CombineFromStringKeyAndAvroValue, so no exception-policy redesign is needed.

Targets: src/Mapping/KeyValueTypeMapping.cs; dedicated tests/Mapping/GuidStringKeyValidationRegressionTests.cs; this instruction; docs/diff_log/diff_guid_key_validation_20261106.md.

Test design: exercise real MappingRegistry and CombineFromStringKeyAndAvroValue; malformed/truncated/whitespace Guid and nullable Guid must throw; valid formats, zero, payload preservation, null/empty fallback and tombstone controls must pass. Run tests against unchanged implementation first, then repeat after the minimal fix. Run full unit/CLI/cache suites and net8/net10 builds serially. Independent QA checks scope, exception propagation and evidence before commit/push.

Excluded: delimiter encoding K-C06, setter exception swallowing, DLQ/commit policy K-R06, public API/dependencies/version, Kafka/profiler testing, merge and release. Mapping-boundary component tests do not prove end-to-end delivery policy. Original review unknowns remain open.
