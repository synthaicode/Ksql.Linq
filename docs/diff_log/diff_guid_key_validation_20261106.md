# K-C05: reject malformed Guid string keys

Date: 2026-11-06. Basis: [approved scope and test design](../../features/guid-key-validation/instruction.md).

## Condition and change

With a nonempty malformed Guid string key and a non-tombstone Avro value, entity reconstruction previously returned a normal-looking Guid.Empty key. FromKeyString now uses Guid.Parse so FormatException propagates through CombineFromStringKeyAndAvroValue. Nullable Guid uses the same validated path. Only this Guid parsing branch changed.

## Compatibility and verification

The same 30 component regression cases exercise the real MappingRegistry/GenericRecord reconstruction boundary. Before: six malformed-input rejection failures and 24 passing controls; after: all 30 pass. Controls preserve supported D/N/B/P/X formats, valid surrounding whitespace, genuine zero Guid, nullable Guid, payload/count values, null/empty-key value fallback, and tombstones.

Full unit suite: 721 passed, 27 existing skips; CLI: 4 passed; Cache: 7 passed; zero failures. L3 guard: 153 passed, 2 existing skips (overlaps unit suite). Release builds for net8.0 and net10.0: zero warnings/errors. Tests executed on net8.0; net10.0 was compile-validated. Red/green/full TRX and logs are retained locally under review-artifacts/guid-key; independent quality evidence is held in the governing XRefKit work/reviews/2026-11-06_ksql_guid_key_fix_quality.md.

## Boundaries

No API, dependency, version, delimiter encoding, DLQ/commit policy or release change. Kafka/profiler and end-to-end delivery were not tested; this change is validated at the mapping boundary. Other original review findings/unknowns remain open. This branch includes the previously verified K-C02/K-R07 base commit ecbfd6ff; the new difference is K-C05 only. Main merge and package release are outside this instruction.
