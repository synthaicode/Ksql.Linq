# K-C04: generated-record cache includes schema identity

Date: 2026-11-06. Basis: [approved scope and test design](../../features/schema-cache-identity/instruction.md#xid-3E7E37ADC4CF).

## Condition and correction

Generating records for the same source type with different schema name/namespace overrides previously returned the first cached implementation. The cache key now contains the actual source Type and the effective schema name/namespace. Existing null/empty/whitespace defaults are normalized, so equivalent inputs still reuse a Lazy-generated type. Same-named types from distinct assemblies retain their own shape.

All generated variants share a module. Emission is serialized and every CLR full name is reserved, with collision-checked suffix allocation when a name is already occupied. This also prevents a generated suffix name colliding with a later source type. The previous CLR base name is retained when available. Avro schema field/default construction is unchanged.

## Reproduction and verification

Before modification: nine real Generate/ISpecificRecord component tests, eight expected identity failures and one passing equivalent-default control. After modification: the same nine cases plus one additional CLR-name collision control all pass. Tests check both request orders, independent name/namespace inputs, cache reuse, valid schema fields, Put/Get, different assembly Types with identical FullName, and 64 parallel requests across four identities. The added collision control was not in the original red run.

Full unit suite:731 passed,27 existing skips,0 failed; CLI:4 passed; Cache:7 passed. Distinct suite passes742. Release L3:163 passed,2 existing skips (overlaps full suite). Release library builds net8.0/net10.0:0 warnings/errors. Tests ran on net8.0; net10.0 compile only. Detailed logs/TRX are retained locally under review-artifacts/schema-cache; independent QA is recorded in the governing XRefKit work/reviews/2026-11-06_ksql_schema_cache_fix_quality.md.

## Boundary

This is an internal cache correction, without API/dependency/version or Avro wire-schema policy changes. No Kafka, Schema Registry service, Streamiz integration, profiler injection, main merge or release was performed. Original review unknowns and unrelated findings remain open. Branch ancestry includes the previously verified K-C02/K-R07/K-C05 fixes; this new difference addresses K-C04 only.
