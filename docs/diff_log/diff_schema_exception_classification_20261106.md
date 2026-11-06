# K-R01: require actual schema exceptions for recovery

Date:2026-11-06. Basis: [approved difference and test design](../../features/schema-exception-classification/instruction.md#xid-E402AC4CD783).

## Condition and correction

The producer previously matched arbitrary exception.ToString text such as incompatible or NAME_MISMATCH, allowing unrelated failures to trigger Schema Registry alignment and resend. IsSchemaMismatch now requires an actual Confluent.SchemaRegistry.SchemaRegistryException node. Existing case-insensitive markers are checked against that node Message only; exactly ErrorCode409 remains accepted as the semantic equivalent of the pre-existing error code:409 ToString marker. Other numeric codes or409xx ranges are not added. InnerException and AggregateException children are traversed without mixing text from other nodes.

## Verification

On unchanged source, corrected23case regression:14expected false-positive failures9passing controls. After correction, same23pass. Covers six markers, direct/wrapped typed controls, unrelated outer/sibling wording, exact409 rendering/direct/wrapped controls, neutral403/500 and actual manager SendAsync with controlled producer+mocked Registry. Normal pre-send alignment makes2lookups in both cases. Unrelated failure retains the identical original exception with1send/2lookups; typed mismatch succeeds after2sends/4lookups (2normal+2recovery).

Full main754passed27existing skips0fail; CLI4passed; Cache7passed (distinct765passes). Release L3 186passed2existing skips overlaps main. Release library builds net8/net10:0warnings/errors. Tests execute net8; net10 compile only. Detailed logs/TRX retained locally in review-artifacts/schema-exception; independent QA recorded in governing XRefKit work/reviews/2026-11-06_ksql_schema_exception_fix_quality.md.

## Boundary

Only classifier changed; no new recovery code range, retry/delivery/cancellation policy, API/dependency/version, main merge or release. Controlled components are not real Kafka/Schema Registry/serializer integration or profiler injection. Existing one-shot retry bound remains unchanged; broader delivery correctness is not established. Other original findings/unknowns remain open; prior verified fixes are retained in branch ancestry.
