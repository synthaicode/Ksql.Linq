# Window ownership and untouched bucket retention

Basis: [approved scope](../../features/window-retention/instruction.md#xid-D5A49A1F43BE), findings K-R03 and K-R04.

## Behavior difference

- Manager acquisition and synchronous append now share an ownership lock with idle/flush removal. Removal checks the exact manager instance and preserves managers with pending messages. A concurrent append cannot become detached by a stale removal decision.
- Emission takes and seals one closed bucket at a time. Cancellation is checked before taking the next bucket. A selector error, exhausted emission retry, or cancellation leaves later unattempted buckets available to a subsequent sweep.
- Flush retains open buckets, including messages appended while an emission callback is awaiting completion. It releases an empty manager after successful processing.

## Preserved behavior and limits

The attempted bucket remains sealed and removed before selector/emission work, as before. This change does not introduce failed-bucket replay or exactly-once delivery. Three emission attempts, optional failure handling, duplicate filtering and late arrival filtering retain their existing policy. Explicit `StopWithoutFlushAndClear` remains destructive. Disposal does not promise delivery of windows whose event-time closing boundary has not arrived.

The ownership lock covers synchronous acquisition, append and removal only; result selectors, emission callbacks and awaits execute outside it. The synchronous deduplication selector still executes within AddMessage under the ownership lock. Synchronous appends for different keys now share this lock. No public API, package version, dependency or release change is introduced.

## Verification handoff

`tests/Window/WindowRetentionRegressionTests.cs` exercises real aggregator paths. The original nine cases were handed to the parent executor before source changes for baseline red verification. Two supplemental cases cover future pending data during idle cleanup and concurrent sweeps. The parent owns serial build/test execution and recorded outcomes; independent QA owns acceptance.

Observed parent verification: original9red6fail3pass; original9plus2supplemental controls11green. Fullmain775pass27existing skips;CLI4pass;Cache7pass;distinct786passes. ReleaseL3 207pass2existing skips overlapsfull. LibraryRelease net8/net10 builds0warnings/errors. Runtime net8 only;net10compileonly. Logs/TRX locally review-artifacts/window-retention; independent QA in governing XRefKit work/reviews/2026-11-06_ksql_window_retention_fix_quality.md.
