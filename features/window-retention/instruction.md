<!-- xid: D5A49A1F43BE -->
<a id="xid-D5A49A1F43BE"></a>

# K-R03/K-R04: retain reachable window data

Approved basis: user request2026-11-06 to continue remaining repairs, prior explicit reproduce/test/independent-check/commit/push authorization retained. Both source-confirmed major findings are implementation_local where correction preserves existing policy.

K-R03 difference: coordinate manager acquisition+append with idle check+conditional same-instance removal, so a completed append cannot be detached by stale idle eviction. Check similar Flush removal so concurrent/new pending windows are not discarded; intentional StopWithoutFlushAndClear remains explicit destructive revocation behavior. Avoid holding monitor across await; no new public API.

K-R04 difference: take/remove only the next closed bucket immediately before attempting it, not all closed buckets up front. Check cancellation before taking next bucket. Preserve existing behavior of the attempted failed bucket, existing retries, success failure-handler behavior and late/dedup sealing. Unattempted later buckets remain available for a subsequent sweep. No exactly-once or failed-bucket replay policy invented.

Targets: src/Window/WindowAggregator.cs, src/Window/WindowManager.cs and WindowAppendStatus only if essential; tests/Window/WindowRetentionRegressionTests.cs; this instruction; docs/diff_log/diff_window_retention_20261106.md.

Test design: real manager/aggregator bounded concurrent append/eviction reproduction (existing seam or lock scheduling; avoid copied algorithm); callback3failure, selectorfailure, cancellation after firstsuccess leave later bucket available after recovery; success and handledfailure controls, normal idlecleanup, late/dedup, concurrent sweeps/flush controls as practical. Tests first red on unchanged source, then unchanged green. Preserve actual controls/counts and scheduling limits. Full main/CLI/cache, L3; buildRelease net8/net10 serially. src/Window is not src/Runtime/Hopping or Query, so Hopping physical tests not implied; no external Kafka/profiler claim.

Excluded: K-R06 commit/DLQ policy and K-C03 deprecatedconfig decisions, failed-attempt replay semantics, end-to-enddelivery, concurrency with intentional destructive stop, wirekey compatibility/SQLsecurity/cancellation other findings, version/dependencies/API/release/mainmerge. Keep source findings and runtime observations distinct. Independent QA and operational-impact-first report before publication.
