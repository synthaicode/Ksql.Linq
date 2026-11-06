<!-- xid: 74E6605E1412 -->
<a id="xid-74E6605E1412"></a>

# K-R05: preserve producer cache ownership under concurrent acquisition

Approved basis: user2026-11-06 request to choose next simple remaining finding, reproduce/test and commit/push if valid. K-R05 is implementation_local: lookup/create/indexer publication can overwrite a concurrently created holder, which manager disposal cannot enumerate.

Difference: serialize producer-cache lookup, initial generation/publication and existing value-only->keyed replacement for both default Type cache and explicit normalized topic cache. Use a shared manager ownership lock; coordinate Dispose against publication and reject acquisition after disposal if necessary to avoid publishing unowned holders. Preserve cache keys, topic normalization, existing public API, send behavior and retry policy. Do not claim safe concurrent send vs disposal or keyed migration of already in-flight sends. Preserve retry on failed creation rather than permanently caching a failed factory.

Targets: src/Messaging/Producers/KafkaProducerManager.cs; tests/Messaging/ProducerCacheOwnershipRegressionTests.cs; this instruction; docs/diff_log/diff_producer_cache_ownership_20261106.md.

Test design: reproduce parallel first acquisition on real GetProducerAsync default and explicit-topic paths, same-key holder identity/cache count, sequential reuse and different-key controls, upgrade if practical, disposal ownership/after-dispose guard if changed. Prefer existing logging seam for a bounded creation pause or actual local native producer construction with no sends, not a new public test hook. Capture expected red before product changes then same green cases. If reproduction cannot be reliable, surface evidence limitation before final approval. Clean up every actually created holder including baseline losers. Full unit/CLI/cache and L3 guard, net8/net10 Release builds serially. Independent QA before commit/push.

Excluded: producer send/Dispose coordination, delivery guarantees, eviction/window/cancellation/SQL/key-wire issues and existing review unknowns, dependencies/API/version, Kafka service/profiler integration, main merge/release. Operational report distinguishes proven holder ownership loss from unmeasured native resource growth, frequency and exhaustion. Separate unresolved design-policy findings K-R06/K-C03 from confirmed defects. External traceability; temporary source comments omitted for autonomous remediation.
