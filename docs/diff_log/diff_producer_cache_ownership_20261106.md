# K-R05: coordinate producer cache ownership

Date2026-11-06. Basis: [approved difference/test plan](../../features/producer-cache-ownership/instruction.md#xid-74E6605E1412).

## Operational impact and correction

Concurrent first acquisition previously returned multiple holders for one cache key. Cache assignment could overwrite an earlier holder; manager disposal enumerates only retained entries. This violates explicit cache/disposal ownership, with potential extra native-resource lifetime; growth/exhaustion/permanent leakage was not measured. Source now serializes default and normalized-topic lookup/generation/publication/upgrades with a shared manager lock. SendAsync post-alignment safety upgrade reacquires through the canonical path. Dispose uses the same lock, and acquisitions after disposal reject rather than republish unowned holders.

The shared lock serializes different-key creation too. Existing dictionary types, keys, public API, delivery/retry behavior and disposal-exception order remain; no general concurrent send/disposal or reentrant-callback safety is claimed. Pre-existing upgrade failure cleanup behavior is not redesigned.

## Reproduction and validation

Unchanged source8cases:4fail4pass0skip;2failures were distinct holders on default/normalizedtopic races,2were acquisition-after-dispose. Same original8plus2supplemental concurrent keyedupgrade controls now10pass. Sequential reuse/differentkey and pre-native-build logging failure/retry controls are included. Upgrade controls verify canonical keyed holder/one old-stub disposal/cache1. Red identity failure occurs before cache assertions; cache overwrite/disposal omission is source-backed inference. Green executes the cache assertions.

Native producer construction without message sends; controlled logger rendezvous waits up to2seconds and assumes worker scheduling. No actual broker/Registry/profiler/resource-exhaustion experiment. Detailed logs/TRX retained locally review-artifacts/producer-ownership. Full main764pass27existing skips0fail; CLI4pass; Cache7pass (distinct775passes). Release L3 196pass2existing skips overlaps full. Library Release net8/net10 builds0warnings/errors. Runtime tests net8, net10 compile only. Independent acceptance pending.

## Deferred policy and scope

K-R06 failed-handler commit policy and K-C03 deprecated decimal configuration remain separate unresolved owner decisions. Other original findings and held unknowns unchanged. Prior verified fixes remain in ancestry; no main merge, package release, API/dependency/version change. Publicmain is not claimed to contain this feature branch.
