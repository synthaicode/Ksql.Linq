# K-R02 cancellation and K-C06 key input validation

Basis: [approved scope](../../features/cancellation-key-validation/instruction.md#xid-C5590464B3D6); continuing repair authorization and explicit user choice to reject NUL components while preserving normal internal key representation.

## Operational behavior

Producer alignment now awaits Registry asynchronously with caller cancellation for normal, recovery and Delete paths. Cancellation is checked before acquisition/request, after waits and before mapping mutation/send. Thirdparty requests do not expose tokens; WaitAsync stops the caller wait, without claiming network abortion. Previously completed mapping work is not rolled back.

Cache reads propagate caller tokens through ReadCachedEntitySet, ITableCache and TableCache running waits, migration retry delay, enumeration and successful return checks. Original two-parameter ToListAsync remains available for callers/reflection; token overload and default interface fallback support legacy implementations. OCE cannot be mistaken for migration retry by message text. Legacy underlying work may continue after caller wait cancellation.

Shared CompositeKeyEncoding rejects NUL in each input component before unchanged delimiter join. Mapping Generic/Specific/fallback, TableCache filters and MarketSchedule index/query use it. Normal key bytes, conversion, null/empty slots and window suffix remain unchanged; decoder/migration behavior not redesigned. Existing ambiguous stored keys are not recoverable by this validation.

## Evidence

Same42cases: originalsource25fail17pass; corrected42pass0fail0skip. Testscoverkey/value delayed Registry normal/recovery/Delete; precancelwithoutacquisition; delayedresponse nomappingupdate/nosend; cachewait/retry/legacyreflection; originalconflictingtuples and normalkey/window/schedule controls. Logs/TRX locallyreview-artifacts/cancellation-key. Fullmain817pass27existing skips;CLI4pass;Cache7pass;distinct828passes. ReleaseL3249pass2existing skips overlapsmain; libraryRelease net8/net10 builds0warnings/errors. Runtime net8only;net10compileonly. IndependentQA pending.

## Scope limits

No realKafka/Registry/profiler abort experiment, no newdelivery/replay/failedhandlercommit/decimalconfiguration policy. No publicAPI/dependency/version/release/mainmerge. Cache legacy2params preserved; this branch includes priorwindow/producer/input fixes. RemainingSQLfalsepositive andwindowwire issues are separate.
