# Release Notes v0.9.8

## Highlights

- Hardens DLQ defaults so applications using minimal `appsettings.json` still start safely.
- Aligns Tumbling + TimeBucket physical tests and examples with the Wiki’s WindowStart policy.
- Clarifies which OssSamples tests are part of the physical test suite vs. archived samples.

---

## 1. DLQ topic name fallback

### Background

- Some physical tests and minimal samples construct `KsqlDslOptions` directly via `new KsqlDslOptions { ... }`
  without binding from `appsettings.ksqldsl.json`.  
  In that path, `DlqTopicName` could remain empty, leading to:
  - `DlqEnvelope.TopicName == ""`
  - `KafkaAdminService.CreateDbTopicAsync("")`
  - `ArgumentException("Topic name is required")` during startup.

### Changes

- `KsqlContext.GetDlqTopicName()`:
  - If `DlqTopicName` is null/empty/whitespace, it now falls back to `"dead_letter_queue"`.

- `KafkaAdminService.EnsureDlqTopicExistsAsync()`:
  - Uses the same fallback when `_options.DlqTopicName` is empty, so DLQ topic checks and
    auto-creation always use a valid name.

### Impact

- Apps that already configure `DlqTopicName` in `appsettings.ksqldsl.json` behave **unchanged**.
- Physical tests and minimal samples without explicit `DlqTopicName` now get a safe default
  (`dead_letter_queue`), avoiding startup failures.

---

## 2. Tumbling + TimeBucket physical tests

### TranslationsTimeBucketTests

- Problem:
  - `Rate.Timestamp` had `[KsqlTimestamp]`, and `Xform.BucketStart` also had `[KsqlTimestamp]`
    while projecting `BucketStart = g.WindowStart()`. This created ambiguous “double timestamp”
    semantics for Streamiz and TableCache.

- Fix:
  - Removed `[KsqlTimestamp]` from `Xform.BucketStart`; it is now treated purely as a logical
    window-start column.
  - Kept `WindowStartRaw` (long) and the assertion logic that compares it against `t0`
    for TimeBucket verification.
  - After the change, `TranslationsTimeBucketTests.Translations_Minimal_Canonical` passes
    as an end-to-end physical test.

### Diagnostics

- Adjusted `PhysicalTestLog` filters so `Streamiz.Kafka.Net.Processors` debug logs are suppressed
  from console output (Information+ only), making physical test diagnostics easier to read.

---

## 3. Tumbling + WindowStart example cleanup

### Wiki alignment

- The Wiki (`Tumbling-Definition.md`) describes the **WindowStart policy**:
  - `g.WindowStart()` corresponds to a bucket concept but is **not injected as a value column**
    in CTAS; window boundaries are reconstructed from the windowed key.
  - For pull queries that specify window boundaries, use `WHERE WINDOWSTART=<millis>` rather than
    projecting WindowStart as a value column.

### Updated examples

- `examples/designtime-ksql-tumbling/Program.cs`
  - Removed `MinuteBar.BucketStart` property.
  - Removed `BucketStart = g.WindowStart()` from the Tumbling `Select`.

- `examples/continuation-schedule/Program.cs`
  - Removed `Bar.BucketStart` property.
  - Removed `BucketStart = g.WindowStart()` from the Tumbling `Select`.
  - Simplified console output to focus on OHLC values rather than bucket timestamps.

- `examples/rows-last-assignment/Program.cs`
  - Removed `[KsqlKey(3)] [KsqlTimestamp] public DateTime BucketStart`.
  - Removed `BucketStart = g.WindowStart()` from the Tumbling `Select`.

- `examples/index.md` / `examples/README.md`
  - Removed language suggesting “include WindowStart() once in Select”.
  - Rephrased `continuation-schedule` to emphasize Tumbling + `continuation: true` over a
    market schedule, without prescribing `WindowStart()` in the DTO.

### Impact

- Reduces the risk that users infer “Tumbling always needs a WindowStart value column in the DTO”.
- Keeps examples consistent with the Wiki’s WindowStart policy while still demonstrating
  OHLC and continuation behavior.

---

## 4. OssSamples physical test structure

### Changes

- Under `physicalTests/OssSamples`, only the three active physical tests remain at the root:
  - `TranslationsTimeBucketTests.cs`
  - `BarTranslationsSmokeTests.cs`
  - `BarTxYearTests.cs`

- All other OssSamples tests were moved to `physicalTests/OssSamples/Archive/`.

- `physicalTests/OssSamples/README.md` now documents:
  - Which tests are compiled into `Ksql.Linq.Tests.Integration`.
  - That `Archive/` contains historical/extra samples not part of the physical suite.

### Rationale

- The `.csproj` already limited compilation to those three files via `<Compile Include=...>`,
  but having many additional tests in the same folder made it unclear which ones were actually run.
- Moving inactive tests into `Archive` makes the active physical surface obvious without
  changing which tests are executed.

---

## Migration Notes

- **DLQ configuration**
  - Apps that previously set `DlqTopicName` explicitly continue to work as before.
  - Apps that did not set `DlqTopicName` now fall back to `dead_letter_queue`, reducing
    startup errors in minimal configurations.

- **WindowStart usage**
  - For Tumbling + WindowStart patterns, prefer the Wiki as the source of truth:
    - `Tumbling-Definition`
    - `Expression-Support-Tumbling-vs-General`
  - Physical tests and examples have been updated to match that guidance.

