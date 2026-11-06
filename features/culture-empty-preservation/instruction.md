# K-C02 / K-R07 scoped correction

The user approved fixing only K-C02 and K-R07, reproducing and testing them,
and committing/pushing the validated branch. No main merge or release is requested.
Base: f9a31349ffc76baedc5cbfebb1c43def7c15d447.

## Approved differences / invariants

- K-C02: numeric SQL literals must use culture-independent protocol formatting.
  The same numeric expression must generate the same SQL under en-US and fr-FR.
  Inspect the shared BuilderValidation.SafeToString and the GroupBy constant
  path identified by the detector. Preserve existing string/bool/null and
  nonnumeric behavior; do not redesign SQL safety checks, escaping, dates or
  query semantics. No dependency/public API/version changes.
- K-R07: a present empty string in a nullable GenericRecord value remains empty.
  Preserve explicit null and nonempty strings. Preserve the existing missing
  field handling; do not redesign schema alignment/retry or make null and empty
  interchangeable. The shared normalization helper covers ordinary/retry calls.

Both feedback items are implementation_local: concrete evidence and local
remediation, no conflict with one another or unresolved policy findings. Other
review findings, particularly K-R06/K-C03, are outside this change.

## Test design / handoff

1. Add deterministic regression cases first, without product fixes. Execute
   them against the unmodified base and retain the expected failure evidence.
2. K-C02: exact SQL/number output with decimal, double/float and integral
   controls under at least en-US/fr-FR; exercise the reported predicate and
   role-equivalent GroupBy constant path. Restore ambient culture in finally.
3. K-R07: nullable string empty/null/nonempty cases plus missing-field control,
   exercised at the actual shared product normalization boundary.
4. Apply the bounded fixes, rerun those exact cases to green and rerun a
   consumer-side reproduction referencing the rebuilt DLL, not preserved 1.1.1.
5. Run required L3 gate + L4 golden (Query/Visitors touched), full main unit
   suite and applicable CLI/cache regressions, and compile both target frameworks.
   Run builds/tests serially. Record skips separately; do not approve goldens
   automatically or alter unrelated expected results to manufacture success.
6. Independent quality re-disposes these two findings only and checks scope,
   culture isolation, controls, code paths and evidence. Commit/push only if
   relevant checks pass. No claim of whole-library correctness.

AGENTS references docs/progress_management.md, which is absent in this snapshot.
Keep current event evidence in the governing XRefKit work/sessions record and
this bounded feature instruction rather than invent a project rule. The
repository-required behavior difference record is added under docs/diff_log.
