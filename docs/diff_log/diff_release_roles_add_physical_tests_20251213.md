# diff: release roles update / add physical tests step (20251213)

## Summary

- Updated `docs/workflows/release_roles_and_steps.md` to include a “physical tests (Windows only)” step in the release flow.

## Motivation

- Some regressions (especially Tumbling/Hopping/Hub rows/TimeBucket/TableCache and ksqlDB dialect issues) can only be caught in a real Kafka/ksqlDB/Schema Registry environment.
- CI intentionally excludes Integration/physical tests, so the release process must explicitly require them when relevant.

## Change details

- `docs/workflows/release_roles_and_steps.md`
  - Added guidance in Local Prep for when/how to run physical tests (Windows only).
  - Added RC verification expectation to re-run the agreed physical smoke set when applicable.
  - Added QA checklist item for physical tests when the change impacts windowing/translation/hub flows.

