# diff: release roles update / AI guide packaging policy (20251213)

## Summary

- Updated `docs/workflows/release_roles_and_steps.md` to align the release procedure with the AI guide packaging policy:
  - `docs/ai_guide_*` are the editable sources.
  - `AI_ASSISTANT_GUIDE.md` is a generated artifact (kept in sync with sources).
  - Release validation includes verifying `AI_ASSISTANT_GUIDE.md` is actually included in `.nupkg` artifacts and readable via `dotnet ksql ai-assist`.

## Motivation

- Without an explicit, enforced flow, it is easy for the packaged `AI_ASSISTANT_GUIDE.md` to become stale relative to `docs/ai_guide_*`.
- `ai-assist` is intended to read the packaged guide file (offline-friendly) and `--copy` should not introduce encoding issues.

## Change details

- `docs/workflows/release_roles_and_steps.md`
  - Local prep now explicitly requires:
    - Editing `docs/ai_guide_*` sources
    - Regenerating `AI_ASSISTANT_GUIDE.md`
    - Optional local pack/unzip verification for both library and CLI packages
    - CLI smoke checks for `ai-assist` / `--copy`
  - RC verification and QA checklist now include:
    - Packaged guide readability and freshness checks
    - CI expectation that missing packaged guide should fail workflows

## Follow-ups (implementation)

- Ensure `.github/workflows/*publish*.yml` actually performs:
  - pre-pack generation of `AI_ASSISTANT_GUIDE.md` from `docs/ai_guide_*`
  - post-pack verification that `AI_ASSISTANT_GUIDE.md` exists inside the produced `.nupkg`

