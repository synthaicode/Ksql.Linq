# diff: release roles update / requirement intake & consultation (20251213)

## Summary

- Inserted an explicit “Requirement intake & design consultation” step before implementation work in `docs/workflows/release_roles_and_steps.md`.

## Motivation

- Prevents starting implementation with unclear scope, verification, or documentation obligations.
- Makes test expectations (L1–L4) and doc scope explicit early, reducing rework at RC/stable.
- Ensures AI guide packaging policy is acknowledged up front (`原稿→生成→同梱→検証`) when relevant.

## Change details

- `docs/workflows/release_roles_and_steps.md`
  - Added a pre-coding checklist covering:
    - scope/impact confirmation
    - guard levels and minimum test matrix
    - doc update scope (Wiki/CHANGELOG/release notes/READMEs/AI guide)
    - agreement phrase for AI guide packaging policy when applicable

