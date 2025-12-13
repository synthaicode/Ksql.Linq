# diff: release_roles_and_steps improvements from critique (20251213)

## Summary

Incorporated review feedback into `docs/workflows/release_roles_and_steps.md` and related CI guards:

- Clarified library/CLI version alignment and dependency model expectations.
- Added safer guidance for release branch merge strategy when `--ff-only` cannot be used.
- Reduced manual tagging/version mismatch risk by deriving CLI package version from tag in CLI publish workflows.
- Added a minimal rollback/emergency policy section.
- Strengthened “docs freshness” expectations (Wiki timing guidance).

## Details

### 1) Dependency & Versioning

- `release_roles_and_steps.md` now explicitly requires:
  - updating `<Version>` in both `src/Ksql.Linq.csproj` and `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj`
  - keeping base versions aligned unless explicitly agreed otherwise
- CI now checks version alignment in PR validation.

### 2) Branch strategy

- `--ff-only` remains preferred, but the document now calls out the two safe alternatives:
  - freeze main during the release window (branch protection / rule)
  - allow a normal merge commit to avoid last-minute rebase mistakes

### 3) Tagging automation risk reduction

- CLI publish workflows now pass `-p:PackageVersion=<tag-derived>` to `dotnet pack`,
  making the package version match the tag and preventing silent mismatches.

### 4) Documentation lag

- Documented that breaking/important changes should not wait until Aftercare;
  Wiki updates should be prepared during RC verification so they can land at release time.

### 5) Rollback strategy

- Added a minimal policy for:
  - critical library bug after publish (unlist + hotfix)
  - CLI publish failure after library publish (acceptable vs blocking + comms)

### 6) AI guide process transparency

- AI guide generation is deterministic (source files under `docs/ai_guide_*` plus a committed generator script),
  and CI enforces freshness and packaging checks.

