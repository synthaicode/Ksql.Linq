# diff: release flow / CLI build consistency (20251213)

## Summary

- Strengthened the release procedure to guarantee **library and CLI are built from the same commit** for stable releases.

## Background

- The CLI tool is built using `ProjectReference` to `Ksql.Linq`.
- When you `dotnet pack` the CLI tool, it embeds the library build output from the current source tree (it does not pull the library from NuGet).
- If commits land between the library release tag and the CLI release tag, users can end up with:
  - NuGet `Ksql.Linq` = commit A
  - CLI embedded `Ksql.Linq` = commit B

## Changes

### 1) Release procedure documentation

- `docs/workflows/release_roles_and_steps.md`
  - Added “Commit pinning (build consistency for CLI)” guidance:
    - Tag `vX.Y.Z` and `cli-vX.Y.Z` must point to the **same commit hash**
    - Do not add commits between those tags

### 2) CI enforcement (CLI stable publish)

- `.github/workflows/cli-nuget-publish.yml`
  - Added a guard step that fails the publish job if:
    - the corresponding library tag `vX.Y.Z` does not exist, or
    - `cli-vX.Y.Z` does not point to the same commit as `vX.Y.Z`

