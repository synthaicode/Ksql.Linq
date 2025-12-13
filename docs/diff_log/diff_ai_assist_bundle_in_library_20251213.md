# diff: ai-assist guide bundling in Ksql.Linq (20251213)

## Summary

- Changed the `ai-assist` command to read the AI guide from the **Ksql.Linq library package** (embedded resource), instead of requiring the CLI tool package to ship its own guide file copy.

## Motivation

- Single source of truth for the guide distribution: the guide is shipped with `Ksql.Linq`.
- Avoid drift where the CLI tool could accidentally ship a different guide copy than the library package.

## Changes

- `src/Ksql.Linq.csproj`
  - Embedded `AI_ASSISTANT_GUIDE.md` into the library assembly as an embedded resource (`Ksql.Linq.AI_ASSISTANT_GUIDE.md`).
  - Kept packaging the guide file in the library `.nupkg` (root) for discoverability.
- `src/Ksql.Linq.Cli/Commands/AiAssistCommand.cs`
  - `ai-assist` now prefers reading the embedded guide from the `Ksql.Linq` assembly.
  - Falls back to a local `AI_ASSISTANT_GUIDE.md` file (legacy) if the embedded guide is not present.
- `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj`
  - Removed the CLI package’s direct inclusion of `AI_ASSISTANT_GUIDE.md`.
- CI/workflows
  - CLI publish workflows now smoke-test that `ai-assist` prints the guide, instead of asserting the guide file exists inside the CLI `.nupkg`.

