# diff: AI_ASSISTANT_GUIDE generation + CI enforcement (20251213)

## Summary

- Introduced a single, enforced flow for the AI guide:
  - Edit `docs/ai_guide_*` (sources)
  - Generate `AI_ASSISTANT_GUIDE.md` (generated artifact)
  - Pack and verify `AI_ASSISTANT_GUIDE.md` exists inside `.nupkg`
- Added CI checks and a PR template checklist so contributors cannot accidentally ship stale/missing AI guide content.

## Motivation

- `ai-assist` is expected to read the packaged guide file and work offline.
- Without CI enforcement, the guide can drift:
  - sources updated but generated file not regenerated
  - pack succeeds but missing/old guide is shipped
- This drift is hard to catch late (RC/stable), so it should fail fast at PR time.

## Changes

### 1) Generator / verifier script

- Added `tools/build-ai-assistant-guide.ps1`
  - `-Write`: generate `AI_ASSISTANT_GUIDE.md` from:
    - `docs/ai_guide_intro_and_workflows.md`
    - `docs/ai_guide_conversation_patterns.md`
    - `docs/ai_guide_technical_sections.md`
  - `-Verify`: fail if `AI_ASSISTANT_GUIDE.md` is out of date
  - Normalizes CRLF/LF for stable comparisons across Windows/Linux

### 2) PR enforcement

- Updated `.github/workflows/pr-validate.yml`
  - Verify `AI_ASSISTANT_GUIDE.md` matches `docs/ai_guide_*`
  - Pack preview and verify `AI_ASSISTANT_GUIDE.md` exists in the library `.nupkg`

### 3) Publish enforcement (RC/stable)

- Updated publish workflows to generate and verify the packaged guide:
  - `.github/workflows/publish-preview.yml`
  - `.github/workflows/publish-github-packages.yml`
  - `.github/workflows/nuget-publish.yml`
  - `.github/workflows/cli-publish-github-packages.yml`
  - `.github/workflows/cli-nuget-publish.yml`

### 4) Contributor checklist

- Added `.github/pull_request_template.md` with an AI guide packaging checklist.

## Operational notes

- `AI_ASSISTANT_GUIDE.md` should not be hand-edited; always edit `docs/ai_guide_*` and regenerate.
- Workflows are expected to fail if the packaged guide is missing or unexpectedly small.

