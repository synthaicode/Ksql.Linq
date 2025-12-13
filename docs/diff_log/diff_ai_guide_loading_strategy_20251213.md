# diff: AI guide loading strategy (20251213)

## Summary
Add guidance to `AI_ASSISTANT_GUIDE.md` for handling AI context-size limits by choosing between:
- **Full load** of the guide (preferred), or
- **Focused load** of only relevant sections (when the full guide does not fit).

## Motivation
- Different AI assistants/products have different context limits and file/URL access constraints.
- Users need an explicit, repeatable way to avoid “paste the whole guide” failures.
- This improves onboarding without expanding OSS responsibility into ksqlDB execution guarantees.

## Changes
- `docs/ai_guide_intro_and_workflows.md`
  - Add “Guide Loading Strategy (Full / Focused)” section with prompt templates and a minimal suggested section set.
- Regenerate `AI_ASSISTANT_GUIDE.md` via `tools/build-ai-assistant-guide.ps1 -Write`.

## Affected files
- `docs/ai_guide_intro_and_workflows.md`
- `AI_ASSISTANT_GUIDE.md` (generated)

