# diff: Copilot agent-mode prompting (20251213)

## Summary
Add a GitHub Copilot prompt template to encourage agent/workspace mode usage when available, and to require explicit disclosure when local file access is not available.

## Motivation
- Copilot environments differ: some can access workspace files (agent/workspace mode), others cannot.
- Users often assume file access; making the AI state capability upfront reduces back-and-forth and incorrect assumptions.
- This stays within OSS responsibility by guiding interaction patterns rather than claiming ksqlDB execution guarantees.

## Changes
- `docs/ai_guide_intro_and_workflows.md`
  - Under “GitHub Copilot Chat”, add a short prompt template:
    - AI must say whether it can access local/workspace files.
    - If not, AI must ask the user to paste relevant snippets.
    - If yes, AI must read `#file:AI_ASSISTANT_GUIDE.md` and follow rules.
    - Output should include a “verify on your ksqlDB” checklist.
- Regenerate `AI_ASSISTANT_GUIDE.md` via `tools/build-ai-assistant-guide.ps1 -Write`.

## Affected files
- `docs/ai_guide_intro_and_workflows.md`
- `AI_ASSISTANT_GUIDE.md` (generated)

