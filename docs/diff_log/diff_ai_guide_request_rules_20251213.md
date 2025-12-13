# diff: AI guide request rules (20251213)

## Summary
Add a short “AI Request Rules (Must / Should)” section to `AI_ASSISTANT_GUIDE.md` (source: `docs/ai_guide_intro_and_workflows.md`).

## Motivation
- Users may rely on different AI products/models (Copilot, ChatGPT, Claude) with varying capabilities.
- The project should not require a specific AI “skill level”, but should provide reproducible inputs and expectations.
- Avoid implying that Ksql.Linq (or the AI) can guarantee ksqlDB acceptance; emphasize user-side verification.

## Changes
- Add **Must / Should** rules under “For Developers”:
  - **Must**: provide Ksql.Linq + ksqlDB version assumptions; provide relevant inputs; request a verification checklist; treat “works on ksqlDB” as non-binding until verified.
  - **Should**: ask for explicit assumptions/open questions; structured output; propose multiple options + a small next-step experiment.

## Affected files
- `docs/ai_guide_intro_and_workflows.md`
- `AI_ASSISTANT_GUIDE.md` (generated; updated via `tools/build-ai-assistant-guide.ps1 -Write`)

## Notes
- Regeneration is required after editing `docs/ai_guide_*`: `pwsh -NoLogo -File tools/build-ai-assistant-guide.ps1 -Write`.

