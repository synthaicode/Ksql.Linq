## Release / Packaging Checklist (AI guide)

- [ ] If I changed `docs/ai_guide_*`, I regenerated `AI_ASSISTANT_GUIDE.md` (do not hand-edit the generated file).
- [ ] `AI_ASSISTANT_GUIDE.md` is readable (no mojibake) and has no duplicated header/footer/license blocks.
- [ ] `dotnet ksql ai-assist` prints the expected guide contents for this change.
- [ ] If I touched `.github/workflows/*publish*.yml`, I confirmed AI guide generation + packaging checks are still enforced.

