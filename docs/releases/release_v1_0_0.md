# Release Notes v1.0.0

## Highlights

- First **1.0** stable release of **Ksql.Linq**.
- Introduced the **AI Assistant Guide** (`AI_ASSISTANT_GUIDE.md`) and CLI integration for **AI-assisted design**.
- Added multi-language support for the AI guide entry flow (20+ languages).

> **Note:** Core runtime behavior in v1.0.0 builds on the fixes shipped in **v0.9.8**.  
> This release itself focuses on AI Assistant Guide packaging and tooling/CI integration.

---

## 1. AI Assistant Guide (Library)

- Added a structured **AI Assistant Guide**, split into:
  - Intro & workflows — `ai_guide_intro_and_workflows.md`
  - Conversation patterns — `ai_guide_conversation_patterns.md`
  - Technical sections — `ai_guide_technical_sections.md`
- CI now concatenates these section files into a single `AI_ASSISTANT_GUIDE.md` and includes it in the **Ksql.Linq** NuGet package.
- The guide text is licensed under **CC BY 4.0**  
  (library code remains **MIT**-licensed).

---

## 2. CLI `ai-assist` Command

From `Ksql.Linq.Cli`, a new command is available:

```bash
dotnet ksql ai-assist
```

- Prints the **AI Assistant Guide** to standard output with:
  - A short header explaining how to use it with AI coding assistants.
  - A closing note to help users start an AI-assisted workflow.
- `--copy` option:
  - Attempts to copy the full guide text to the clipboard  
    (Windows / macOS / Linux, best-effort).
  - If clipboard integration fails, the command still succeeds;  
    you can paste from the terminal output.

CLI packages also include `AI_ASSISTANT_GUIDE.md` so the guide is always available next to the tool.

---

## 3. Multi-language AI Entry Messages

The `ai-assist` command now supports localized header/footer texts for:

- **Europe & Americas**: `en`, `es`, `fr`, `de`, `pt`, `it`, `ro`
- **Asia**: `ja`, `zh`, `ko`, `hi`, `id`, `vi`, `th`
- **Middle East & Africa**: `ar`, `fa`, `sw`
- **Others**: `ru`, `tr`

Runtime behavior:

- Detects the UI culture (two-letter language code).
- Attempts to fetch a localized header/footer from GitHub (1-second timeout).
- Falls back to **English** if:
  - the localized text is not found, or
  - network access fails.

---

## 4. CI/CD Integration

Library workflows:

- `publish-github-packages.yml`
- `nuget-publish.yml`

Now:

- Generate `AI_ASSISTANT_GUIDE.md` from the section docs.
- Verify that `AI_ASSISTANT_GUIDE.md` is present in the packaged `.nupkg`.

CLI workflows:

- `cli-publish-github-packages.yml`
- `cli-nuget-publish.yml`

Now:

- Generate `AI_ASSISTANT_GUIDE.md` before packing the CLI tool.
- Ensure both the library and CLI ship the same guide version.

## Migration Notes

- No breaking changes in the public API compared to recent **0.9.x** releases.
- Upgrading from **v0.9.7 → v1.0.0** is expected to be **drop-in** for existing applications.
- For AI-assisted workflows:
  - Use `AI_ASSISTANT_GUIDE.md` (from the library or CLI package) as the primary guide for AI coding assistants.
  - Prefer:

    ```bash
    dotnet ksql ai-assist --copy
    ```

    to quickly feed the latest guide text into your AI assistant.

---

## Known Issues

- Clipboard copy in `ai-assist --copy` is **best-effort** and depends on platform tools:
  - Windows: `clip`
  - macOS: `pbcopy`
  - Linux: `wl-copy`, `xclip`, etc.
- If clipboard integration is not available, the command still exits successfully;  
  copy/paste manually from the terminal output.
