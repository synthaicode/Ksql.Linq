# AI_ASSISTANT_GUIDE Rebuild Plan (v2)

> Owner: Amagi (overall direction)  
> Contributors: Naruse (C# / Ksql.Linq implementation), Hiromu (documentation / communication), Kyoka (quality review)

---

## 1. Goals

- Make `AI_ASSISTANT_GUIDE.md` a **trustworthy, versioned companion** to the Wiki:
  - Wiki = canonical technical reference (behavior, configuration, APIs).
  - AI guide = AI-oriented summary + conversation scaffold, always aligned with Wiki and code.
- Remove historical inconsistencies:
  - Eliminate references to non-existent APIs or deprecated patterns.
  - Clearly separate **implemented** vs **planned** behavior.
- Simplify maintenance:
  - Reduce duplication with Wiki.
  - Structure the guide so future updates can be driven from a smaller, structured source (YAML/spec) and diff_log.

---

## 2. Scope of Rebuild (v2)

**Keep (mostly) as-is**:
- Overall format:
  - 6-step conversation flow (Prerequisites → Summary → Principles → Options → Recommendation → Next Steps).
  - AI profile and “For Developers” sections.
- Tone and purpose:
  - AI-facing, honest about uncertainty and version differences.

**Rebuild (content refresh based on Wiki + code)**:
- `Library Overview` / `Core Architecture`
- `Design Patterns`
- `Common Use Cases`
- `API Reference Quick Start`
- `Examples Index`
- `Decision Trees`
- `Best Practices`
- `Design-Time Workflow`
- `Troubleshooting`

Out of scope for v2:
- Full automation / generation from structured spec (planned as a follow-up).

---

## 3. Phased Approach

### Phase 1 – Foundations & Guardrails

- [x] Bring `AI_ASSISTANT_GUIDE.md` under version control in this repo.
- [x] Add implementation status note and “Wiki is source of truth” note.
- [x] Create mapping doc: `docs/ai_assistant_wiki_mapping.md`.
- [x] Clean up obvious inconsistencies:
  - Non-existent APIs: `ToView()`, `FirstOrDefaultAsync()`, `FindAsync()`, etc.
  - Windowing: mark Hopping / Session as planned, correct Tumbling API shape.

### Phase 2 – Section-by-Section Rebuild

For each section listed in Scope:

1. **Identify canonical Wiki + code sources**  
   - Use `docs/ai_assistant_wiki_mapping.md` as the starting point.
   - Confirm relevant Wiki pages (e.g., `Streams-and-Tables.md`, `Tumbling-*.md`, `CLI-Usage.md`, `API-Reference.md`, runtime/operations docs).
   - Confirm public API via `src/PublicAPI.*.txt` and relevant examples/tests.

2. **Draft v2 content (section-level)**  
   - Keep headings and overall layout.
   - Rewrite text and code samples so that:
     - Every API call exists in code/PublicAPI.
     - Concepts and terminology match Wiki wording.
     - Status is explicit: `Implemented / Preview / Planned`.

3. **Review and diff_log**  
   - Kyoka reviews for:
     - Consistency with Wiki and code.
     - Clarity and lack of contradictions.
   - Record change in `docs/diff_log/diff_ai_assistant_guide_<section>_<YYYYMMDD>.md`.

4. **Mark section as “v2 complete”**  
   - Optionally note completion status in this plan or a simple checklist in `docs/ai_assistant_wiki_mapping.md`.

### Phase 3 – Optional Structured Spec

*(Future work, not required for initial v2)*:

- Introduce a structured spec for the “high-churn” parts:
  - API Reference entries.
  - Design Patterns index.
  - Decision Trees definitions.
- Add a small generator (CLI subcommand or tool) that:
  - Reads spec (YAML/JSON).
  - Writes the corresponding sections of `AI_ASSISTANT_GUIDE.md`.
- Wire a CI check so regenerated content must match committed guide.

---

## 4. Section Priorities (Proposed)

1. **Design-Time Workflow + CLI usage**  
   - High impact for users trying to adopt design-time features.
   - Align with `CLI-Usage.md`, `API-Workflow.md`, and designtime docs under `docs/`.

2. **API Reference Quick Start + Design Patterns**  
   - Ensure all listed APIs exist and reflect PublicAPI.
   - Reference the right examples and Wiki pages.

3. **Best Practices + Troubleshooting**  
   - Align with `Runtime-Tuning-Plan-v0-9-6.md`, `Lag-Monitoring-and-Tuning.md`, `Operations-*`, and policy docs.

4. **Common Use Cases + Decision Trees**  
   - Use existing examples and Wiki scenarios as the factual base.
   - Keep “Guidance-oriented” nature but avoid assuming unimplemented features.

---

## 5. Roles and Responsibilities

- **Amagi (天城)**:
  - Owns overall direction and scope (what is in/out of v2).
  - Approves major structural changes (e.g., adding/removing sections).

- **Naruse (鳴瀬)**:
  - Verifies technical correctness against code and PublicAPI.
  - Ensures examples compile conceptually and match actual DSL.

- **Hiromu (広夢)**:
  - Drives narrative, structure, and wording for AI-friendliness.
  - Keeps the guide concise and aligned with communication goals.

- **Kyoka (鏡花)**:
  - Acts as the quality gate:
    - Checks for contradictions between guide/Wiki/code.
    - Reviews status labels and “implemented vs planned” marking.

---

## 6. Completion Definition for v2

`AI_ASSISTANT_GUIDE v2` is considered complete when:

- All scoped sections have:
  - Verified alignment with the mapped Wiki pages and PublicAPI.
  - Status labels (Implemented / Guidance / Planned) where appropriate.
  - No references to non-existent APIs.
- `docs/ai_assistant_wiki_mapping.md` is up to date, reflecting the rebuilt sections.
- diff_log entries exist for each major section rebuild, documenting:
  - What changed.
  - Which Wiki pages and APIs were used as references.

---

## 7. How OSS Changes Affect the Technical Sections

When Ksql.Linq evolves (bugfixes, behavior changes, new features), treat the **code + Wiki + examples + CHANGELOG** as the primary sources of truth, and update the AI guide’s technical sections only when design decisions are impacted.

### 7.1 Existing Feature Changes

For a change that updates existing behavior:

- Always:
  - Update code.
  - Update Wiki.
  - Update examples if they are affected.
  - Update `CHANGELOG.md` and `docs/releases/release_vx_y_z.md`.
- Update `ai_guide_technical_sections.md` **only if**:
  - Recommended design patterns change (e.g., STREAM vs TABLE guidance, windowing constraints, error-handling strategy).
  - The change affects how developers should choose In/Between/Out/Integration patterns.

In that case:
- Adjust the relevant In/Between/Out/Integration block.
- Add or update links to the appropriate Design Patterns / Examples / Wiki pages.
- Record the update in `docs/diff_log/diff_ai_assistant_guide_*_YYYYMMDD.md`.

### 7.2 New Feature Additions

For new features:

- Always:
  - Update code and tests.
  - Add or extend Wiki pages for the new capability.
  - Add at least one example that demonstrates the feature.
  - Update `CHANGELOG.md` and the release notes.
- For AI guide technical sections:
  - Decide where the feature lives in the In/Between/Out/Integration view:
    - New input/source pattern?
    - New processing/edit pattern (filter/join/windowing/error handling/etc.)?
    - New output/integration pattern?
  - If the feature represents a stable design pattern or a new recommended path, add:
    - 1–2 lines describing the pattern and when to use it.
    - Links to Wiki + example(s).
  - If the feature is still experimental or niche, keep it in Wiki/examples only until its design stabilizes.

---

## 8. CI Integration for AI Guide

To keep the AI guide in sync with the code and packaging, CI should enforce a few lightweight checks.

### 8.1 Guide Generation and Packaging

- Before packing RC/stable packages:
  - Generate `AI_ASSISTANT_GUIDE.md` by concatenating:
    - `docs/ai_guide_intro_and_workflows.md`
    - `docs/ai_guide_conversation_patterns.md`
    - `docs/ai_guide_technical_sections.md`
  - Ensure `src/Ksql.Linq.csproj` includes `AI_ASSISTANT_GUIDE.md` as a packed file (NuGet root).
- In `publish-github-packages.yml` and `nuget-publish.yml`:
  - After `dotnet pack`, unzip the produced `.nupkg` and verify that `AI_ASSISTANT_GUIDE.md` exists at the expected path.
  - Fail the job if the guide is missing.

### 8.2 CLI Compatibility (ai-assist)

- Once `dotnet ksql ai-assist` (or equivalent) is implemented:
  - Add a simple CI smoke test (can be a separate job or part of CLI tests):
    - Install the CLI from the packed artifact.
    - Run `dotnet ksql ai-assist` and verify:
      - Exit code is 0.
      - Output is non-empty.
    - Optionally, run `dotnet ksql ai-assist --help` to ensure the option is wired.

### 8.3 Documentation-only Changes

- For PRs that touch only `docs/ai_guide_*` and related diff_log:
  - CI may skip heavy tests (integration) but should still:
    - Run basic build to ensure guide generation tooling (if any) compiles.
    - Keep the packaging check to avoid accidentally dropping the guide from the NuGet package.
