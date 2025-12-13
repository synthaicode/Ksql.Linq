# Ksql.Linq Release Roles & Steps (Library + CLI)

This document summarizes **who does what** during a release, and **which GitHub Actions workflows** are involved.
It covers both the **core library** (`Ksql.Linq`) and the **CLI tool** (`Ksql.Linq.Cli`).

---

## 0. Local Prep (Developer)

**Owner:** Core developer (release engineer)  
**Branch:** `release/<version>` (e.g., `release/1.0.0`)

**Responsibilities:**
- Create or update the release branch:
  - `git switch -c release/<version>` (or `git switch release/<version>`)
- Requirement intake & design consultation (before coding):
  - Confirm **what changes** (feature/bugfix), **who is affected** (API/behavior), and **how to verify** (UT/physical/CLI/docs).
  - Decide the guard level expectations (L1–L4) and the minimum test matrix for this change.
  - Confirm documentation scope: Wiki (canonical), CHANGELOG, release notes, READMEs, and AI guide (docs sources → generated → packaged).
  - If the change touches AI guide/CLI packaging, explicitly include the agreement phrase: `原稿→生成→同梱→検証`.
- Align versioning and notes:
  - Update `<Version>` and `<PackageReleaseNotes>` in `src/Ksql.Linq.csproj`.
  - Update `<Version>` in `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj` to the **same base version** as the library (unless explicitly agreed otherwise).
  - Dependency model (clarify to avoid release-time surprises):
    - The CLI is built via `ProjectReference` to `Ksql.Linq` during development.
    - Release packages must still be version-consistent: validate that the packed CLI/tool resolves the intended library bits for the same version.
  - Append a new `docs/diff_log/diff_<feature>_<YYYYMMDD>.md` for notable changes.
  - Update `docs/releases/release_v<version>.md` (or create it for new minor versions).
  - AI guide (source → generated → packaged):
    - **Edit sources** (do not hand-edit the generated file):
      - `docs/ai_guide_intro_and_workflows.md`
      - `docs/ai_guide_conversation_patterns.md`
      - `docs/ai_guide_technical_sections.md`
    - **Regenerate** `AI_ASSISTANT_GUIDE.md` (generated artifact committed to the repo) using the project’s build script/tooling.
      - Note: generation is deterministic (a pure concatenation of the three source files, enforced by CI), not an LLM/prompt-driven step.
    - **Verify packaging** locally (optional but recommended):
      - Library: `dotnet pack src/Ksql.Linq.csproj -c Release -o .artifacts`
      - CLI tool: `dotnet pack src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release -o .artifacts/cli`
      - Confirm `AI_ASSISTANT_GUIDE.md` exists inside the produced `.nupkg` (unzip and inspect).
    - **Smoke** the CLI against packaged content:
      - `dotnet ksql ai-assist` prints the packaged guide
      - `dotnet ksql ai-assist --copy` copies it to clipboard (where supported; verify no mojibake)
- Keep examples in sync (see also §6):
  - Update `examples/Directory.Build.props` to reference the new `Ksql.Linq` NuGet version (e.g., `1.0.0`).
  - Run a quick build for examples to confirm compatibility (optional but recommended):
    - `dotnet build examples/headers-meta/HeadersMeta.csproj -c Release`
    - `dotnet build examples/windowing/windowing.csproj -c Release`
- Public API check (library):
  - `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`
- Unit tests (no physical tests / Integration):
  - `dotnet test tests/Ksql.Linq.Tests.csproj -c Release --filter "TestCategory!=Integration"`
- Physical tests (Windows only; run when changes touch windowing/DSL/runtime/CLI behavior):
  - Purpose: catch regressions that only reproduce in a real Kafka/ksqlDB/Schema Registry environment.
  - Run policy:
    - If the change impacts Tumbling/Hopping/Hub rows (`*_1s_rows`), TimeBucket/TableCache, or query translation: run the physical test set.
    - If the change is docs-only, skip.
  - How to run:
    - Use the physical test runner for this repo (see `physicalTests/`).
    - Run at least the smoke set agreed in the requirement intake step.
    - Record the executed tests + results (PASS/FAIL) in the PR/issue comment.
- CLI smoke build:
  - `dotnet build src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release`
  - `dotnet pack src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release -o .artifacts/cli` (optional local check)
- Commit and push:
  - `git commit` with a clear message (e.g., *Prepare Ksql.Linq 1.0.0: CLI + AI guide cleanup*)
  - `git push origin release/<version>`

**GitHub Actions:** none yet (only local work).

---

## 1. RC Publish to GitHub Packages (Library)

**Owner:** Core developer (trigger), CI runs the workflow  
**Workflows:** `.github/workflows/publish-preview.yml`

**Trigger:**
- Manual RC (ad-hoc or internal):  
  - Run `publish-preview.yml` via **workflow_dispatch** on the appropriate branch (typically `release/<version>`).
- Tagged RC (official RC tied to a commit):  
  - Push a tag like `v<version>-rcN` (e.g., `v1.1.0-rc1`).  
  - This also triggers `publish-preview.yml` via `on.push.tags`.

**What CI does:**
- Restore, build, test (no Integration).
- `dotnet pack` the library with an RC/preview suffix.
- Publish the package to **GitHub Packages**.

**Developer responsibilities:**
- Confirm the `Publish preview package to GitHub Packages` job is green.
- Locally verify that the RC can be restored and used:
  - Configure a `NuGet.config` pointing to GitHub Packages.
  - `dotnet add package Ksql.Linq -v <version>-<suffix>` in a throwaway project.
  - Run basic usage (e.g., examples or a small test app).

---

## 2. RC Publish to GitHub Packages (CLI)

**Owner:** Core developer (trigger), CI runs the workflow  
**Workflows:** `.github/workflows/cli-publish-github-packages.yml` (name may differ)

**Trigger:**
- Push a tag like `cli-v<version>-rc.<n>` (e.g., `cli-v1.0.0-rc.1`).

**What CI does:**
- Restore and build `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj`.
- `dotnet pack` the CLI as a .NET tool package.
- Publish `Ksql.Linq.Cli` to **GitHub Packages**.

**Developer responsibilities:**
- Create and push the RC tag:
  - `git tag cli-v1.0.0-rc.1`
  - `git push origin cli-v1.0.0-rc.1`
- Verify the CLI RC can be installed from GitHub Packages:
  - `dotnet tool install --global Ksql.Linq.Cli --version 1.0.0-rc.* --add-source <github-feed>`
  - `dotnet ksql script --help`
  - `dotnet ksql ai-assist` (basic smoke test of AI guide integration)

---

## 3. RC Verification & GO Decision

**Owner:** Dev + Test (verification), Lead (GO/NO-GO decision)

**Verification tasks:**
- Library:
  - Run sample apps and examples against the RC from GitHub Packages.
  - Confirm critical features (TimeBucket, DLQ, Tumbling, etc.) behave as expected.
  - If applicable, re-run the agreed physical test smoke set (Windows only) against the RC.
- CLI:
  - Run `dotnet ksql script` and `dotnet ksql avro` against sample contexts.
  - AI guide checks:
    - Verify the packaged `AI_ASSISTANT_GUIDE.md` is up to date and readable (no encoding issues, duplication, or stale content).
    - Run `dotnet ksql ai-assist` and confirm it reads the packaged guide file (no external dependency).
    - If used: run `dotnet ksql ai-assist --copy` and confirm the clipboard content is not mojibake.

**GO decision (Lead):**
- If RC is acceptable:
  - Merge `release/<version>` into `main`:
    - Preferred (simple history): `git checkout main` → `git merge --ff-only release/<version>` → `git push origin main`
    - If `--ff-only` fails because `main` moved:
      - Option A (recommended): temporarily **freeze main** (branch protection / release window rule), then re-run the merge.
      - Option B: do a normal merge commit (`git merge release/<version>`) with review, to avoid last-minute rebase accidents.
- The `main` branch now represents the final release content.

**Commit pinning (build consistency for CLI)**:
- The CLI tool is built using `ProjectReference` to `Ksql.Linq`, so `dotnet pack` embeds the library build output from that same commit.
- To avoid “NuGet library contents” vs “CLI embedded library contents” drift:
  - Treat the post-GO commit on `main` as **immutable for release**.
  - Publish the library tag `vX.Y.Z` and the CLI tag `cli-vX.Y.Z` from the **same commit hash** (no intervening commits).
  - Policy: “Do not release from a commit other than the tagged commit.”

---

## 4. Stable Release Signal & Auto-Tag (Library)

**Owner:** Lead (signal), CI runs the workflows  
**Workflows:**
- `.github/workflows/promote-ready-label.yml`
- `.github/workflows/promote-ready-comment.yml`
- `.github/workflows/nuget-publish.yml`

**Signal options (Lead):**
- On the tracking Issue/PR:
  - Add the label `release-ready`, **or**
  - Comment `/release-ready`

**What CI does:**
- The `promote-ready-*` workflow:
  - Reads `<Version>` from `src/Ksql.Linq.csproj` on **origin/main**.
  - Creates a Git tag `v<Version>` on `origin/main` (e.g., `v1.0.0`).
- Tag push triggers `nuget-publish.yml`:
  - Strict build and tests (no Integration).
  - `dotnet pack` the library.
  - Push to **nuget.org** using `NUGET_API_KEY` secret.

**Lead responsibilities:**
- Monitor `Publish Stable to nuget.org` job for success.
- Verify the `Ksql.Linq` page on nuget.org shows the new version.

---

## 5. Stable Release Tag & Publish (CLI)

**Owner:** Lead or core developer (tag), CI runs the workflow  
**Workflows:** `.github/workflows/cli-nuget-publish.yml` (name may differ)

**Trigger:**
- Push a stable CLI tag: `cli-v<version>` (e.g., `cli-v1.0.0`).

**What CI does:**
- Build and test the CLI project.
- `dotnet pack` as a .NET tool package.
- Push `Ksql.Linq.Cli` to **nuget.org** using `NUGET_API_KEY`.

**Developer responsibilities:**
- Create and push the tag after library release is confirmed:
  - Use the **exact CLI version** from `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj` (avoid typos/mismatch).
  - Tag format must match workflows:
    - Stable: `cli-vX.Y.Z`
    - RC: `cli-vX.Y.Z-rc.N`
  - Build consistency rule:
    - Create `cli-vX.Y.Z` on the **same commit** as the library tag `vX.Y.Z`.
    - Do not add commits between `vX.Y.Z` and `cli-vX.Y.Z`.
  - Then push the tag:
    - `git tag cli-v<version>`
    - `git push origin cli-v<version>`
- Verify on nuget.org that `Ksql.Linq.Cli` appears with the expected version and README.

---

## 6. Aftercare & Documentation Sync

**Owner:** Core developer + Docs lead (広夢 / 文乃)

**Tasks:**
- `docs/diff_log`:
  - Ensure all major changes have a `diff_<topic>_<YYYYMMDD>.md` entry.
  - Confirm any Streamiz/WindowStart/TimeBucket adjustments are logged.
- Release notes:
  - Update or confirm `docs/releases/release_v<version>.md` for both library and CLI.
  - Cross-check content against `CHANGELOG.md` (if applicable).
- README / Wiki / AI guide (Docs担当中心):
  - `src/README.md` と `src/Ksql.Linq.Cli/README.md` が新バージョンの機能と位置づけを反映していることを確認・更新（担当: 広夢）。
  - `AI_ASSISTANT_GUIDE.md` が `docs/ai_guide_*` の内容と一致していることを確認・更新（担当: 文乃）。
  - **Wiki 更新（担当: 広夢 + 文乃）**:
    - 変更された仕様に関わる Wiki ページ（例: Tumbling / WindowStart / DLQ / Examples / CLI-Usage / Examples Index / AI Support 関連）を確認し、必要な修正を反映。
    - AIガイドからリンクされている Wiki セクションが、リリース時点の挙動と矛盾していないことを確認。
    - 必要であれば、`Home.md` や `Overview.md` に今回のリリースで重要な変更点を追記。
    - **ラグ対策**: 破壊的変更や導線変更がある場合、Wikiは Aftercare に回さず、RC検証～GO判断の段階で更新案を確定し、リリースと同時に反映できる状態にする（下書き/PR/コミット準備）。
- Examples:
  - Confirm `examples/Directory.Build.props` references the released version (e.g., `1.0.0`).
  - Optionally, run `build_examples` workflow or a local equivalent.

---

## 7. Roles Summary

### 7.1 Named Roles (AIチームとの対応)

| Role | Primary Owner | Notes |
|------|---------------|-------|
| Release Lead / Coordinator | **天城 (Amagi)** | 全体スケジュールと優先順位の調整役。各担当の進捗を集約し、GO/NO-GO を「調整」するが、コマンド実行や実装の担当ではない。 |
| Library Implementor | 鳴瀬 (Naruse) | コアライブラリの実装・テスト・diff_log 更新。 |
| CLI Implementor | 凪 (Nagi) | `Ksql.Linq.Cli` の実装、AIガイド連携、CLI用 README/ドキュメントの更新。 |
| Test / Physical Env | 詩音 (Shion) | 物理テスト・Docker/kafka 環境での検証。 |
| Docs / AI Guide | 広夢 (Hiromu) / 文乃 (Fumino) | README・Wiki・AI_ASSISTANT_GUIDE の内容調整と言語表現。 |

### 7.2 Phase Responsibilities

| Phase | Role | Responsibility |
|-------|------|----------------|
| 0. Local prep | Developer | Update versions, diff_log, AI guide, examples, run tests, push `release/<version>` |
| 1. Library RC | Developer + CI | RC build & publish to GitHub Packages |
| 2. CLI RC | Developer + CI | Tag `cli-v<version>-rc.*`, publish CLI RC to GitHub Packages |
| 3. Verification | Dev + Test | Validate RCs (library + CLI), run examples and smoke tests |
| 3. GO decision | **天城 (Amagi)** + Owners | 各担当からの報告を受けて GO/NO-GO を決定する（実際のマージやタグ作成は担当エンジニアが実施）。 |
| 4. Library stable | Library Implementor + CI | Lead の GO を受けて `release-ready` シグナルを付与し、自動タグ `v<version>` で nuget.org に公開。 |
| 5. CLI stable | CLI Implementor + CI | Lead の GO を受けて `cli-v<version>` をタグ付けし、CLI ツールを nuget.org に公開。 |
| 6. Aftercare | Dev + Docs | Sync diff_log, release notes, README, Wiki, AI guide, examples |

This structure keeps **responsibilities explicit**, with **天城 (Amagi)** acting as the release coordinator (司令塔) and individual owners (Naruse / Nagi / Shion / Hiromu / Fumino) handling concrete implementation, testing, and documentation tasks. 

---

## 8. Minimal QA Checklist (鏡花視点)

Before declaring a release **ready**, confirm at least the following items.  
天城は各担当からの報告を集約し、GO/NO-GO の判断材料とする。

### 8.1 Library (Ksql.Linq)

- [ ] `src/Ksql.Linq.csproj` の `<Version>` / `<PackageReleaseNotes>` がターゲットバージョンと一致している。  
- [ ] Public API strict build 通過済み：  
  - `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`  
- [ ] 単体テスト（Integration 以外）がグリーン：  
  - `dotnet test tests/Ksql.Linq.Tests.csproj -c Release --filter "TestCategory!=Integration"`  
- [ ] 物理テスト（Windowsのみ、該当変更時）が実行され、結果が記録されている：  
  - Tumbling/Hopping/Hub rows/TimeBucket/TableCache/翻訳系の変更がある場合は必須  
- [ ] 主要な diff が `docs/diff_log/diff_*.md` に追記されている（特に Streamiz/WindowStart/TimeBucket/DDL まわり）。  

### 8.2 CLI (Ksql.Linq.Cli)

- [ ] `src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj` の `<Version>` がライブラリと整合している。  
- [ ] CLI ビルド成功：  
  - `dotnet build src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release`  
- [ ] ローカルパック確認（任意）：  
  - `dotnet pack src/Ksql.Linq.Cli/Ksql.Linq.Cli.csproj -c Release -o .artifacts/cli`  
- [ ] 最低限のコマンド動作確認：  
  - `dotnet ksql script --help`  
  - `dotnet ksql avro --help`  
  - `dotnet ksql ai-assist`（AIガイドが読めることを確認）  

### 8.3 Examples

- [ ] `examples/Directory.Build.props` の `Ksql.Linq` バージョンがリリース対象（例: `1.0.0`）になっている。  
- [ ] 代表例のビルド確認（カテゴリごとに最低1本を目安）：  
  - Windowing 系: `dotnet build examples/windowing/windowing.csproj -c Release`  
  - Headers/メタ系: `dotnet build examples/headers-meta/HeadersMeta.csproj -c Release`  
  - DLQ/エラー系など、必要に応じて追加。  

### 8.4 Docs / Wiki / AI Guide

- [ ] `src/README.md` が「Ksql.Linq 1.x」の主対象読者・主な価値・サポート状況（Tumbling/Hopping 等）を正しく反映している。  
- [ ] `src/Ksql.Linq.Cli/README.md` が CLI 1.x の機能（`script` / `avro` / `ai-assist`）と GitHub Packages/NuGet の利用方法を説明している。  
- [ ] AI guide 原稿（`docs/ai_guide_*`）の更新が反映され、`AI_ASSISTANT_GUIDE.md` が最新である（手動編集していない）。  
- [ ] `AI_ASSISTANT_GUIDE.md` が文字化けせず、License の重複やヘッダ/フッタの重複がない。  
- [ ] Wiki の主要ページが今回のリリース内容と整合している：  
  - Tumbling / WindowStart / TimeBucket 関連 (`Tumbling-*.md`, `Expression-Support-Tumbling-vs-General`, `Tumbling-Overview`, `Tumbling-Definition` など)。  
  - DLQ / Streamiz / TableCache 関連。  
  - `CLI-Usage.md`, `Examples.md`, `Kafka-Ksql-Linq-User-Guide.md` 等。  

### 8.5 CI / Workflows

- [ ] `publish-preview.yml` (ライブラリ RC) がグリーン。  
- [ ] CLI 用 GitHub Packages ワークフロー（`cli-v*.*.*-rc.*` トリガー）が通過し、RC をインストールして動作確認済み。  
- [ ] `nuget-publish.yml` が `v<version>` タグで通過し、ライブラリが nuget.org に反映されている。  
- [ ] CLI 用 nuget publish ワークフロー（`cli-v<version>` タグ）が通過し、`Ksql.Linq.Cli` が nuget.org に反映されている。  
- [ ] パッケージ同梱検証（AI guide）が CI で有効になっている：`AI_ASSISTANT_GUIDE.md` が `.nupkg` に入っていない場合は workflow が fail する。  

---

## 9. Rollback / Emergency Policy (minimal)

NuGet publish is effectively irreversible (you can unlist, but the version is consumed). Prepare the following minimal policy:

- Library release has a critical bug:
  - Decide whether to **unlist** the broken version.
  - Start a hotfix release (`X.Y.(Z+1)`) with a minimal diff and clear release notes.
- CLI publish fails after library is already published:
  - Decide whether this is acceptable (library only) or blocks release.
  - If acceptable, publish CLI as soon as possible and communicate the delay (release notes / README / issue).
  - If not acceptable, stop and ship a coordinated hotfix release.


このチェックリストを満たしていることを各担当が報告し、天城が最終確認することで、リリース品質を安定して維持できる。 
