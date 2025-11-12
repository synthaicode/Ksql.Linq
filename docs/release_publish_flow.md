# Ksql.Linq リリース/公開フロー（RC → 安定版）

このドキュメントは、GitHub Packages へのプレ公開（RC）と、nuget.org への安定版公開の一連の手順をまとめたものです。最短・確実に通すための事前チェックと、各CIの役割、タグ運用、典型的な失敗パターンを含みます。

---

## 用語と前提
- RC: Release Candidate（例: `v0.9.1-rc4`）。GitHub Packages へ公開して実地検証する段階。
- 安定版: 例 `v0.9.1`。nuget.org へ正式公開（取り消し・上書き不可）。
- main: 公開対象コミットを常に main 上に置く。タグは必ず main を Target に作成。
- Secrets（Repository secrets）
  - `NUGET_API_KEY`: nuget.org の API キー
  - `GITHUB_TOKEN`: Actions が自動で供給。GitHub Packages への push に使用（packages:write 権限）。

---

## CI の役割整理
- GitHub Packages への公開（pre/RC）
  - Workflow: `Publish to GitHub Packages (NuGet)`（publish-github-packages.yml）
  - トリガー: `v*.*.*-rc.*`（RCタグ）/ workflow_dispatch
  - Public API 厳格チェック: 無効（RCでブロックしない）
  - 認証: `dotnet nuget add source --username ${{ github.actor }} --password ${{ secrets.GITHUB_TOKEN }}`

- nuget.org への公開（安定）
  - Workflow: `Build, Test, Pack, Publish (main only)`（nuget-publish.yml）
  - トリガー: `v*.*.*`（安定タグ）
  - Public API 厳格チェック: 有効（`-p:StrictPublicApi=true -warnaserror:RS0016,RS0017`）
  - 認証: `NUGET_API_KEY`

---

## フロー全体（概要）
1) RC を GitHub Packages に公開（カナリア検証）
2) RC のパッケージで examples/wiki 手順のスモークテスト（restore/ビルド）
3) 問題なければ、安定版タグを作成して nuget.org に公開

---

## 1. RC 公開（GitHub Packages）
事前チェック（RC）
- [ ] 公開したいコミットが `main` にある
- [ ] 新しい未使用のタグ名（例: `v0.9.1-rc4`）

手順（GitHub UI）
- Repo → Releases → New release
- Choose a tag → 新規作成 `v0.9.1-rc4`
- Target: `main`
- Publish release

期待されるCIの挙動
- Actions → `Publish to GitHub Packages (NuGet)` が起動
- Pack → Push（GPR）が緑（成功）

RCパッケージの利用確認（任意）
- `NuGet.config` に GitHub Packages を追加したクリーン環境で `dotnet restore` が通るか
- examples プロジェクトをそのバージョンへ固定してビルド確認

---

## 2. 安定版公開（nuget.org）
事前チェック（必須）
- [ ] 公開したいコミットが `main` にある
- [ ] 新しい未使用のタグ名（例: `v0.9.1`）
- [ ] ローカルで厳格チェック（CIと同条件）
  - `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`
  - 失敗した場合は `src/PublicAPI.*.txt` を同期（不足を Unshipped に追加、重複は解消）または internal 化して再実行

手順（GitHub UI）
- Repo → Releases → New release
- Choose a tag → 新規作成 `v0.9.1`
- Target: `main`
- Publish release

期待されるCIの挙動
- Actions → `Build, Test, Pack, Publish (main only)` が起動
- 「Public API strict check (stable tags only)」が緑
- Pack → `Push nupkg/snupkg to nuget.org` が緑
- 数分後に https://www.nuget.org/packages/Ksql.Linq/ に反映

---

## よくあるエラーと対処
- RS0016/RS0017（Public API差分）
  - 安定版のみブロック。ローカルの厳格チェックで事前に潰す。
  - 直し方: `src/PublicAPI.Unshipped.txt` に不足を追加／重複は Shipped/Unshipped を整理／不要APIは internal 化。

- MSB1006: Property is not valid. Switch: RS0017
  - セミコロン分割が原因。CIは `-warnaserror:RS0016,RS0017` に統一済み。

- “Tag commit is NOT on main; skip publish”
  - タグの Target が main でない。Releases で main を選び直し、新しいタグ名で再作成。

- `skip-duplicate`
  - 既に同バージョンが存在。タグ番号（`-rcN` か `Z`）をインクリメントして再実行。

- GitHub Packages 401 Unauthorized
  - 認証方式の不一致が原因。CIは `nuget add source --username ${{ github.actor }} --password ${{ secrets.GITHUB_TOKEN }}` に統一済み。

- PackageVersion=main などで pack 失敗
  - どこかで `PackageVersion` が環境に入っている。手元では `unset PackageVersion`（pwshは `Remove-Item Env:PackageVersion`）。CIは `-p:PackageVersion=` を明示指定済み。

---

## タグ運用の注意
- タグは再利用しない（同名再発行や re-run は古いYAML/古いコミットで走る原因）
- RC と安定で役割を分離
  - RC: GitHub Packages での動作確認／やり直し可
  - 安定: nuget.org へ正式公開（不可逆）

---

## ローカル検証コマンド（控え）
- ビルド: `dotnet build -c Release`
- 厳格PublicAPI（安定版前）: `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`
- パック確認: `dotnet pack src/Ksql.Linq.csproj -c Release --no-build -p:PackageVersion=0.9.1-rcX`

---

## 成功チェックリスト
- [ ] RC: GitHub Packages へ push 成功／クリーン環境で restore/ビルドOK
- [ ] 安定: ローカル厳格チェックRS0016/17=0
- [ ] 安定: Actions の「Public API strict check」「Push to nuget.org」が緑
- [ ] nuget.org のパッケージページに新バージョンが表示

---

## 変更履歴
- 2025-11-12: CI整理（RCで厳格OFF／安定のみ厳格ON、GPR認証修正、セミコロン問題解消、タグ運用ポリシー反映）

