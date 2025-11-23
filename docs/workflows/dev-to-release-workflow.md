# Dev → Release End-to-End Workflow

このドキュメントは、**ブランチ作成 → 機能修正 → ドキュメント・Wiki更新 → RC 発行 → 安定版リリース** までの流れを 1 枚で俯瞰するためのガイドです。詳細は既存の各 workflow ドキュメントを参照してください。

関連ドキュメント:
- コード変更: `docs/workflows/code-change-workflow.md`
- ドキュメント / Wiki: `docs/workflows/docs-workflow.md`
- リリース: `docs/workflows/release-workflow.md`, `docs/release_publish_flow.md`

---

## 1. ブランチ作成とスコープ定義

1. 司令・天城が対象バージョンとスコープを決定する。
   - 例: `v0.9.7` の範囲（バグ修正 / 機能追加 /ドキュメント整備など）。
2. ブランチを作成する。
   - 安定リリース準備: `release/0.9.7`
   - 機能単位の試作: `feature/<機能名>`（必要に応じて）。
3. 変更の目的・背景を簡潔にメモ（Issue / 進捗ログなど）に残しておく。

---

## 2. 機能実装とテスト（コード変更フロー）

> 詳細: `docs/workflows/code-change-workflow.md`

1. 設計方針の整理
   - 鳴瀬が変更方針・影響範囲を把握し、必要であれば `docs/diff_log` に設計草案を追加。
2. 実装
   - 既存スタイルに合わせて C# コードを修正。
   - 不要な抽象化や将来のためだけの構造は避ける。
3. テスト
   - 迅人がユニットテストを追加 / 更新。
   - `tests/` 配下のテストを実行し、影響有無を確認（CI では `pr-validate.yml` / `test-workflow.md` に準拠）。
4. レビュー
   - 鏡花がコードレビューを実施し、品質基準に適合しているか確認。

---

## 3. ドキュメント / Wiki / diff_log 更新

> 詳細: `docs/workflows/docs-workflow.md`

1. `docs/` 更新
   - 新しい仕様・設計・CLI 変更などを `docs/` 配下に反映する。
   - 広夢: 利用者目線で文章を整理。
   - 鳴瀬: 技術的な正確性を確認。
2. Wiki 更新（別リポジトリ）
   - コードリポジトリ: `C:\dev\0.9.0\Ksql.Linq`
   - Wiki リポジトリ例: `C:\dev\Ksql.Linq.wiki`
   - `git -C ../../Ksql.Linq.wiki pull` で最新化し、対応する `*.md` / `_Sidebar.md` を編集 → commit/push。
3. diff_log 追加
   - 楠木が `docs/diff_log/diff_{機能名}_{YYYYMMDD}.md` を追加し、変更内容・背景・関連ドキュメントを記録。
4. Release Notes 更新
   - `docs/releases/release_vx_y_z.md` と `CHANGELOG.md` を更新。
   - NuGet 用の `PackageReleaseNotes` / `src/README.md` のリンク先が最新になっていることを確認。

---

## 4. RC リリース（GitHub Packages）

> 詳細: `docs/workflows/release-workflow.md`, `docs/release_publish_flow.md`

1. release ブランチ `release/0.9.7` 上で、テスト・PublicAPI チェック・ドキュメント更新が完了していることを確認。
2. RC タグを作成して push。
   - 例: `v0.9.7-rc1`
3. CI で次が自動実行される。
   - `publish-github-packages.yml`  
     - GitHub Packages へ `Ksql.Linq 0.9.7-rc1` を公開。
   - `examples-build.yml`（workflow_call 経由）  
     - RC パッケージを使って `examples/` をビルド。
4. RC 検証
   - 必要であれば実プロジェクトや examples を利用して動作確認。
   - 問題があれば `release/0.9.7` で修正 → 再度 `v0.9.7-rcN` タグを発行。

---

## 5. 安定版リリース（nuget.org）

1. RC で問題がないことを確認したら、`main` にマージ（または `release/0.9.7` を `main` に fast-forward）。
2. 安定版タグを作成して push。
   - 例: `v0.9.7`
3. CI で `nuget-publish.yml` が動作し、次を実行:
   - PublicAPI 厳格チェック付きビルド（`StrictPublicApi=true`）。
   - 単体テスト（Integration を除外）。
   - `Ksql.Linq` パッケージの pack と nuget.org への push。
4. 公開確認
   - nuget.org の `Ksql.Linq` ページで新しいバージョンと Release notes を確認。
   - GitHub Packages / Wiki / docs/ のリンクや説明が整合しているかを軽くチェック。

---

## 6. まとめ / ロール別観点

- 司令 / 天城: どのバージョンで何を入れるか、ブランチ運用とリリースGO/NO-GO の判断。
- 鳴瀬: 実装・テスト・API整合性（PublicAPI, examples, RC検証）。
- 広夢: README / docs / Release Notes / 外部向け記事との整合。
- 鏡花: CI結果・PublicAPIチェック・ドキュメントの矛盾確認。
- 楠木: diff_log と進捗ログの整理、Wiki 更新状況のトレース。

このドキュメントを「全体の地図」とし、個々のフェーズでは対応する `*_workflow.md` を参照して詳細手順を確認してください。

