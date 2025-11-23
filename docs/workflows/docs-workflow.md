# Documentation / Wiki Workflow

このドキュメントは、ドキュメントや Wiki の更新を行うための標準的なワークフローをまとめたものです。

## 対象と目的
- 対象:
  - `docs/` 配下ドキュメントの追加・更新
  - Wiki ページや `_Sidebar.md` の更新
  - diff_log エントリの追加
- 目的: ドキュメントと実装・テストを同期させ、履歴を保全する

## 役割
- 広夢: ドキュメント構成・表現の整理
- 楠木: diff_log と進捗ログの管理
- 鳴瀬: 技術内容の正確性レビュー
- 鏡花: ドキュメント品質（抜け・ブレ・矛盾）の確認

## 標準フロー
1. 更新対象の特定
   - 変更された機能や設計に対応して、必要なドキュメント・Wiki ページを洗い出す

2. ドキュメントの更新
   - 広夢が利用者目線で説明を追加・修正
   - 鳴瀬が技術的な正確性を確認

3. Wiki / Sidebar の更新
   - 必要に応じて Wiki ページを追加・編集
   - `_Sidebar.md` のリンク構造を更新し、リンク切れやマージコンフリクトがないことを確認

4. diff_log の更新
   - 楠木が `docs/diff_log/diff_{機能名}_{YYYYMMDD}.md` を作成し、今回の変更内容・背景・関連ドキュメントを記録

---

## ローカル Wiki リポジトリ運用ルール

Wiki は GitHub 本体とは別リポジトリ (`Ksql.Linq.wiki.git`) で管理されるため、ローカルでは次の構成を前提とする（例）:

- コードリポジトリ: `C:\dev\0.9.0\Ksql.Linq`
- Wiki リポジトリ: `C:\dev\Ksql.Linq.wiki`

### 更新タイミング

- `release/x.y.z` ブランチでの作業が完了し、ドキュメントや diff_log を更新したタイミング
- `docs/` 配下の設計ドキュメントや `docs/releases/release_vx_y_z.md` を更新したタイミング

### 手順（開発フローに組み込む）

1. Wiki リポジトリを最新化する
   - 上記の例のように配置している場合、`Ksql.Linq` から見て:
     - `git -C ../../Ksql.Linq.wiki pull`

2. 対応する Wiki ページを特定する
   - `docs/ai_assistant_wiki_mapping.md` や、各機能の設計ドキュメントから「どの Wiki ページに反映すべきか」を確認する

3. Wiki ページを更新する
   - `C:\dev\Ksql.Linq.wiki` 配下で対象の `*.md` を編集
   - 必要に応じて `_Sidebar.md` のリンクを追従

4. コミットと push
   - `git -C ../Ksql.Linq.wiki status` で差分を確認
   - `git -C ../Ksql.Linq.wiki commit -am "Update wiki for vX.Y.Z (feature ...)"`
   - `git -C ../Ksql.Linq.wiki push`

5. diff_log / Release Notes にリンクを残す
   - 関連する `docs/diff_log/diff_*.md` や `docs/releases/release_vx_y_z.md` に、必要であれば更新した Wiki ページへのリンクを記録する

