# Release Workflow (Ksql.Linq)

このドキュメントは、Ksql.Linq のリリース作業を AI / 人間メンバーが一貫した手順で実行するためのワークフロー定義です。

## 対象と目的
- 対象: ライブラリの RC リリース / 安定版リリース
- 目的: `docs/release_publish_flow.md` に沿って、安全に NuGet / GitHub Packages へ公開する

## 役割
- 司令: リリースの GO/NO-GO とバージョン決定
- 天城: 全体進行管理、タスク割り振り
- 鳴瀬: コード最終確認（Public API、仕様差分チェック）
- 鏡花: 品質ゲート確認（ビルド / テスト / Public API チェック結果）
- 楠木: 進捗ログ・diff_log への記録

## 標準フロー
1. リリース計画の確認
   - 司令と天城が対象バージョン（例: `v0.9.1-rc4` / `v0.9.1`）とスコープを合意
   - 既存の diff_log を確認し、リリースノート対象を整理

2. 事前チェック
   - 鳴瀬が `src/PublicAPI.*.txt` を含む Public API の状態を確認
   - 手元でのビルド / テスト / strict Public API ビルドを実行

3. RC もしくは安定版のタグ作成
   - `docs/release_publish_flow.md` の「1. RC 」「2. 本番リリース」に従い GitHub Releases からタグを作成

4. CI の実行と確認
   - 対象 Workflow:
     - GitHub Packages 向け RC: `publish-github-packages.yml`
     - nuget.org 向け本番: `nuget-publish.yml`
   - 鏡花が CI 結果（Public API strict check 含む）をレビューし、問題がないことを確認

5. 公開結果の確認
   - GitHub Packages / nuget.org 上のパッケージが期待どおりに更新されていることを確認

6. 記録
   - 楠木が docs/diff_log に必要に応じて差分を追記
   - リリースノートや外部向け情報は広夢が整理

## 詳細仕様の参照
- 手順詳細は `docs/release_publish_flow.md` を参照すること

