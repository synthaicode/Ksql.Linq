# デザイン時 KSQL スクリプト出力ワークフロー（v0.9.5）

## 1. ゴール

- Kafka / ksqlDB を起動せずに、ビルド済み DLL から `KsqlContext` を生成し、全 KSQL（DDL / CSAS / CTAS / SELECT）とトピックオプション（retention など）を含んだスクリプトを出力できるようにする。
- 生成した KSQL スクリプトをレビュー・差分比較・本番適用フローに安全に組み込めるようにする。

---

## 2. 開発者の運用イメージ

### 2-1. プロジェクトへの準備

- 手順:
  - アプリ側プロジェクトに `MyKsqlContext`（`KsqlContext` 派生）を実装しておく。
  - 同じプロジェクトに `MyKsqlContextFactory : IDesignTimeKsqlContextFactory` を追加する。
  - 必要に応じて `appsettings.json` にトピックオプション（`RetentionMs` など）を定義しておく。
- ポイント:
  - `MyKsqlContextFactory` は「本番起動」とは別経路で、**接続なし/設定最小限** で `KsqlContext` を構築する。
  - `KsqlContext` 内に、トピック名・フォーマット・retention などのメタ情報を保持できるようにする。

### 2-2. ローカルでのスクリプト生成

- 手順（例）:
  - `dotnet build` でアプリプロジェクトをビルドする。
  - 次のようなコマンドで KSQL を一括出力する:
    - `dotnet ksql script --project ./src/MyApp/MyApp.csproj --context MyKsqlContext --output ./ksql/generated.sql`
- 期待される結果:
  - `./ksql/generated.sql` に以下が含まれる:
    - 冒頭のヘッダコメント（Ksql.Linq バージョン／アセンブリ名・バージョン／生成日時）。
    - `CREATE STREAM/TABLE`, `CSAS/CTAS`, `SELECT` 文。
    - トピックオプション（retention など）を反映した `WITH` 句。
- よくある利用シーン:
  - 新しいストリーム/テーブルを追加した際に、デザイン時に生成された KSQL をレビューしてから本番に流す。

### 2-3. appsettings.json を使ったオプション反映

- 手順（イメージ）:
  - `appsettings.json` にトピックごとの設定を記述する。
  - `MyKsqlContextFactory` で `appsettings.json` を読み込み、`KsqlContext` 初期化時にメタ情報を渡す。
  - Script Builder が、そのメタ情報を `WITH` 句に変換して KSQL に埋め込む。
- ポイント:
  - 「本番と同じ appsettings を前提にした KSQL」をデザイン時に確認できる。
  - Kafka / ksqlDB に接続せずに retention やフォーマットの設定漏れを検知しやすくする。

---

## 3. チーム/CI での運用イメージ

### 3-1. プルリクエストでのレビュー

- 手順:
  - 開発者がローカルで `dotnet ksql script` を実行し、`ksql/generated.sql` をコミット（または PR に添付）する。
  - レビュアーは、コードだけでなく生成された KSQL の差分も確認する。
- メリット:
  - 「コードは正しそうだが、KSQL の実スクリプトが意図通りか？」を事前に確認できる。
  - retention やフォーマットの設定変更が KSQL にどう反映されるかを PR 上で確認できる。

### 3-2. CI での自動生成と差分チェック（将来的な運用）

- 手順（将来案）:
  - CI ジョブ内で `dotnet build` と `dotnet ksql script` を実行し、KSQL スクリプトを生成する。
  - 生成結果をアーティファクトとして保存し、必要に応じて過去のスクリプトとの diff をとる。
- 期待効果:
  - スキーマやトピック設定の変更が、いつ・どのリリースで入ったかをトレースしやすくなる。
  - 本番に流す KSQL とソースコードバージョンの対応を CI ログから辿れる。

---

## 4. 本番適用フローとのつなぎ方（イメージ）

- パターン例:
  - ステージング環境:
    - デザイン時に生成した KSQL をステージングの ksqlDB に適用し、動作確認を行う。
  - 本番環境:
    - リリースブランチのビルドアーティファクトから KSQL を生成し、運用手順に従って適用する。
- トレーサビリティ:
  - スクリプト先頭のヘッダにより、「どのアセンブリバージョン/いつ生成された KSQL か」を確認できる。
  - 誤った古いスクリプトを適用するリスクを下げる。

---

## 5. 参考ドキュメント

- 設計詳細:
  - `docs/designtime_ksql_script_plan_v0_9_5.md`
  - `docs/designtime_ksql_script_plan_v0_9_5_appsettings_retention.md`
- 関連ワークフロー:
  - `docs/workflows/code-change-workflow.md`
  - `docs/workflows/test-workflow.md`

