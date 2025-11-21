# Release Notes v0.9.7

## Breaking Changes
- `KsqlDslOptions.DeserializationErrorPolicy` と `ReadFromFinalTopicByDefault` を `{ get; init; }` に変更（初期化時のみ設定可能）。コードで後からセットしていた場合は初期化に寄せること。
- ksql の待機設定は `KsqlDslOptions` を最優先し、環境変数フォールバックを廃止。環境変数 `KSQL_QUERY_RUNNING_*` に依存していた場合は appsettings で明示設定すること。
- PublicAPI: 既存の Unshipped 定義を Shipped に移動し、Unshipped は空にリセット。API差分の基準点が更新された。

## New Features / Changes
- AI支援ドキュメントをパッケージ同梱: `AI_ASSISTANT_GUIDE.md` と制約集 `docs/sql_constraint_violations_100_patterns.md` を `docs/` 配下に含める。README に誘導を追加。
- 待機ロジックの設定統一: RUNNING判定の連続回数/ポーリング間隔/安定化ウィンドウは `KsqlDslOptions` の値を使用し、未設定時のみデフォルト値（Consecutive=5, Interval=2000ms, Stability=15s, Timeout=180s）を適用。
- デフォルト値の単一管理: `KsqlDslOptions` から `DefaultValue` 属性を削除し、初期化子のみでデフォルトを管理。

### Key KsqlDslOptions (defaults)
- `KsqlQueryRunningConsecutiveCount`: 5  
- `KsqlQueryRunningPollIntervalMs`: 2000  
- `KsqlQueryRunningStabilityWindowSeconds`: 15  
- `KsqlQueryRunningTimeoutSeconds`: 180  
- `KsqlWarmupDelayMs`: 3000  
- `KsqlDdlRetryCount`: 5  
- `KsqlDdlRetryInitialDelayMs`: 1000  
- `AdjustReplicationFactorToBrokerCount`: true  
- `KsqlSimpleEntityWarmupSeconds`: 15 / `KsqlQueryEntityWarmupSeconds`: 10 / `KsqlEntityDdlVisibilityTimeoutSeconds`: 12  
- `DeserializationErrorPolicy`: Skip  
- `ReadFromFinalTopicByDefault`: false  
- `DecimalPrecision`: 18 / `DecimalScale`: 2  

## Migration Guide
- 環境変数 `KSQL_QUERY_RUNNING_*` に依存していた場合は、appsettings の `KsqlDsl` セクションに同名項目を設定する（未設定なら上記デフォルトが適用される）。
- `DeserializationErrorPolicy` / `ReadFromFinalTopicByDefault` を code で後付けセットしていた場合は、オプション初期化時に設定する形へ変更する。
- PublicAPI 基準点が Shipped に移ったため、以降のAPI差分は Unshipped に追加して管理する。

## Known Issues
- 現時点で新規の既知問題はありません。既存の待機動作は設定値変更により挙動が変わる場合があるため、必要に応じてタイミングを確認してください。
