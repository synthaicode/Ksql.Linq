# Ksql.Linq v0.9.6 Runtime Tuning Plan

目的: 本番運用での「待ち時間」「リトライ」「安定化待ち」を appsettings.json から調整できるようにし、環境ごとのチューニングをコード変更なしで行えるようにする。

---

## 1. 既に外部化済みの項目（復習）

- **DDL リトライ（SchemaRegistrar → ksqlDB）**
  - `KsqlDsl.KsqlDdlRetryCount`  
    - 説明: DDL（CREATE STREAM/TABLE / CSAS / CTAS）を失敗時に再試行する回数。  
    - 既定値: 5（実際の試行回数は `KsqlDdlRetryCount + 1`）。
  - `KsqlDsl.KsqlDdlRetryInitialDelayMs`  
    - 説明: DDL リトライ用の初期待機（ミリ秒）。指数バックオフの起点。  
    - 既定値: 1000ms。

- **RUNNING 判定まわり（SHOW QUERIES ベース）**
  - `KsqlDsl.KsqlQueryRunningConsecutiveCount`  
    - 説明: SHOW QUERIES で RUNNING を連続何回観測したら「安定」とみなすか。  
    - 既定値: 5。
  - `KsqlDsl.KsqlQueryRunningPollIntervalMs`  
    - 説明: RUNNING 判定のために SHOW QUERIES を投げる間隔（ミリ秒）。  
    - 既定値: 2000ms。
  - `KsqlDsl.KsqlQueryRunningStabilityWindowSeconds`  
    - 説明: 必要回数 RUNNING を満たしたあと、追加でどれだけの安定化時間を置いて再チェックするか（秒）。  
    - 既定値: 15秒。

これらはすでに `KsqlDsl` セクションにマップ済みで、環境ごとに調整可能。

---

## 2. 追加で外部化したい候補

ここからは「0.9.6 で外部化を検討する項目」として整理する。

### 2.1 クエリ安定化の全体タイムアウト

- 現状:
  - `GetQueryRunningTimeout()` の既定値は 180 秒。  
  - 環境変数 `KSQL_QUERY_RUNNING_TIMEOUT_SECONDS` で上書き可能だが、appsettings からは直接制御できない。
- 課題:
  - 環境ごとに「どこまで待つか」の許容値が異なる（本番は長め、検証環境は短めにしたい）。
- 案:
  - `KsqlDslOptions` に `KsqlQueryRunningTimeoutSeconds` を追加し、`KsqlContext` 側で優先的に参照。  
  - 環境変数は fallback（appsettings 無設定時の上書き手段）として残す。

### 2.2 DDL フェーズのウォームアップ時間

- 現状:
  - `SchemaRegistrar` 内で、DDL 前に以下の固定値を使用している。  
    - `WarmupKsqlWithTopicsOrFail(TimeSpan.FromSeconds(15), ...)`（シンプル DDL 前）  
    - `WarmupKsqlWithTopicsOrFail(TimeSpan.FromSeconds(10), ...)`（クエリ定義 DDL 前）  
    - `WaitForEntityDdlAsync(..., TimeSpan.FromSeconds(12))` など。  
  - 一方で `KsqlDslOptions.KsqlWarmupDelayMs` は、別のウォームアップ（初回 DDL までの待機）に使っている。
- 課題:
  - これらの秒数は環境依存度が高く、本番/検証で値を変えたいことが多い。  
  - 固定値のままだと、ksqlDB が重い環境でタイムアウトしやすい。
- 案:
  - `KsqlWarmupDelayMs` を含め、ウォームアップ関連を以下に整理する。  
    - `KsqlWarmupDelayMs`（既存）: 初回 DDL まで待つベースの遅延。  
    - `KsqlSimpleEntityWarmupSeconds`（新規）: シンプルエンティティ DDL 前の Warmup 秒数。  
    - `KsqlQueryEntityWarmupSeconds`（新規）: クエリ定義エンティティ DDL 前の Warmup 秒数。  
    - `KsqlEntityDdlVisibilityTimeoutSeconds`（新規）: `WaitForEntityDdlAsync` でメタデータ反映を待つ最大秒数。
  - 実装上は、既存の固定 `TimeSpan.FromSeconds(...)` を `KsqlDslOptions` 経由の値に置き換える。

### 2.3 ksqlDB HTTP クライアントのタイムアウト

- 現状:
  - `KsqlDbClient` は `TimeSpan.FromSeconds(60)` を fallback として利用している箇所がある（ksqlDB REST 呼び出し）。
- 課題:
  - ネットワークやクラスタが重い環境では、60 秒では足りないケースもある一方、開発環境では短くしたいこともある。
- 案:
  - `KsqlServerOptions` または `KsqlDsl` 直下に `KsqlHttpTimeoutSeconds` を追加し、`KsqlDbClient` の `CancellationTokenSource` 作成時に使用。  
  - 未設定の場合は従来どおり 60 秒を fallback とする。

---

## 3. appsettings.json の構成例

運用でよく調整しそうな項目をまとめた例:

```json
{
  "KsqlDsl": {
    "KsqlDbUrl": "http://127.0.0.1:18088",

    // DDL リトライ
    "KsqlDdlRetryCount": 5,
    "KsqlDdlRetryInitialDelayMs": 1000,

    // RUNNING 判定
    "KsqlQueryRunningConsecutiveCount": 5,
    "KsqlQueryRunningPollIntervalMs": 2000,
    "KsqlQueryRunningStabilityWindowSeconds": 15,

    // クエリ安定化タイムアウト（候補）
    "KsqlQueryRunningTimeoutSeconds": 180,

    // ウォームアップ関連（候補）
    "KsqlWarmupDelayMs": 3000,
    "KsqlSimpleEntityWarmupSeconds": 15,
    "KsqlQueryEntityWarmupSeconds": 10,
    "KsqlEntityDdlVisibilityTimeoutSeconds": 12,

    // ksqlDB HTTP タイムアウト（候補）
    "KsqlHttpTimeoutSeconds": 60
  }
}
```

> ※ 「候補」と付けたキーは v0.9.6 での導入検討項目。実装時は実際のプロパティ名・デフォルト値と同期させる。

---

## 4. 今後の方針

- **優先度高**
  - `KsqlQueryRunningTimeoutSeconds` を `KsqlDslOptions` に追加し、環境変数より優先する。
  - `KsqlWarmupDelayMs` と SchemaRegistrar 内の Warmup 秒数を整理し、すべてオプション化する。
- **優先度中**
  - `KsqlHttpTimeoutSeconds` を導入し、`KsqlDbClient` の REST 呼び出しに反映。
- **優先度低**
  - 物理テスト側（physicalTests）の Wait/Retry パラメータも appsettings ベースに寄せる（現在はスクリプト/コード直書きが多い）。
