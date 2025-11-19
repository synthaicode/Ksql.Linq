# Ksql.Linq v0.9.5 追加設計メモ: appsettings.json と KSQL 出力オプション

## 1. 目的

- `appsettings.json` に記述されたトピック関連オプション（例: `retention.ms` など）を、デザイン時 KSQL スクリプト出力に反映できるようにする。
- Kafka / ksqlDB を起動せずに、「本番と同じトピック設定」を前提とした KSQL を生成できるようにする。

---

## 2. 前提と基本方針

- コアのゴール（`docs/designtime_ksql_script_plan_v0_9_5.md`）と同じく、「デザイン時 KsqlContext 生成」が入口。
- `IDesignTimeKsqlContextFactory` が返す `KsqlContext` が、どのように設定を取り込むかは **利用者側で選べる** ようにする。
  - `appsettings.json` / `appsettings.Development.json` などから読み込む。
  - C# コード内で直接オプションを組み立てる。
- ライブラリ側（`IKsqlScriptBuilder`）は、「`KsqlContext` が持っている有効なオプション」を KSQL の `WITH` 句に落とし込む役に徹する。

---

## 3. appsettings.json からのオプション反映イメージ

### 3-1. appsettings.json 側の例

```json
{
  "Ksql": {
    "Streams": {
      "Orders": {
        "Topic": "orders",
        "ValueFormat": "JSON",
        "RetentionMs": 604800000
      }
    }
  }
}
```

### 3-2. KsqlContext / モデル側

- `KsqlContext` もしくはその周辺で、上記設定を「エンティティごとのメタ情報」として保持する。
  - 例: `Topic`, `ValueFormat`, `RetentionMs`, `Partitions`, `Replicas` など。
- `IDesignTimeKsqlContextFactory` 実装内で `ConfigurationBuilder` を使う場合:
  - `new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: true)` などで読み込み。
  - 読み込んだ設定を `KsqlContext` のコンストラクタや初期化処理に渡す。

### 3-3. Script Builder 側の利用

- `IKsqlScriptBuilder` の実装は、`KsqlContext` 内のメタ情報から KSQL の `WITH` 句を組み立てる。
  - 例:  
    - `RETENTION_MS = 604800000`  
    - `KAFKA_TOPIC = 'orders'`  
    - `VALUE_FORMAT = 'JSON'`
- 設定が存在しない項目は、KSQL のデフォルト値またはライブラリ側の既定値にフォールバックする。

---

## 4. CLI との連携イメージ

- デフォルト:  
  - CLI（`dotnet ksql script`）は **特定の appsettings.json を直接は意識しない**。  
  - `IDesignTimeKsqlContextFactory` 実装内で、適切な設定ファイルを読み込む。
- 拡張オプション案（必要になった場合）:
  - `dotnet ksql script --appsettings ./appsettings.Production.json`
  - CLI が `--appsettings` で指定されたパスを `IDesignTimeKsqlContextFactory` に渡せるように API/引数を追加する。

---

## 5. 実装フェーズへの組み込み

- M1/M2 の範囲では、「`KsqlContext` が保持しているメタ情報を Script Builder がそのまま KSQL に落とす」形を先に完成させる。
- M3 以降または別マイルストーンとして、`appsettings.json` を前提としたサンプル実装・ドキュメントを追加する。
  - サンプル:  
    - `appsettings.json` に `Ksql:Streams:Orders:RetentionMs` を書く。  
    - `MyKsqlContextFactory` でそれを読み込み、`KsqlContext` に適用。  
    - `dotnet ksql script` の出力に `RETENTION_MS` 付きの `WITH` 句が現れることを確認。

