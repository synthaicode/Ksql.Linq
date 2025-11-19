# Ksql.Linq v0.9.5 設計メモ: Schema Registry とデザイン時 KSQL 出力

## 1. 目的

- 「value 側に Schema Registry 登録済みクラスが入る」前提でも、Kafka / ksqlDB を起動せずに KSQL スクリプトを生成できるようにする。
- デザイン時 KSQL 出力に、**フォーマット種別・Subject 名などのメタ情報**を反映し、必要に応じて **実際の Schema Registry と接続して検証情報を付与**できる構成にする。

---

## 2. 基本方針（2レイヤー構成）

1. **メタ情報レイヤー（オフライン前提）**  
   - 型（エンティティ）に対して、「どの Subject / Format を使うか」の情報を `KsqlContext` のモデルに格納する。  
   - デザイン時 KSQL スクリプトには、このメタ情報を **WITH 句およびコメント** として反映する。  
   - Schema Registry へのネットワーク接続は不要。

2. **検証レイヤー（オンライン・任意）**  
   - CLI オプション（例: `--include-schema-registry`）を指定した場合のみ、Schema Registry に接続して Subject の登録状態やバージョンを取得する。  
   - 取得結果は KSQL スクリプトのコメント（`-- Subject: foo-value v12 (id=123)` など）として付与する。  
   - 本仕様では「存在確認・バージョン取得」のみを想定し、スキーマ本文のインライン展開までは行わない。

---

## 3. モデルへのメタ情報の持たせ方（オフライン）

### 3-1. 設定の入口

- 想定する指定方法:
  - 属性ベース:  
    - 将来的な例: `[KsqlSchema(ValueFormat = "AVRO", Subject = "my-topic-value")]`  
  - 設定ベース (`appsettings.json` / `KsqlDsl.Entities`):  
    - 例:  
      ```json
      "KsqlDsl": {
        "Entities": [
          {
            "Entity": "OrderEvent",
            "ValueFormat": "Avro",
            "ValueSchemaSubject": "orders-value"
          }
        ]
      }
      ```
- `IDesignTimeKsqlContextFactory` から生成される `KsqlContext` は、これらの設定を読み込み、エンティティごとのメタ情報として内部に保持する。

### 3-2. Script Builder での利用

- `IKsqlScriptBuilder` 実装は、エンティティのメタ情報から以下のような WITH 句を構成可能とする:
  - `VALUE_FORMAT = 'AVRO'`
  - `VALUE_SCHEMA_SUBJECT = 'orders-value'`（命名はライブラリ側で決定）
- メタ情報が存在しない場合は、既存のデフォルト（JSON など）または設定の既定値にフォールバック。
- スクリプト先頭コメントや各 CREATE 文の直前に、メタ情報の要約をコメントとして追加可能とする:
  - 例: `-- Schema: Subject=orders-value, Format=Avro`

---

## 4. Schema Registry 検証オプション（オンライン）

### 4-1. CLI オプション案

- 例:
  - `dotnet ksql script --include-schema-registry`
  - `dotnet ksql script --include-schema-registry --schema-timeout 00:00:10`
- 動作:
  1. `IDesignTimeKsqlContextFactory` から `KsqlContext` を取得。
  2. `KsqlDsl.SchemaRegistry.Url`（および必要な認証情報）から SchemaRegistryClient を構築。
  3. モデルに格納された Subject 名ごとに:
     - 対象 Subject が存在するか確認。
     - 最新 version / id を取得。
  4. KSQL スクリプトにコメントとして付与:
     - 例:  
       ```sql
       -- SchemaRegistry: Subject=orders-value, Version=12, Id=345
       CREATE STREAM ...
       ```
- 失敗時の扱い:
  - オプション使用時にエラーが発生した場合:
    - ベースの KSQL 生成は継続し、Schema Registry に関するコメント部分のみ警告コメントを出す（`-- SchemaRegistry: lookup failed (reason)`）。
    - CLI の戻り値/ログで警告を明示する。

---

## 5. サンプルへの反映イメージ

### 5-1. `examples/designtime-ksql-script`

- `OrderEvent` に対し、`appsettings.json` または将来の属性で:
  - `ValueFormat = Avro`
  - `ValueSchemaSubject = orders-value`
  を設定。
- デザイン時 KSQL スクリプト出力例（イメージ）:
  ```sql
  -- Schema: Subject=orders-value, Format=Avro
  -- SchemaRegistry: Subject=orders-value, Version=12, Id=345   -- （オプション使用時）
  CREATE STREAM Orders ...
    WITH (
      KAFKA_TOPIC = 'orders',
      VALUE_FORMAT = 'AVRO',
      VALUE_SCHEMA_SUBJECT = 'orders-value'
    );
  ```

### 5-2. `examples/designtime-ksql-tumbling`

- `Tick` / `MinuteBar` についても同様に ValueFormat/Subject メタ情報を持たせることで、Tumbling ビューの KSQL に Schema Registry 情報を含めた形で出力できる。

---

## 6. 実装フェーズへの組み込み

- v0.9.5 の範囲:
  - モデルに ValueFormat / ValueSchemaSubject 相当のメタ情報を持てるようにする。
  - Script Builder から WITH 句およびコメントとしてメタ情報を反映できるようにする。
- その後の拡張:
  - CLI に `--include-schema-registry` オプションを追加し、`KsqlDsl.SchemaRegistry` 設定を用いたオンライン検証を実装する。
  - 必要に応じて、「Schema Registry バージョンの固定」や「特定 version/id を前提とした KSQL コメント付与」などを検討する。

