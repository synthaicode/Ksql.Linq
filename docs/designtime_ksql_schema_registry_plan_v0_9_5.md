# Ksql.Linq v0.9.5 実装計画: デザイン時 KSQL と Schema Registry 連携

## 1. ゴール

- デザイン時 KSQL スクリプト出力において、Schema Registry を前提とした value 側クラスを正しく扱えるようにする。
- Kafka / ksqlDB を起動せずに、以下を実現する:
  - ValueFormat（Avro/Json 等）と Subject 名を WITH 句＋コメントに反映。
  - オプション指定時に Schema Registry へ接続し、Subject の存在確認・version/id の取得結果をスクリプトにコメントとして付与。

---

## 2. 実装フェーズ概要

### M1. モデル拡張（メタ情報レイヤー）

**目的**: `KsqlContext` の内部モデルに、Schema Registry 関連のメタ情報（ValueFormat / Subject）を安全に保持できるようにする。

- 作業項目:
  - `Configuration.ResolvedEntityConfig` など、エンティティごとの解決済み設定に以下を追加する方向で検討:
    - `ValueFormat`（列挙または文字列）
    - `ValueSchemaSubject`（文字列）
  - `KsqlDsl.Entities` 設定から上述のプロパティへ値を流し込めるようにする。
    - `appsettings.json` の例を docs 側に追加しつつ、既存の設定読み込みパスに影響が出ないように拡張。
  - 将来の属性ベース指定（例: `[KsqlSchema(ValueFormat = "Avro", Subject = "orders-value")]`）を想定しつつ、現時点では設定ベースにフォーカス。

### M2. Script Builder 拡張（WITH 句・コメント出力）

**目的**: モデルに載った Schema Registry メタ情報を、デザイン時 KSQL スクリプトに反映する。

- 作業項目:
  - `IKsqlScriptBuilder` / `KsqlScript` 実装（または予定の実装）に、以下を追加:
    - 各 `CREATE STREAM/TABLE` 文に対して、対象エンティティの `ValueFormat` / `ValueSchemaSubject` を確認。
    - WITH 句に `VALUE_FORMAT='AVRO'` などを追記（既存の ValueFormat 出力ロジックがあれば統合）。
    - `VALUE_SCHEMA_SUBJECT='orders-value'` のようなキー名を決定し、WITH 句に追加。
  - コメント出力:
    - `-- Schema: Subject=orders-value, Format=Avro` のような 1 行コメントを `CREATE` 文の直前に出力。
  - 既存生成ロジックとの整合性確認（SchemaRegistryTools/WithClauseBuilder 等との責務分割を明確化）。

### M3. CLI 拡張（Schema Registry オプション）

**目的**: オプション指定時に Schema Registry へ問い合わせ、Subject 単位で version/id を取得してスクリプトにコメントとして反映する。

- 作業項目:
  - CLI（`dotnet ksql` 想定）の引数設計:
    - `--include-schema-registry`
    - `--schema-timeout 00:00:10`（任意）
  - 実装フロー:
    1. `IDesignTimeKsqlContextFactory` から `KsqlContext` を取得。
    2. `KsqlDsl.SchemaRegistry.Url` や認証情報から `ISchemaRegistryClient` を構築。
    3. モデルに保持している `ValueSchemaSubject` 一覧を走査し、各 Subject について:
       - 存在確認。
       - 最新 version / id を取得。
    4. `KsqlScript` 生成時に、対応するエンティティの `CREATE` 直前にコメント出力:
       - 例: `-- SchemaRegistry: Subject=orders-value, Version=12, Id=345`
  - 失敗時の扱い:
    - 例外はキャッチし、KSQL 生成は継続。
    - コメント行に `-- SchemaRegistry: lookup failed (reason)` を残し、CLI のログ/戻り値で警告扱いとする。

### M4. サンプル更新とドキュメント同期

**目的**: 実装された機能を、既存サンプルとドキュメントに反映し、利用者が迷わず使える状態にする。

- 作業項目:
  - `examples/designtime-ksql-script` / `examples/designtime-ksql-tumbling`:
    - `appsettings.json` へ `KsqlDsl.Entities` の Schema Registry 関連設定例（ValueFormat / ValueSchemaSubject）を追加。
    - README に「Schema Registry 前提でのデザイン時 KSQL 出力」の説明と、将来の `--include-schema-registry` 利用例を追記。
  - docs:
    - 本計画に沿って `designtime_ksql_schema_registry_design_v0_9_5.md` を見直し、実装と乖離があれば更新。
    - デザイン時 KSQL ワークフロー (`docs/workflows/designtime-ksql-script-workflow.md`) に、Schema Registry オプション利用時の流れを 1 セクション追加。

---

## 3. マイルストーンと優先度

1. **M1 → M2** はセットで最優先（オフライン前提でのメタ情報反映までを v0.9.5 のコアとする）。  
2. **M3** は CLI 実装状況と連動して進める（最悪、v0.9.6 以降にスライド可能なように分離）。  
3. **M4** は機能完成後にまとめて対応し、examples/README/ワークフロードキュメントが常に最新状態になるようにする。  

この順序で進めることで、まず「Schema Registry を前提とした KSQL スクリプトの形」を早期に固定し、その後でオンライン検証（version/id 取得）を段階的に追加していく。

