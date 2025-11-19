# デザイン時 KSQL / Avro スキーマ出力ガイド（v0.9.5）

## 1. 目的

- アプリを起動せずに、`KsqlContext` から **KSQL DDL スクリプト** と **Avro スキーマ（.avsc 相当）** を生成する方法をまとめます。
- Kafka / ksqlDB / Schema Registry が起動していない環境でも、`appsettings.json` と POCO モデルから「何が流れるか」を把握できることを目的とします。

---

## 2. 前提となるコンポーネント

- ライブラリ側（`Ksql.Linq`）
  - `IDesignTimeKsqlContextFactory`
    - デザイン時専用の `KsqlContext` 生成ファクトリ。
  - `DefaultKsqlScriptBuilder` (`Ksql.Linq.Query.Script`)
    - `KsqlContext` から KSQL DDL スクリプトを構築。
    - `CREATE STREAM/TABLE`, `CSAS/CTAS` と `WITH (...)` 句（`KAFKA_TOPIC` / `VALUE_FORMAT` / `PARTITIONS` / `REPLICAS` / `RETENTION_MS` / `VALUE_SCHEMA_SUBJECT` 等）を出力。
    - スクリプトヘッダに `GeneratedBy`（Ksql.Linq のバージョン）・`TargetAssembly`・`GeneratedAt` をコメントとして埋め込む。
  - `DefaultAvroSchemaExporter` (`Ksql.Linq.Query.Script`)
    - `KsqlContext` 内の `MappingRegistry` から、各エンティティの **value 側 Avro スキーマ** を JSON 文字列として取得。
    - Schema Registry には接続せず、「この DLL バージョンが前提にしている Avro スキーマ」をオフラインで再現。

- サンプル（`examples/designtime-ksql-script` / `examples/designtime-ksql-tumbling`）
  - デザイン時コンテキストとファクトリ実装（`OrdersKsqlContext` / `TumblingKsqlContext` + `*DesignTimeKsqlContextFactory`）。
  - `Program.cs` から ScriptBuilder / AvroSchemaExporter を呼び出し、標準出力に結果を表示。

---

## 3. アプリ側の実装パターン

### 3-1. デザイン時コンテキストとファクトリ

1. `KsqlContext` 派生クラスに `IsDesignTime` / `SkipSchemaRegistration` を override した「デザイン時専用コンテキスト」を用意します。

```csharp
public sealed class MyKsqlContext : KsqlContext
{
    public MyKsqlContext(IConfiguration configuration, ILoggerFactory? loggerFactory = null)
        : base(configuration, loggerFactory) { }

    public EventSet<MyEntity> MyEntities { get; set; } = null!;

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MyEntity>();
    }

    protected override bool IsDesignTime => true;
    protected override bool SkipSchemaRegistration => true;
}
```

2. `IDesignTimeKsqlContextFactory` を実装し、デザイン時に `KsqlContext` を生成できる入口を定義します。

```csharp
public sealed class MyDesignTimeKsqlContextFactory : IDesignTimeKsqlContextFactory
{
    public KsqlContext CreateDesignTimeContext()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        return new MyKsqlContext(configuration);
    }
}
```

### 3-2. appsettings.json と KsqlDsl 設定

- `KsqlDsl` セクションに接続情報とトピック設定を記述します。
- 例（`examples/designtime-ksql-script/appsettings.json` より）:

```json
{
  "KsqlDsl": {
    "Common": {
      "BootstrapServers": "localhost:9092",
      "ClientId": "designtime-ksql-script-sample"
    },
    "SchemaRegistry": {
      "Url": "http://localhost:8085"
    },
    "KsqlDbUrl": "http://localhost:8088",
    "Topics": {
      "orders_v1": {
        "Creation": {
          "NumPartitions": 1,
          "ReplicationFactor": 1,
          "Configs": {
            "retention.ms": "604800000"
          }
        }
      }
    },
    "Entities": [
      {
        "Entity": "OrderEvent",
        "SourceTopic": "orders_v1"
      }
    ]
  }
}
```

この設定は wiki (`Appsettings.md` / `Configuration-Reference.md`) と整合しており、`Topics.<topic>.Creation` や `Entities` の意味も同一です。

---

## 4. デザイン時 KSQL スクリプトの生成（ライブラリ経由）

### 4-1. コードからの呼び出し例

```csharp
var factory = new MyDesignTimeKsqlContextFactory();
using var context = factory.CreateDesignTimeContext();

var scriptBuilder = new DefaultKsqlScriptBuilder();
var script = scriptBuilder.Build(context);

Console.WriteLine(script.ToSql());
```

### 4-2. 出力イメージ

`examples/designtime-ksql-script` の実行例:

```sql
-- Design-time KSQL script for OrdersKsqlContext
-- GeneratedBy: Ksql.Linq 0.0.0.0
-- TargetAssembly: DesigntimeKsqlScript 1.0.0.0
-- GeneratedAt: 2025-11-18T22:52:53.4163543+09:00

-- Schema: Subject=orders_v1-value, Entity=OrderEvent
-- Namespace: Examples.DesigntimeKsqlScript
CREATE STREAM orders_v1 WITH (KAFKA_TOPIC='orders_v1', VALUE_FORMAT='AVRO', PARTITIONS=1, REPLICAS=1, VALUE_SCHEMA_SUBJECT='orders_v1-value');

-- Schema: Subject=orders_summary_v1-value, Entity=OrderSummary
-- Namespace: Examples.DesigntimeKsqlScript
CREATE STREAM IF NOT EXISTS ordersummary  WITH (KAFKA_TOPIC='orders_summary_v1', VALUE_FORMAT='AVRO', PARTITIONS=1, REPLICAS=1, VALUE_SCHEMA_SUBJECT='orders_summary_v1-value') AS
SELECT o.ID AS Id, o.CREATEDAT.DATE AS CreatedDate
FROM ORDERS o
WHERE (Status = 'Completed')
EMIT CHANGES;
```

- ヘッダ:
  - `GeneratedBy`: 使用した Ksql.Linq のアセンブリ名・バージョン。
  - `TargetAssembly`: 対象アセンブリ名・バージョン（どの DLL から生成されたか）。
  - `GeneratedAt`: 生成日時（ISO 8601）。  
- 各エンティティごとのコメント:
  - `Schema: Subject=...` は内部で使用する Schema Registry の Subject 名（`{topic}-value`）。  
  - `Namespace: ...` は CLR の Namespace。  

---

## 5. Avro スキーマ（.avsc 相当）の出力

### 5-1. コードからの呼び出し例

```csharp
var avroExporter = new DefaultAvroSchemaExporter();
var schemas = avroExporter.ExportValueSchemas(context);

foreach (var kv in schemas)
{
    var entityName = kv.Key;      // 例: "Examples.DesigntimeKsqlScript.OrderEvent"
    var avroJson = kv.Value;      // Avro schema JSON (record)

    Console.WriteLine($"-- Entity: {entityName}");
    Console.WriteLine(avroJson);
    Console.WriteLine();
}
```

### 5-2. 出力イメージ

```text
-- Avro value schemas (.avsc) for entities
-- Entity: Examples.DesigntimeKsqlScript.OrderEvent
{"type":"record","name":"orders_v1_valueAvro","namespace":"examples_designtimeksqlscript","fields":[{"name":"Id","default":0,"type":"int"},{"name":"CreatedAt","default":0,"type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"Status","default":null,"type":["null","string"]}]}

-- Entity: Examples.DesigntimeKsqlScript.OrderSummary
{"type":"record","name":"orders_summary_v1_valueAvro","namespace":"examples_designtimeksqlscript","fields":[{"name":"Id","default":0,"type":"int"},{"name":"CreatedDate","default":0,"type":{"type":"long","logicalType":"timestamp-millis"}}]}
```

- これは `MappingRegistry` が実際に使用する Avro スキーマ（`KeyValueTypeMapping.AvroValueSchema`）をそのまま出力しており、Schema Registry への接続は不要です。
- 必要に応じて、この JSON を `.avsc` ファイルとして保存し、別リポジトリや CI で管理することもできます。

---

## 6. 今後の CLI 統合に向けた位置づけ（概要）

- 将来的に `Ksql.Linq.Cli` のような `.NET Tool` を追加する場合も、上記の設計はそのまま再利用できます。
  - CLI 側は「DLL をビルド＆ロードし、`IDesignTimeKsqlContextFactory` から `KsqlContext` を取得するところ」までを担当。
  - DDL / Avro schema の生成は、ライブラリ側の `DefaultKsqlScriptBuilder` / `DefaultAvroSchemaExporter` を呼ぶだけ、とする。
- これにより、ライブラリ／ツール／examples の三者が同じ経路で DDL / Avro を生成するため、「どこで試しても同じ結果」が得られる構造になります。

---

## 7. コマンドラインでの利用方法

このガイドで説明した機能は、現状 `dotnet run` ベースで examples から利用できます。

### 7-1. デザイン時 KSQL スクリプト（DDL）の出力

1. ライブラリと examples をビルドします。

```bash
dotnet build src/Ksql.Linq.csproj -c Release
```
デザイン時 KSQL スクリプト（OrdersKsqlContext）を出力します。
```bash
dotnet run --project examples/designtime-ksql-script/DesigntimeKsqlScript.csproj -c Release
```
実行結果には以下が含まれます。
スクリプトヘッダ（GeneratedBy / TargetAssembly / GeneratedAt）
各エンティティの CREATE STREAM/TABLE ... WITH (...)
examples/designtime-ksql-script/appsettings.json の KsqlDsl.Topics / Entities を変更してから再度 dotnet run すると、WITH (KAFKA_TOPIC=..., PARTITIONS=..., RETENTION_MS=...) がどう変化するかを確認できます。

### 7-2. Avro スキーマ（.avsc 相当）の出力
同じコマンドで、各エンティティの Avro スキーマ JSON も出力されます。

```bash
dotnet run --project examples/designtime-ksql-script/DesigntimeKsqlScript.csproj -c Release
```
出力例:
```
-- Avro value schemas (.avsc) for entities
-- Entity: Examples.DesigntimeKsqlScript.OrderEvent
{"type":"record","name":"orders_v1_valueAvro","namespace":"examples_designtimeksqlscript","fields":[...]}

-- Entity: Examples.DesigntimeKsqlScript.OrderSummary
{"type":"record","name":"orders_summary_v1_valueAvro","namespace":"examples_designtimeksqlscript","fields":[...]}
```
必要であれば、この JSON を .avsc ファイルとして保存し、別リポジトリや CI で管理できます。

### 7-3. Tumbling コンテキストでの利用
Tumbling を含むデザイン時 KSQL / Avro を確認する場合は、次のコマンドを利用します。

```bash
dotnet run --project examples/designtime-ksql-tumbling/DesigntimeKsqlTumbling.csproj -c Release
```
Tumbling を使った CREATE STREAM ... と、対応する Avro スキーマ JSON がまとめて出力されます。

---

## 7. CLI からの利用例（Ksql.Linq.Cli）

`Ksql.Linq.Cli` を .NET ツールとしてインストールすると、`dotnet ksql` コマンドからデザイン時 KSQL / Avro 出力を呼び出せます。
（ツールのインストール方法は別途パッケージ公開後に整備予定です）

### 7-1. KSQL スクリプト生成（`dotnet ksql script`）

```bash
dotnet ksql script \
  --project ./src/MyApp/MyApp.csproj \
  --output ./ksql/generated.sql \
  --verbose
```
オプション

-p, --project（必須）
対象プロジェクトまたは DLL のパス
例: ./src/MyApp/MyApp.csproj / ./artifacts/MyApp.dll
-c, --context
KsqlContext クラス名。複数の IDesignTimeKsqlContextFactory がある場合に指定。
例: MyKsqlContext
-o, --output
生成した KSQL スクリプトの出力ファイルパス。
未指定時は標準出力に書き出し。
--config
appsettings.json のパス。必要であれば Factory 側で解釈して使用する。
--no-header
スクリプト先頭のヘッダーコメント（GeneratedBy/TargetAssembly/GeneratedAt など）を除外。
-v, --verbose
DLL ロードやコンテキスト解決などの詳細ログを表示。
動作イメージ

--project から DLL を解決（AssemblyResolver）。
DLL から IDesignTimeKsqlContextFactory 実装を探し、CreateDesignTimeContext() で KsqlContext を生成（DesignTimeContextLoader）。
DefaultKsqlScriptBuilder.Build(context) でスクリプトを生成。
--output があればファイルへ、なければ標準出力へ出力。

### 7-2. Avro スキーマ生成（dotnet ksql avro）

```bash
dotnet ksql avro \
  --project ./src/MyApp/MyApp.csproj \
  --output ./schemas/ \
  --verbose
```
オプション

-p, --project（必須）
対象プロジェクトまたは DLL のパス。
-c, --context
KsqlContext クラス名（複数 Factory がある場合に指定）。
-o, --output（必須）
Avro スキーマ（.avsc）を出力するディレクトリ。存在しなければ作成されます。
--config
appsettings.json のパス。
-v, --verbose
詳細ログを表示。
動作イメージ

dotnet ksql script 同様に DLL と KsqlContext を解決。
DefaultAvroSchemaExporter.ExportValueSchemas(context) で
Dictionary<string, string>（キー: エンティティの型名、値: Avro schema JSON）を取得。
--output ディレクトリ配下に <型名>.avsc 形式でファイルを書き出し。
例: Examples_DesigntimeKsqlScript_OrderEvent.avsc など。