# Runtime Events サンプル

目的: ランタイム観測イベント（RuntimeEventBus）の基本的な使い方を最小コードで示します。

## できること
- Streamiz RUNNING 到達の観測（`streamiz.state: running`）
- rows_last 準備完了の観測（`rows_last.ready: done`）

## 実行前提
- Kafka / Schema Registry / ksqlDB が起動済みで、`KsqlDsl` 設定が `appsettings.json` などから与えられていること。
- 例の既定では `LiveTopic=bar_1m_live` と `RowsLastTopic=bar_1s_rows_last` を待機します。必要に応じて環境に合わせて上書きしてください。

## 使い方
```bash
dotnet run --project examples/runtime-events/RuntimeEvents.csproj
```

出力例:
```
[2025-10-21T12:34:56.789Z] streamiz.state:running entity=Bar_1m topic=bar_1m_live app=ksql-dsl-app-bar_1m_live state=RUNNING ok=True msg=KafkaStream reached RUNNING
[2025-10-21T12:34:57.012Z] rows_last.ready:done entity=Bar topic=bar_1s_rows_last ok=True msg=rows_last CTAS ready
[sample] rows_last.ready + streamiz.running observed.
```

## コードの要点
- `RuntimeEventBus.SetSink(new ConsoleSink());` で可視化を有効化
- 一時購読 `SubscribeOnce(...)` で特定イベントを待機（`TaskCompletionSource`）

詳細なイベント一覧は `docs/runtime-events.md` を参照してください。

