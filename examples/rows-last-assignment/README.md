# rows-last-assignment (Assigned/Revoked-driven ingestion)

目的（1行）
- コンシューマの割当て（Assigned）で rows → rows_last 取り込みを開始し、剥奪（Revoked）で停止→再待機する最小例。

前提
- .NET 8 SDK
- Docker（Kafka/ksqlDB/SR が起動済み）
  - 例: physicalTests の compose などで起動し、以下が通ること
    - `curl -s http://127.0.0.1:18088/info` → RUNNING
    - `curl -s http://127.0.0.1:18081/subjects` → JSON応答

構成
- エンティティ
  - `Rate`（deduprates）→ 1分Tumbling + GroupBy(BROKER,SYMBOL)
  - `Bar`（bar_1m_live）: OPEN/HIGH/LOW/CLOSE を集約
- 取り込み制御（実装済み・ライブラリ側）
  - Assignedで開始、Revokedで停止し未処理は送出しない（Flush-None）
  - rows_last CTAS は LATEST_BY_OFFSET + GROUP BY のTABLEとして作成
  - SRのKEY/VALUEスキーマを起動時Debugログで観測

実行
- 1台起動
  - `dotnet run --project examples/rows-last-assignment -c Release`
  - 期待ログ
    - `Row monitor assignment observed for topic deduprates`
    - `Row monitor runId=... consuming sourceTopic=deduprates targetTopic=bar_1s_rows ...`
    - `Aligned KEY schema ... fields=[...]` / `Aligned VALUE schema ... fields=[...]`
- 2台目起動（別シェル）
  - 同一コマンドで2つ目を起動（同一グループ）
  - 旧active: `revocation ... stopping consumption` → 停止→再待機
  - 新active: `assignment observed ... consuming ...`

確認クエリ（任意, ksql CLI/REST）
- rows_last到達（T1）
  - `SELECT 1 FROM BAR_1S_ROWS_LAST WHERE BROKER='B1' AND SYMBOL='S1' LIMIT 1;`
- 1mライブ（T2）
  - `SELECT BROKER,SYMBOL,BUCKETSTART,OPEN,HIGH,LOW,KSQLTIMEFRAMECLOSE FROM BAR_1M_LIVE WHERE BROKER='B1' AND SYMBOL='S1' LIMIT 5;`

注意
- Revoked時は未処理バッファを送出しません（Flush-None）。別インスタンスがコミット位置から再処理するため、整合矛盾は生じません。
- rows_last は TABLE（compact）として作成され、同一キーの重複上書きは無害化されます。
