# コード重複削減・共通化計画（P2以降）

目的（1行）
- 重複ロジックを共通化し、保守性と可観測性を高める（段階移行）。

スコープ
- `src/` のコア実装（Kafka/KSQL/Schema/Runtime/Query）
- 例外: `examples/` と `physicalTests/` は利用者/環境前提のため影響最小で進める

優先度（候補と概要）
- P2（効果大・リスク小）
  - SRサブジェクト名生成の統一: `"{topic}-key"`, `"{topic}-value"` をヘルパー化
    - 代表箇所: `src/Context/KsqlContext.Schema.cs:234,239,261` ほか
  - Type→Topic名の正規化: `type.Name.ToLowerInvariant()` の横断置換（既存の`GetKafkaTopicName()`を活用）
    - 代表箇所: `src/Context/KsqlContext.Model.cs:120`, `src/Messaging/Producers/KafkaProducerManager.cs:83`
  - RuntimeEventBus.Publish の定型化: イベント送出（Name/Phase/Entity/Topic/...）をヘルパーに集約
    - 代表箇所: `src/Context/KsqlContext.Schema.cs:360`, `src/Runtime/Schema/SchemaRegistrar.cs:210` など

- P3（中）
  - SafeDelayAsync: `Task.Delay` のキャンセル安全なラッパで待機共通化
  - 値変換/プロパティ反映の共通化: `Convert.ChangeType`＋リフレクションの共通ユーティリティ

- P4（後回し可）
  - KSQL識別子の正規化ユーティリティ: Upper/Lower/引用符の扱い統一
  - 派生エンティティのベース名算出: `topicAttr/TopicName/EntityType` の合成ロジック集約
  - ポーリング安定化: 連続成立＋安定化ウィンドウの `PollUntilStableAsync`

進め方（フェーズ）
1) ヘルパー/APIの最小実装（単体テスト可能な純粋関数を優先）
2) 置換をコア経路から段階適用（1PR=1種ヘルパの原則でレビュー容易化）
3) ビルド＋基本ユニットを実行（物理試験は非実施ポリシーを遵守）
4) `docs/diff_log/` に差分記録（設計・移行意図を明記）

成功基準（定量）
- grep件数の減少:
  - `"{topic}-key"|"{topic}-value"` の直接生成回数が50%以上減少
  - `type.Name.ToLowerInvariant()` の出現回数がゼロ（例外: 名前生成専用箇所）
- LOC変化は中立〜微減（初期増分はヘルパー導入のため許容）

非機能要件
- 既存のログ/イベント送出の意味論を保持（メッセージ/Phase/成功可否）
- 例外時のDLQ動線は行動変化なし

リスクと緩和
- 振る舞いの微差: ヘルパー導入でメッセージ文言やタイミングが変化しないよう差分確認
- 置換漏れ: grepベースの棚卸し＋レビュー者ダブルチェック

運用/ブランチ
- 作業ブランチ: `release/v0.9.1-rc`
- プッシュ: 権限端末で `git push origin release/v0.9.1-rc`

タスク一覧（チェックリスト）
- [x] SRサブジェクト名ヘルパ導入（`Subject.KeyFor(topic)`, `Subject.ValueFor(topic)）`）
- [x] 既存の `"{topic}-key"|"{topic}-value"` 置換（一部主要箇所）
- [x] Type→Topic名の横断置換（`GetKafkaTopicName()`）（主要箇所）
- [x] RuntimeEventBus.Publish ヘルパ導入（例外安全・非同期fire&forget）
- [x] SafeDelayAsync 導入と適用（主要箇所）
- [x] 値変換/プロパティ反映ユーティリティ導入と適用（主要箇所）
- [ ] 識別子正規化ユーティリティ導入
- [ ] ベース名算出ヘルパ導入（派生エンティティ計画系）
- [ ] PollUntilStableAsync 導入（必要に応じて適用）

担当
- 実装: 鳴瀬
- レビュー: 鏡花
- diff_log/進行整備: 楠木

タイムライン（目安）
- P2: 1.5〜2.5日（3PR想定）
- P3: 1.5〜2日
- P4: 1〜2日（必要度に応じて後ろ倒し）

備考
- 既存のP1（Topic名統一／RetryPolicy適用）は完了済み。以降は置換とユーティリティ拡充で削減効果を伸ばす。
