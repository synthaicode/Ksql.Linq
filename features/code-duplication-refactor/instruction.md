目的: コード重複の削減（P1 着手）

実施内容（今回）
- Topic名解決の統一: `EntityModel.GetTopicName()` へ寄せる
- 主要箇所とテストを置換
- 共通リトライ基盤の用意: `Core/Retry/RetryPolicy`

未着手/次回
- 既存のリトライループを `RetryPolicy` に段階移行（EventSet, KsqlContext, SchemaRegistrar 他）
- `GetOrCreateMetadata()` パターンの整理
- `catch { }` のログ方針統一

チェックリスト
- [x] 拡張メソッドで Topic 名が一意に決まる
- [x] 代表テストが拡張メソッドを使用
- [x] リトライ基盤の最小機能を実装
- [ ] 主要リトライ箇所の置換

