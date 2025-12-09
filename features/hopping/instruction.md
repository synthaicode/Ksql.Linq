# Hopping Rework Plan

## 実装計画
1) DDL生成
   - `KsqlCreateWindowedStatementBuilder`でJOIN時もエイリアスを保持し、`FROM <src> <alias> WINDOW HOPPING (...) JOIN ...`となる正規表現へ修正。
   - SIZE/ADVANCE/GRACEのシリアライズ書式を整理し、TUMBLINGへの非影響を確認。
   - HOPPINGのDDLに`WINDOWSTART/WINDOWEND`列を明示的に出力しないこと（ksqlDB側で自動生成されるため）。
2) メタデータ適用
   - `HasHopping()`検知時に`QueryMetadata`へ`Role=Live`、`TimeframeRaw=<SIZE>`、必要なら`GraceSeconds`/`TimeKey`を設定。
   - `SchemaRegistrar`でHoppingメタを適用したDDL発行とキャッシュ無効化を維持。
3) バリデーション/DSL
   - Hopping利用時に`GroupBy`必須、`WindowStart()`投影は1回のみ許容（Tumbling相当のガードを適用）。
4) API/モデル
   - 窓DTOを`IWindowedRecord`実装に寄せ、`TimeBucket`等で`WindowStart`/`WindowEnd`がバインドされることを確認。
5) ドキュメント/差分
   - 作業ごとに`docs/diff_log/`へ記録し、必要に応じて`overview.md`/`AGENTS.md`の整合を確認。

## UT計画（レベル別）
- レベル1: DDLビルダー単体 (`KsqlCreateWindowedStatementBuilder`/`KsqlCreateHoppingStatementBuilderTests`)
  - 正常: 非JOIN・JOINで挿入位置と書式が正しいこと。
  - 異常: GroupByなし/WindowStart未投影で例外。TUMBLING非影響スナップショット。
- レベル2: メタデータ/Registrar (`SchemaRegistrar`周辺)
  - `QueryMetadata`へ`Role/TimeframeRaw/GraceSeconds/TimeKey`がセットされ、Hoppingでキャッシュ無効化が効くこと。非Hopping非影響。
- レベル3: DSLモデル/Projection (`KsqlQueryModel`/`KsqlQueryable`)
  - `WindowStart()`1回のみ許容、GroupBy必須ガードが動くこと。`IWindowedRecord`実装DTOに境界がマップされること。
- レベル4: TimeBucket/Helper API
  - `WhereBucketBetween`/`FindWindowAsync`がHoppingメタと整合し、他ウィンドウでリグレッションしないこと。
- レベル5: スナップショット/公開API
  - DDL出力スナップショットで破壊的変更検知、PublicAPI差分が期待通りであること。
