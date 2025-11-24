# ksqlDB 制約違反パターン集 with Ksql.Linq (100パターン)

**Ksql.Linq** は、Kafka/ksqlDB 操作のための C# LINQ ライブラリです。このドキュメントは、**ksqlDB（バックエンドストリーム処理エンジン）** の制約と **LINQ to Objects** の違いを理解し、Ksql.Linq で正しいコードを書くための包括的なガイドです。

## このドキュメントについて

- **対象**: ksqlDB のストリーム処理制約（JOIN、集約、ウィンドウ等）
- **役割**: Ksql.Linq はこれらの制約を**設計時・実行時に検出**し、開発者にガイダンスを提供
- **目的**: LINQ to Objects とは異なる ksqlDB の制約を事前に把握し、エラーを回避

> **Status (Ksql.Linq v1.0.0)**  
> - 本書の 100 パターンは、ksqlDB 制約の「設計チェックリスト」です。  
> - Ksql.Linq v1.0.0 が **全パターンを検出することは保証されません**。  
> - 本書中の一部メソッド名は **疑似コード** であり、現行の Ksql.Linq 公開 API とは一致しない場合があります。  
> - v1.0.0 時点でライブラリ側に実装されている代表的な検証ロジックの例:  
>   - 式の深さ・ノード数制限（パターン 75–82 相当 / `BuilderValidation.ValidateExpression(...)`）。  
>   - `ToListAsync()` の使用制限（DLQ や Stream での禁止 / パターン 83 など）。  
> - それ以外のパターンは、**ksqlDB の一般的な制約を理解するための参考情報**として扱ってください。

## 目次
1. [JOIN制約違反 (12パターン)](#join制約違反)
2. [WHERE句制約違反 (12パターン)](#where句制約違反)
3. [HAVING句制約違反 (10パターン)](#having句制約違反)
4. [GROUP BY句制約違反 (10パターン)](#group-by句制約違反)
5. [ORDER BY句制約違反 (10パターン)](#order-by句制約違反)
6. [SELECT句制約違反 (10パターン)](#select句制約違反)
7. [ウィンドウ操作制約違反 (10パターン)](#ウィンドウ操作制約違反)
8. [式の深さ・複雑さ制約違反 (8パターン)](#式の深さ複雑さ制約違反)
9. [Stream vs Table操作制約違反 (10パターン)](#stream-vs-table操作制約違反)
10. [関数使用制約違反 (10パターン)](#関数使用制約違反)
11. [型・データ制約違反 (8パターン)](#型データ制約違反)

---

## JOIN制約違反

> **Status (v1.0.0)**: 概念リスト。JOIN 制約の多くはまだ「完全自動検出」にはなっていません。Ksql.Linq でクエリを組む際の設計指針として利用してください。

> **ksqlDB の制約**: ストリーム処理では最大2テーブルまでのJOINをサポート。FULL OUTER、RIGHT、CROSS、GROUP JOINは非サポート。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 1 | `orders.Join(customers, ...).Join(products, ...)` (3テーブル結合) | `StreamProcessingException: Stream processing supports maximum 2 table joins. Found 3 tables. Consider data denormalization or use batch processing for complex relationships.` | マテリアライズドビューを使用: `var enriched = orders.Join(customers, ...); await CreateMaterializedView(enriched); var result = enriched.Join(products, ...);` |
| 2 | `orders.GroupJoin(customers, ...)` | `StreamProcessingException: Unsupported join patterns detected: GROUP JOIN (use regular JOIN with GROUP BY instead). Supported: INNER, LEFT OUTER joins with co-partitioned data.` | `orders.Join(customers, ...).GroupBy(x => x.CustomerId)` |
| 4 | `orders.Join(customers, o => o.Name, c => c.Id, ...)` (異なるキー型) | `StreamProcessingException: JOIN key types must match. Outer key: String, Inner key: Int32. Ensure both tables are partitioned by the same key type for optimal performance.` | キーを同じ型に変換: `orders.Join(customers, o => o.CustomerId, c => c.Id, ...)` |
| 5 | `orders.CrossJoin(products)` | `StreamProcessingException: Unsupported join patterns detected: CROSS JOIN (performance risk in streaming)` | 明示的なキーで結合: `orders.Join(products, _ => true, _ => true, ...)` (非推奨、パフォーマンス問題あり) |
| 6 | `stream1.Join(stream2, ...).Join(stream3, ...).Join(stream4, ...)` (4テーブル結合) | `StreamProcessingException: Stream processing supports maximum 2 table joins. Found 4 tables. Alternative: Create materialized views or use event sourcing patterns.` | イベントソーシングパターンで非正規化データを作成 |
| 8 | `orders.Join(customers.Join(products, ...), ...)` (ネストされたJOIN) | `StreamProcessingException: Stream processing supports maximum 2 table joins. Found 3 tables.` | フラットな構造に変更: 中間マテリアライズドビューを作成 |
| 9 | `orders.Join(customers, o => new { o.Id, o.Type }, c => new { c.Id, c.Type }, ...)` (複合キー結合、パーティション不一致) | `[KSQL-LINQ WARNING] JOIN performance optimization: Ensure topics 'orders' and 'customers' have same partition count and key distribution.` | 単一キーに統一またはリパーティション: `orders.Join(customers, o => o.Id, c => c.Id, ...)` |
| 10 | `stream1.Where(x => x.Id > 0).Join(stream2.Where(y => y.Active), ...)` (JOIN前に複雑なフィルタ) | パフォーマンス警告: フィルタがパーティション分布を変更 | JOIN後にフィルタ: `stream1.Join(stream2, ...).Where(x => x.Left.Id > 0 && x.Right.Active)` |
| 11 | `orders.Join(customers, o => o.CustomerId.ToString(), c => c.Id.ToString(), ...)` (JOIN キーでの変換) | `StreamProcessingException: JOIN key types must match. Complex transformations in join keys may affect partitioning.` | ソーストピックレベルでキー型を統一 |

---

## WHERE句制約違反

> **Status (v1.0.0)**: 概念リスト。実際のエラー/警告メッセージは ksqlDB 側のバージョンや設定に依存します。

> **ksqlDB の制約**: WHERE句では集約関数とサブクエリは使用不可。集約が必要な場合はHAVING句を使用。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 13 | `.Where(e => new[] { e.A, e.B }.Sum() > 0)` (集約関数を使用) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.` | `.GroupBy(e => e.Id).Where(g => g.Sum(x => x.A + x.B) > 0)` (HAVINGとして実装) |
| 14 | `.Where(e => e.Orders.Count() > 5)` (ナビゲーションプロパティでの集約) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.` | `.GroupBy(e => e.CustomerId).Where(g => g.Count() > 5)` |
| 15 | `.Where(e => db.Orders.Any(o => o.CustomerId == e.Id))` (サブクエリ) | `InvalidOperationException: Subqueries are not supported in WHERE clause in KSQL` | JOIN を使用: `customers.Join(orders, c => c.Id, o => o.CustomerId, ...)` |
| 16 | `.Where(e => e.Price > db.Products.Average(p => p.Price))` (サブクエリで集約) | `InvalidOperationException: Subqueries are not supported in WHERE clause in KSQL` | 2段階クエリ: 先に平均を計算してから使用 |
| 17 | `.Where(e => new[] { e.Val1, e.Val2, e.Val3 }.Max() > 100)` (配列メソッドでの集約) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.` | `.Where(e => e.Val1 > 100 || e.Val2 > 100 || e.Val3 > 100)` (論理式に展開) |
| 18 | `.Where(e => e.Items.Average(i => i.Price) > 50)` (ネストされたコレクション集約) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.` | データをフラット化してからGROUP BY + HAVINGを使用 |
| 19 | `.Where(e => orders.Where(o => o.CustomerId == e.Id).Count() > 0)` (相関サブクエリ) | `InvalidOperationException: Subqueries are not supported in WHERE clause in KSQL` | `customers.Join(orders, ...)` にリファクタリング |
| 20 | `.Where(e => e.Items.Select(i => i.Price).Sum() > 100)` (SELECT + 集約) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause.` | `GroupBy` を使用してから `Where` (HAVING) を適用 |
| 21 | `.Where(e => Enumerable.Range(1, e.Count).Sum() > 50)` (動的配列生成 + 集約) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause.` | 数式に変換: `.Where(e => e.Count * (e.Count + 1) / 2 > 50)` |
| 22 | `.Where(e => string.IsNullOrEmpty((from x in data select x.Name).FirstOrDefault()))` (LINQ式内のサブクエリ) | `InvalidOperationException: Subqueries are not supported in WHERE clause in KSQL` | JOIN と単純な比較に分解 |
| 23 | `.Where(e => e.Tags.Count(t => t.StartsWith("A")) > 3)` (条件付き集約) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause.` | タグをフラット化してGROUP BY: `tags.Where(t => t.StartsWith("A")).GroupBy(t => t.EntityId).Where(g => g.Count() > 3)` |
| 24 | `.Where(e => e.Prices.GroupBy(p => p.Currency).Count() > 1)` (WHERE内でGROUP BY) | `InvalidOperationException: Aggregate functions are not allowed in WHERE clause.` | 外側でGROUP BYを使用 |

---

## HAVING句制約違反

> **Status (v1.0.0)**: 概念リスト。Ksql.Linq が HAVING 句のすべての誤用を検出するわけではありません。

> **ksqlDB の制約**: HAVING句では集約関数またはGROUP BY列のみ参照可能。ネストされた集約は非サポート。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 25 | `.GroupBy(e => e.Category).Where(g => g.First().Price > 100)` (GROUP BY列以外を参照) | `InvalidOperationException: HAVING clause can only reference aggregate functions or columns in GROUP BY clause` | `.GroupBy(e => e.Category).Where(g => g.Max(x => x.Price) > 100)` (集約関数を使用) |
| 26 | `.GroupBy(e => e.Category).Where(g => g.Sum(x => g.Count() * x.Price) > 1000)` (ネストされた集約) | `NotSupportedException: Nested aggregate functions are not supported` | `.GroupBy(e => e.Category).Select(g => new { Cat = g.Key, Total = g.Sum(x => x.Price), Cnt = g.Count() }).Where(x => x.Total * x.Cnt > 1000)` |
| 27 | `.GroupBy(e => e.Id).Where(g => g.Select(x => x.Amount).Sum() > 100)` (SELECT + 集約) | 実行時エラー (スタイル問題) | `.GroupBy(e => e.Id).Where(g => g.Sum(x => x.Amount) > 100)` (簡潔に) |
| 28 | `.GroupBy(e => e.Type).Where(g => g.OrderBy(x => x.Date).First().Status == "Active")` (非集約列) | `InvalidOperationException: HAVING clause can only reference aggregate functions or columns in GROUP BY clause` | ウィンドウ関数または先に最初の行を選択 |
| 29 | `.GroupBy(e => e.Region).Where(g => g.ToList().Count > 10)` (ToListを使用) | `InvalidOperationException: HAVING clause can only reference aggregate functions or columns in GROUP BY clause` | `.GroupBy(e => e.Region).Where(g => g.Count() > 10)` |
| 30 | `.GroupBy(e => e.Category).Where(g => g.Key.Length > 5 && g.Any())` (GROUP BY列 + 集約の混在) | 構文は正しいが非効率 | 先にWHEREでフィルタ: `.Where(e => e.Category.Length > 5).GroupBy(e => e.Category).Where(g => g.Any())` |
| 31 | `.GroupBy(e => e.UserId).Where(g => g.Average(x => g.Sum(y => y.Amount)) > 50)` (二重ネスト集約) | `NotSupportedException: Nested aggregate functions are not supported` | 中間変数に分解: SELECT句で集約を計算してから比較 |
| 32 | `.GroupBy(e => new { e.A, e.B }).Where(g => g.C > 10)` (GROUP BY外の列) | `InvalidOperationException: HAVING clause can only reference aggregate functions or columns in GROUP BY clause` | `.GroupBy(e => new { e.A, e.B }).Where(g => g.Key.A > 10)` または集約を使用 |
| 33 | `.GroupBy(e => e.Id).Where(g => g.SelectMany(x => x.Items).Count() > 5)` (SelectMany + 集約) | `InvalidOperationException: HAVING clause can only reference aggregate functions or columns in GROUP BY clause` | フラット化したデータでGROUP BY |
| 34 | `.GroupBy(e => e.Status).Where(g => new[] { 1, 2, 3 }.Contains(g.Count()))` (複雑な条件) | パフォーマンス問題 | `.GroupBy(e => e.Status).Where(g => g.Count() >= 1 && g.Count() <= 3)` |

---

## GROUP BY句制約違反

> **Status (v1.0.0)**: 概念リスト。GROUP BY のキー選択は、ksqlDB の仕様とトピック設計を優先して確認してください。

> **ksqlDB の制約**: GROUP BY句では集約関数は使用不可。最大10キーまで推奨（パフォーマンス最適化）。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 35 | `.GroupBy(e => e.Items.Sum(i => i.Price))` (集約関数を使用) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | 先にSELECTで計算: `.Select(e => new { e, Total = e.Items.Sum(i => i.Price) }).GroupBy(x => x.Total)` (注: ksqlDBではサポートされない可能性あり) |
| 36 | `.GroupBy(e => e.Prices.Max())` (配列メソッドで集約) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | マテリアライズド列を作成してからGROUP BY |
| 37 | `.GroupBy(e => new { e.A, e.B, e.C, e.D, e.E, e.F, e.G, e.H, e.I, e.J, e.K })` (11キー) | `InvalidOperationException: GROUP BY supports maximum 10 keys for optimal performance. Found 11 keys. Consider using composite keys or data denormalization.` | 複合キーを1つに: `.GroupBy(e => $"{e.A}_{e.B}_{e.C}...")` または非正規化 |
| 38 | `.GroupBy(e => e.Orders.Count())` (ナビゲーションプロパティで集約) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | フラット化して計算済み列でGROUP BY |
| 39 | `.GroupBy(e => e.Items.Average(i => i.Score))` (ネストされた集約) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | ウィンドウ関数または先に平均を計算 |
| 40 | `.GroupBy(e => Enumerable.Range(1, 10).Sum())` (定数集約) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | 定数値に置換: `.GroupBy(e => 55)` |
| 41 | `.GroupBy(e => new { K1 = e.F1, K2 = e.F2, K3 = e.F3, K4 = e.F4, K5 = e.F5, K6 = e.F6, K7 = e.F7, K8 = e.F8, K9 = e.F9, K10 = e.F10, K11 = e.F11 })` | `InvalidOperationException: GROUP BY supports maximum 10 keys for optimal performance. Found 11 keys.` | 最も重要な10キーに削減 |
| 42 | `.GroupBy(e => e.Values.Sum() + e.Values.Count())` (複数集約の組み合わせ) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | 計算済み列をマテリアライズ |
| 43 | `.GroupBy(e => e.Related.Select(r => r.Amount).Max())` (SELECT + 集約) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | JOIN と集約を使用して前処理 |
| 44 | `.GroupBy(e => e.Transactions.Where(t => t.Type == "Sale").Sum(t => t.Amount))` (条件付き集約) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` | マテリアライズドビューで事前集約 |

---

## ORDER BY句制約違反

> **Status (v1.0.0)**: 概念リスト。ORDER BY の制約は、ksqlDB の実行プランに強く依存します。

> **ksqlDB の制約**: ORDER BYはプルクエリでのみサポート。単純な列参照のみ可能。最大5列まで推奨。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 45 | `.OrderBy(e => e.Price * 1.1 + e.Tax)` (複雑な式) | `InvalidOperationException: ORDER BY in KSQL should use simple column references. Complex expressions in ORDER BY may not be supported.` | SELECTで計算してから並び替え: `.Select(e => new { e, Total = e.Price * 1.1 + e.Tax }).OrderBy(x => x.Total)` |
| 46 | `.OrderBy(e => e.F1).ThenBy(e => e.F2).ThenBy(e => e.F3).ThenBy(e => e.F4).ThenBy(e => e.F5).ThenBy(e => e.F6)` (6列) | `InvalidOperationException: ORDER BY supports maximum 5 columns for optimal performance. Found 6 columns. Consider reducing sort columns.` | 最も重要な5列に削減 |
| 47 | `stream.OrderBy(e => e.Timestamp)` (プッシュクエリで使用) | `[KSQL-LINQ INFO] ORDER BY in KSQL is limited to Pull Queries and specific scenarios. Push Queries (streaming) do not guarantee order due to distributed processing.` | プルクエリに変更またはクライアント側で並び替え |
| 48 | `.OrderBy(e => e.Items.Max(i => i.Priority))` (集約関数) | `InvalidOperationException: ORDER BY in KSQL should use simple column references. Complex expressions in ORDER BY may not be supported.` | GROUP BYでmaxを計算してからOrderBy |
| 49 | `.OrderBy(e => e.Name.ToUpper().Substring(0, 3))` (複数の関数呼び出し) | `InvalidOperationException: ORDER BY in KSQL should use simple column references.` | SELECTで計算: `.Select(e => new { e, Key = e.Name.ToUpper().Substring(0, 3) }).OrderBy(x => x.Key)` |
| 50 | `.OrderBy(e => e.A).ThenBy(e => e.B).ThenBy(e => e.C).ThenBy(e => e.D).ThenBy(e => e.E).ThenBy(e => e.F).ThenBy(e => e.G)` (7列) | `InvalidOperationException: ORDER BY supports maximum 5 columns for optimal performance. Found 7 columns.` | 複合キーを作成: `.OrderBy(e => $"{e.A}_{e.B}_{e.C}_{e.D}_{e.E}")` |
| 51 | `.OrderBy(e => e.Prices.Sum())` (配列メソッドで集約) | `InvalidOperationException: ORDER BY in KSQL should use simple column references.` | 集約をSELECTで事前計算 |
| 52 | `.OrderBy(e => e.Date.Year * 10000 + e.Date.Month * 100 + e.Date.Day)` (複雑な日付計算) | `InvalidOperationException: ORDER BY in KSQL should use simple column references.` | `.OrderBy(e => e.Date)` (直接日付列を使用) |
| 53 | `.OrderBy(e => (e.A, e.B, e.C, e.D, e.E, e.F))` (タプルで6要素) | `InvalidOperationException: ORDER BY supports maximum 5 columns for optimal performance.` | 5要素に削減 |
| 54 | `.OrderBy(e => Math.Abs(e.Value - target))` (数学関数) | `InvalidOperationException: ORDER BY in KSQL should use simple column references.` | SELECT句で計算してからソート |

---

## SELECT句制約違反

> **Status (v1.0.0)**: 概念リスト。一部は `BuilderValidation` の式検証で捕捉されますが、完全ではありません。

> **ksqlDB の制約**: GROUP BYなしで集約関数と非集約列を混在させることは不可。ネストされた集約は非サポート。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 55 | `.Select(e => new { e.Name, Total = e.Items.Sum(i => i.Price) })` (GROUP BY なしで集約と非集約を混在) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | `.GroupBy(e => e.Name).Select(g => new { Name = g.Key, Total = g.Sum(x => x.Items.Sum(i => i.Price)) })` |
| 56 | `.Select(e => new { e.Id, AvgPrice = e.Prices.Average() })` (配列メソッドで集約、GROUP BYなし) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | データをフラット化してGROUP BY |
| 57 | `.GroupBy(e => e.Category).Select(g => g.Sum(x => g.Average(y => y.Price)))` (ネストされた集約) | `NotSupportedException: Nested aggregate functions are not supported` | 2段階に分割: `.GroupBy(e => e.Category).Select(g => new { Sum = g.Sum(x => x.Price), Avg = g.Average(x => x.Price) })` |
| 58 | `.Select(e => new { e.Name, Count = new[] { e.A, e.B, e.C }.Count() })` (疑似集約、GROUP BYなし) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | `.GroupBy(e => e.Name).Select(g => new { Name = g.Key, Count = g.Count() })` |
| 59 | `.GroupBy(e => e.Id).Select(g => new { Total = g.Sum(x => g.Count() * x.Price) })` (集約内に集約) | `NotSupportedException: Nested aggregate functions are not supported` | 分離: `.GroupBy(e => e.Id).Select(g => new { Total = g.Sum(x => x.Price), Cnt = g.Count() }).Select(x => new { Total = x.Total * x.Cnt })` |
| 60 | `.Select(e => new { e.Status, MaxVal = e.Values.Max() })` (非集約 + ナビゲーション集約、GROUP BYなし) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | `.GroupBy(e => e.Status).Select(g => new { Status = g.Key, MaxVal = g.Max(x => x.Values.Max()) })` |
| 61 | `.Select(e => new { e.UserId, DoubleSum = new[] { e.Amount }.Sum() + new[] { e.Tax }.Sum() })` (複数疑似集約、GROUP BYなし) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | `.GroupBy(e => e.UserId).Select(g => new { UserId = g.Key, DoubleSum = g.Sum(x => x.Amount + x.Tax) })` |
| 62 | `.Select(e => new { e, Agg = orders.Where(o => o.CustomerId == e.Id).Sum(o => o.Total) })` (相関サブクエリで集約) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | JOINしてからGROUP BY |
| 63 | `.GroupBy(e => e.Type).Select(g => g.Max(x => g.Min(y => y.Value)))` (Max内にMin) | `NotSupportedException: Nested aggregate functions are not supported` | 別々に計算: `.GroupBy(e => e.Type).Select(g => new { Max = g.Max(x => x.Value), Min = g.Min(x => x.Value) })` |
| 64 | `.Select(e => new { e.Name, CountIf = e.Items.Count(i => i.Active) })` (条件付き集約、GROUP BYなし) | `InvalidOperationException: SELECT clause cannot mix aggregate functions with non-aggregate columns without GROUP BY` | フラット化してGROUP BY: `items.Where(i => i.Active).GroupBy(i => i.ParentName).Select(g => new { Name = g.Key, CountIf = g.Count() })` |

---

## 式の深さ・複雑さ制約違反

> **Status (v1.0.0)**: **実装済みロジックと対応**。`BuilderValidation.ValidateExpression(...)` により、深さ 50 / ノード数 1000 を超える式には `InvalidOperationException` が実際に投げられます（メッセージも概ねここに記載のとおり）。

> **Ksql.Linq の制約**: パフォーマンスとスタックオーバーフロー防止のため、式の深さは最大50、ノード数は最大1000に制限。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|----------------------------|---------------------|
| 75 | `Expression.Not(Expression.Not(...))` (55回ネスト) | `InvalidOperationException: Expression depth exceeds maximum allowed depth of 50. Consider simplifying the expression or breaking it into multiple operations.` | 論理を簡素化: 奇数回のNotは1つのNotに、偶数回は無しに |
| 76 | `e => (((e.A + e.B) * e.C) / e.D) + (((e.E - e.F) * e.G) / e.H) + ...` (1001ノード) | `InvalidOperationException: Expression complexity exceeds maximum allowed nodes of 1000. Current expression has 1001 nodes.` | 複数のステップに分割: 中間変数を使用 |
| 77 | `e => e.A ? (e.B ? (e.C ? ... : ...) : ...) : ...` (深くネストされた三項演算子、51レベル) | `InvalidOperationException: Expression depth exceeds maximum allowed depth of 50.` | switch文やディクショナリルックアップにリファクタ |
| 78 | `e => e.Val1 + e.Val2 + e.Val3 + ... + e.Val200` (200項の加算、1000ノード超) | `InvalidOperationException: Expression complexity exceeds maximum allowed nodes of 1000.` | `.Select(e => new[] { e.Val1, e.Val2, ... }.Sum())` (配列使用) |
| 79 | `e => MethodCall1(MethodCall2(MethodCall3(...MethodCall60(...))))` (60レベルのメソッドチェーン) | `InvalidOperationException: Expression depth exceeds maximum allowed depth of 50.` | パイプライン処理に分割: 中間結果を保存 |
| 80 | `e => e.A && e.B && e.C && ... && e.Z && e.AA && ... && e.ZZ` (複雑な論理式、1000ノード超) | `InvalidOperationException: Expression complexity exceeds maximum allowed nodes of 1000.` | `.Where(e => conditions1).Where(e => conditions2)...` に分割 |
| 81 | 再帰的ラムダ式 (深さ60) | `InvalidOperationException: Expression depth exceeds maximum allowed depth of 50.` | 反復的アプローチに変更 |
| 82 | `e => new { F1 = expr1, F2 = expr2, ..., F100 = expr100 }` (各exprが複雑で合計1200ノード) | `InvalidOperationException: Expression complexity exceeds maximum allowed nodes of 1000.` | 複数のSELECTに分割または不要なフィールドを削除 |

---

## Stream vs Table操作制約違反

> **Status (v1.0.0)**: 一部が実装と対応。特に `ToListAsync()` の使用制限（Stream / DLQ では例外）は `EventSet<T>.ToListAsync` が実際に行っています。それ以外のパターンは概念的な整理として利用してください。

> **ksqlDB の制約**: Streamは無限データソース、Tableはマテリアライズドビュー。操作方法が異なる。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 83 | `await ctx.MyStream.ToListAsync()` (ストリームでToListAsync) | `InvalidOperationException: ToListAsync() is not supported on a Stream source. Use ForEachAsync for streaming consumption or convert to a Table via materialization.` | `await ctx.MyStream.ForEachAsync(item => { /* process */ });` |
| 85 | `ctx.MyTable.ForEachAsync(item => ...)` (テーブルでストリーミング消費) | 実行時警告: テーブルはスナップショット | `await ctx.MyTable.ToListAsync()` (バッチ取得) |
| 86 | `ctx.MyStream.Count()` (ストリームで同期Count) | デッドロック: ストリームは無限なので終了しない | `.GroupBy(_ => 1).Select(g => g.Count())` (継続的な集約として) |
| 87 | `await ctx.MyStream.ToArrayAsync()` (ストリームで配列化) | タイムアウト: 無限ストリームを配列化できない | `.Take(100).ToArrayAsync()` (件数制限) または `ForEachAsync` 使用 |
| 88 | `ctx.MyStream.Last()` (ストリームで最後の要素) | 無限待機: ストリームに「最後」はない | ウィンドウ集約で最新値を追跡 |
| 89 | `var list = ctx.MyStream.ToList()` (同期ToList) | デッドロック | `await ctx.MyStream.Take(N).ToListAsync()` (非同期 + 制限) |
| 91 | `await ctx.MyTable.Take(10).ForEachAsync(...)` (テーブルでストリーミング処理) | パフォーマンス警告 | `await ctx.MyTable.Take(10).ToListAsync()` 後にforeachで処理 |
| 92 | `ctx.MyStream.Sum(e => e.Amount)` (ストリームで同期集約) | デッドロック | `.GroupBy(_ => 1).Select(g => g.Sum(x => x.Amount))` (継続集約) |

---

## 関数使用制約違反

> **Status (v1.0.0)**: 概念リスト。関数ごとのサポート状況は、`Function-Support` / `ksql-function-type-mapping` などのドキュメントと合わせて確認してください。

> **ksqlDB の制約**: ksqlDBがサポートする関数セットのみ使用可能。.NETライブラリ関数やUDFは事前登録が必要。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 93 | `.GroupBy(e => e.Name.Substring(0, e.Name.IndexOf('@')))` (GROUP BYで複雑な文字列操作) | `InvalidOperationException: Aggregate functions are not allowed in GROUP BY clause` (または関数サポートエラー) | SELECTで事前計算: `.Select(e => new { e, Domain = e.Name.Substring(0, e.Name.IndexOf('@')) }).GroupBy(x => x.Domain)` |
| 94 | `.Where(e => MyCustomFunction(e.Value) > 100)` (UDF使用、ksqlDBに未登録) | 実行時エラー: `Unknown function: MyCustomFunction` | ksqlDBにUDFを登録するか、式を展開 |
| 95 | `.Select(e => new { e.Id, Json = JsonConvert.SerializeObject(e) })` (サポートされないライブラリ関数) | 実行時エラー: 関数が変換できない | ksqlDBのネイティブJSON関数を使用またはクライアント側で処理 |
| 96 | `.OrderBy(e => Guid.NewGuid())` (ORDER BYでランダム関数) | `InvalidOperationException: ORDER BY in KSQL should use simple column references.` | ランダムソートは非決定的、ksqlDBでは非サポート |
| 97 | `.Where(e => Regex.IsMatch(e.Email, pattern))` (正規表現、ksqlDB形式と異なる) | 実行時エラー: .NET Regexは変換不可 | ksqlDBの `REGEXP_MATCH` 関数を使用: カスタム拡張メソッドで対応 |
| 98 | `.Select(e => new { e.Id, Hash = e.Password.GetHashCode() })` (GetHashCode使用) | 実行時エラー: GetHashCodeは変換不可 | ksqlDBのハッシュ関数を使用 (例: `SHA256`) |
| 99 | `.GroupBy(e => DateTime.Now.Year - e.BirthDate.Year)` (DateTime.Now使用) | 実行時エラー: 非決定的関数 | `.Select(e => new { e, Age = DateTime.UtcNow.Year - e.BirthDate.Year }).GroupBy(x => x.Age)` (注: ksqlDBでは `CURRENT_TIMESTAMP` 使用) |
| 100 | `.Where(e => e.Tags.Any(t => MyComplexPredicate(t)))` (複雑なカスタムロジック) | 実行時エラー: カスタムメソッドは変換不可 | ロジックをSQL互換の式に展開: `.Where(e => e.Tags.Any(t => t.StartsWith("prefix") && t.Length > 5))` |

---

## 型・データ制約違反

> **ksqlDB の制約**: 型の一貫性、NULL セマンティクス、DECIMAL精度等、SQLに準拠したデータ型制約。

| No. | 違反パターン (LINQ to Objects) | Ksql.Linq / ksqlDB エラーメッセージ | 正しい Ksql.Linq コード |
|-----|-------------------------------|--------------------------------------|---------------------|
| 101 | `.Select(e => e.Status ? e.IntValue : e.StringValue)` (CASE式で型不一致) | `NotSupportedException: CASE expression type mismatch: System.Int32 and System.String` | 型を統一: `.Select(e => e.Status ? e.IntValue.ToString() : e.StringValue)` |
| 102 | `orders.Join(customers, o => o.CustomerId.ToString(), c => c.Id, ...)` (JOINキー型不一致) | `StreamProcessingException: JOIN key types must match. Outer key: String, Inner key: Int32.` | 型を統一: `o => o.CustomerId` (両方int) |
| 103 | `.Where(e => e.Price == null)` (NULL比較、NULLセマンティクスの違い) | 実行時警告: ksqlDBのNULLは特殊 | `.Where(e => e.Price == null || !e.Price.HasValue)` (Nullable型対応) |
| 104 | `.Select(e => new { e.Id, Value = e.Amount ?? e.Fallback })` (NULL合体演算子、型不一致の可能性) | 型エラー: AmountとFallbackの型が異なる | 型を統一してからNULL合体 |
| 105 | `.GroupBy(e => e.Category).Select(g => new { Cat = g.Key, Arr = g.ToArray() })` (配列型のシリアライズ) | 実行時エラー: 配列型がサポートされない場合あり | `COLLECT_LIST`を使用: `.Select(g => new { Cat = g.Key, List = g.Select(x => x.Id).ToList() })` (ksqlDBでは `COLLECT_LIST`) |
| 106 | `.Where(e => e.DecimalValue > 999999999999999999999999999999.99m)` (DECIMAL精度オーバーフロー) | 実行時エラー: DECIMAL精度制限 | 適切な精度を使用: ksqlDBのDECIMAL制限を確認 |
| 107 | `.Select(e => new { e.Id, Bytes = Encoding.UTF8.GetBytes(e.Name) })` (バイト配列生成) | 実行時エラー: Encodingは変換不可 | ksqlDBのバイナリ関数を使用またはクライアント側で処理 |
| 108 | `.Where(e => e.Timestamp > new DateTime(2024, 1, 1))` (タイムゾーン未指定) | 実行時警告: タイムゾーンの不一致 | UTC明示: `.Where(e => e.Timestamp > new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc))` |

---

## 使用方法

### パターンの探し方
1. **エラーメッセージで検索**: Ksql.Linq または ksqlDB から返されたエラーメッセージをこのドキュメントで検索
2. **カテゴリから探す**: 使用している句 (WHERE, GROUP BY等) のセクションを参照
3. **コードパターンで探す**: 自分のLINQコードと似たパターンを検索

### 修正のベストプラクティス
1. **早期フィルタリング**: WHERE句はできるだけ単純に、集約が必要なら先にGROUP BY
2. **計算の分離**: 複雑な計算はSELECT句で事前に行い、後続の句では単純な列参照を使用
3. **マテリアライズドビュー**: 複雑なJOINや集約は中間結果をマテリアライズ
4. **非正規化**: ksqlDB のストリーム処理では正規化よりも非正規化が効率的
5. **型の統一**: JOIN/CASE等では必ず型を統一

### 追加リソース
- [Ksql.Linq 公式ドキュメント](../README.md)
- [ksqlDB 公式ドキュメント](https://docs.ksqldb.io/)
- [サンプルコード](../examples/)
- [Ksql.Linq Wiki](https://github.com/synthaicode/Ksql.Linq/wiki)

---

## 貢献

このドキュメントは継続的に更新されます。新しいパターンや修正案があれば、Issue または Pull Request をお願いします。

## ライセンス

このドキュメントは Ksql.Linq プロジェクトの一部として、[MIT License](../LICENSE) の下で提供されています。
