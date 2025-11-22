# Hopping Windowsサポート提案書
## Ksql.Linq OSS Enhancement Proposal

---

## 📋 概要 (Overview)

**目的**: Ksql.LinqにHopping Windows（ホッピングウィンドウ）のサポートを追加し、重複するウィンドウベースのストリーム処理を可能にする

**現状**: 現在、Ksql.LinqはTumbling Windows（固定サイズ・非重複ウィンドウ）のみをサポート

**提案**: Hopping Windowsを追加実装し、より柔軟なウィンドウ処理パターンを提供

---

## 🎯 Hopping Windowsとは

### 定義
Hopping Windowsは、固定サイズのウィンドウが指定された間隔（hop）で前進するウィンドウ型です。

### 特徴
- **固定ウィンドウサイズ**: ウィンドウの長さは一定
- **可変Hop間隔**: ウィンドウが進む間隔を指定可能
- **重複可能**: hop < window size の場合、ウィンドウが重複
- **非重複も可能**: hop = window size の場合、Tumbling Windowと同等

### ユースケース

#### 1. スライディング移動平均
```
ウィンドウサイズ: 5分
Hop間隔: 1分
→ 1分ごとに過去5分間のデータで集計
```

#### 2. 重複する異常検知
```
ウィンドウサイズ: 1時間
Hop間隔: 15分
→ 15分ごとに過去1時間のパターンを監視
```

#### 3. リアルタイムダッシュボード
```
ウィンドウサイズ: 10分
Hop間隔: 1分
→ 毎分更新される10分間の統計
```

---

## 📊 Tumbling vs Hopping 比較

```
Tumbling (5分ウィンドウ):
|--W1--|--W2--|--W3--|--W4--|
0     5     10    15    20 (分)

Hopping (5分ウィンドウ, 2分Hop):
|--W1--|
  |--W2--|
    |--W3--|
      |--W4--|
        |--W5--|
0  2  4  6  8  10 (分)
```

---

## 🎨 複数時間帯サポートの利点

### Tumblingの成功パターンを継承

Ksql.Linqの既存Tumbling実装は、**複数時間帯を一度に処理**する優れた設計を持っています：

```csharp
// 1つのDSL呼び出しで、4つの異なる粒度のストリームを生成
.Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1, 5, 15, 60 } })

// 生成結果: bar_1m, bar_5m, bar_15m, bar_60m
```

### Hoppingでも同様の発想を採用

**提案**: Hoppingでも複数のウィンドウサイズ + 共通hop間隔をサポート

```csharp
// 5分/10分/15分ウィンドウを、全て1分間隔でhop
.Hopping(
    time: t => t.Timestamp,
    windows: new HoppingWindows
    {
        Minutes = new[] { 5, 10, 15 },
        HopInterval = TimeSpan.FromMinutes(1)
    })

// 生成結果:
// - stats_5m_hop1m  (5分の移動平均、毎分更新)
// - stats_10m_hop1m (10分の移動平均、毎分更新)
// - stats_15m_hop1m (15分の移動平均、毎分更新)
```

### 実用的なユースケース

#### 1. **リアルタイムダッシュボード**
複数のタイムスケールを同時に表示:
- 短期トレンド: 1分/5分移動平均
- 中期トレンド: 15分/30分移動平均
- 長期トレンド: 1時間/4時間移動平均

#### 2. **マルチスケール異常検知**
異なる時間窓で同時にパターン監視:
- 即時検知: 5分ウィンドウ
- 中期パターン: 15分ウィンドウ
- トレンド分析: 1時間ウィンドウ

#### 3. **A/Bテストの多粒度分析**
同じデータを複数の時間粒度で集計し、最適な分析窓を発見

### 設計上の一貫性

| 機能 | Tumbling | Hopping（提案） |
|------|----------|----------------|
| 複数サイズ | ✅ `Minutes = [1,5,15]` | ✅ `Minutes = [5,10,15]` |
| BuildAll() | ✅ 各サイズごとにSQL生成 | ✅ 同様の機能 |
| 命名規則 | `bar_5m_live` | `bar_5m_hop1m_live` |
| 独立ストリーム | ✅ サイズごとに独立 | ✅ サイズごとに独立 |

---

## 🏗️ 現在のアーキテクチャ分析

### 既存のTumbling Windows実装

#### 1. **DSL層** (`/src/Query/Dsl/KsqlQueryable.cs:64-87`)
```csharp
public KsqlQueryable<T1> Tumbling(
    Expression<Func<T1, DateTime>> time,
    Windows windows,
    int baseUnitSeconds = 10,
    TimeSpan? grace = null,
    bool continuation = false)
```

**分析**:
- ✅ Windowsオブジェクトで複数のウィンドウサイズを指定可能
- ✅ Grace period対応
- ✅ Continuation mode（空ウィンドウ出力）
- ❌ Hop間隔の指定なし（常にwindow size = hop）

#### 2. **Window管理** (`/src/Window/WindowManager.cs`)
```csharp
internal sealed class WindowManager<TSource, TKey>
{
    private readonly TimeSpan _windowSize;
    private readonly TimeSpan _gracePeriod;
    private readonly Dictionary<DateTime, WindowBucket> _openWindows;
}
```

**分析**:
- ✅ ウィンドウの開閉管理
- ✅ Grace period後のシール処理
- ✅ 重複排除機能
- ❌ 重複ウィンドウの管理機能なし（1メッセージ = 1ウィンドウ前提）

#### 3. **SQL生成** (`/src/Query/Builders/Statements/KsqlCreateWindowedStatementBuilder.cs:67-90`)
```csharp
private static string FormatWindow(string timeframe)
{
    return unit switch
    {
        's' => $"WINDOW TUMBLING (SIZE {val} SECONDS)",
        'm' => $"WINDOW TUMBLING (SIZE {val} MINUTES)",
        ...
    };
}
```

**分析**:
- ✅ KSQL構文生成
- ❌ HOPPING構文未対応（`WINDOW HOPPING (SIZE ..., ADVANCE BY ...)`が必要）

---

## 🚀 提案する実装設計

### Phase 1: データ構造拡張

#### 1.1 `Windows`クラスの拡張

**現在** (`/src/Query/Dsl/Windows.cs`):
```csharp
public class Windows
{
    public int[]? Minutes { get; set; }
    public int[]? Hours { get; set; }
    public int[]? Days { get; set; }
    public int[]? Months { get; set; }
}
```

**提案**:
```csharp
public class Windows
{
    public int[]? Minutes { get; set; }
    public int[]? Hours { get; set; }
    public int[]? Days { get; set; }
    public int[]? Months { get; set; }

    // NEW: Hopping間隔（未指定の場合はTumbling）
    public TimeSpan? HopInterval { get; set; }
}
```

#### 1.2 新しい`HoppingWindows`クラス（推奨案）

**Tumblingとの一貫性を保ちつつ、複数時間帯をサポート**:

```csharp
namespace Ksql.Linq.Query.Dsl;

/// <summary>
/// Hopping window specification with multiple sizes and shared hop interval
/// Inspired by Tumbling's multi-timeframe design (Minutes = new[] { 1, 5, 15, 60 })
/// </summary>
public class HoppingWindows
{
    // 複数のウィンドウサイズを指定可能（Tumblingと同様）
    public int[]? Minutes { get; set; }
    public int[]? Hours { get; set; }
    public int[]? Days { get; set; }

    // 全ウィンドウで共通のHop間隔
    public TimeSpan HopInterval { get; set; }

    /// <summary>
    /// Creates hopping windows with multiple sizes and shared hop interval
    /// Example: HoppingWindows.Create(hopMinutes: 1, windowMinutes: new[] { 5, 10, 15 })
    /// Generates: 5m/10m/15m windows, all hopping every 1 minute
    /// </summary>
    public static HoppingWindows CreateMinutes(int hopMinutes, params int[] windowMinutes)
    {
        if (hopMinutes <= 0)
            throw new ArgumentException("Hop interval must be positive");

        foreach (var win in windowMinutes)
        {
            if (hopMinutes > win)
                throw new ArgumentException($"Hop {hopMinutes}m cannot exceed window {win}m");
        }

        return new HoppingWindows
        {
            HopInterval = TimeSpan.FromMinutes(hopMinutes),
            Minutes = windowMinutes
        };
    }
}
```

### Phase 2: DSL API拡張

#### 2.1 `KsqlQueryable<T1>`への新メソッド追加

**Option A: 既存Tumblingメソッドの拡張**
```csharp
public KsqlQueryable<T1> Tumbling(
    Expression<Func<T1, DateTime>> time,
    Windows windows,
    int baseUnitSeconds = 10,
    TimeSpan? grace = null,
    bool continuation = false,
    TimeSpan? hopInterval = null)  // NEW parameter
{
    _model.Extras["HasTumblingWindow"] = true;
    _model.HopInterval = hopInterval;  // NULL = Tumbling, 指定 = Hopping
    // ... 既存の処理
}
```

**Option B: 新しいHoppingメソッド（推奨・複数時間帯対応）**
```csharp
/// <summary>
/// Apply hopping windows with multiple sizes and shared hop interval
/// Follows Tumbling's multi-timeframe pattern for consistency
/// </summary>
public KsqlQueryable<T1> Hopping(
    Expression<Func<T1, DateTime>> time,
    HoppingWindows windows,
    TimeSpan? grace = null,
    bool continuation = false)
{
    _model.Extras["WindowType"] = "HOPPING";
    _model.HoppingWindows = windows; // 複数のウィンドウサイズ + 共通hop
    _model.Continuation = continuation;

    if (time.Body is MemberExpression me)
        _model.TimeKey = me.Member.Name;
    else if (time.Body is UnaryExpression ue && ue.Operand is MemberExpression me2)
        _model.TimeKey = me2.Member.Name;

    // Tumblingと同様に、各ウィンドウサイズを_model.Windowsに追加
    if (windows.Minutes != null)
        foreach (var m in windows.Minutes)
            _model.Windows.Add($"{m}m:hop{(int)windows.HopInterval.TotalMinutes}m");

    if (windows.Hours != null)
        foreach (var h in windows.Hours)
            _model.Windows.Add($"{h}h:hop{(int)windows.HopInterval.TotalHours}h");

    if (windows.Days != null)
        foreach (var d in windows.Days)
            _model.Windows.Add($"{d}d:hop{(int)windows.HopInterval.TotalDays}d");

    if (grace.HasValue)
        _model.GraceSeconds = (int)Math.Ceiling(grace.Value.TotalSeconds);

    _stage = QueryBuildStage.Window;
    return this;
}

/// <summary>
/// Simple overload for single hopping window
/// </summary>
public KsqlQueryable<T1> Hopping(
    Expression<Func<T1, DateTime>> time,
    TimeSpan windowSize,
    TimeSpan hopInterval,
    TimeSpan? grace = null,
    bool continuation = false)
{
    var windows = new HoppingWindows
    {
        HopInterval = hopInterval
    };

    // windowSizeを適切な単位に分解
    if (windowSize.TotalMinutes < 60 && windowSize.TotalMinutes == (int)windowSize.TotalMinutes)
        windows.Minutes = new[] { (int)windowSize.TotalMinutes };
    else if (windowSize.TotalHours < 24 && windowSize.TotalHours == (int)windowSize.TotalHours)
        windows.Hours = new[] { (int)windowSize.TotalHours };
    else if (windowSize.TotalDays == (int)windowSize.TotalDays)
        windows.Days = new[] { (int)windowSize.TotalDays };

    return Hopping(time, windows, grace, continuation);
}
```

#### 2.2 使用例

**例1: 単一ウィンドウサイズ（シンプル）**
```csharp
// 5分ウィンドウ、1分ごとに移動
context.Set<Trade>()
    .Hopping(
        time: t => t.Timestamp,
        windowSize: TimeSpan.FromMinutes(5),
        hopInterval: TimeSpan.FromMinutes(1),
        grace: TimeSpan.FromSeconds(30))
    .GroupBy(t => t.Symbol)
    .Select(g => new
    {
        Symbol = g.Key,
        WindowStart = g.WindowStart(),
        WindowEnd = g.WindowEnd(),
        AvgPrice = g.Average(t => t.Price),
        Volume = g.Sum(t => t.Quantity)
    });
```

**例2: 複数ウィンドウサイズ（Tumbling風・推奨）**
```csharp
// 5分/10分/15分ウィンドウを、全て1分ごとに移動
// Tumblingの Minutes = new[] { 1, 5, 15 } と同様の発想
var hoppingWindows = HoppingWindows.CreateMinutes(
    hopMinutes: 1,
    windowMinutes: 5, 10, 15);

context.Set<Trade>()
    .Hopping(
        time: t => t.Timestamp,
        windows: hoppingWindows,
        grace: TimeSpan.FromSeconds(30))
    .GroupBy(t => t.Symbol)
    .Select(g => new
    {
        Symbol = g.Key,
        WindowStart = g.WindowStart(),
        AvgPrice = g.Average(t => t.Price)
    });

// 結果: 3つの独立したストリームが生成される
// - trade_avg_5m_hop1m_live   (5分ウィンドウ、1分hop)
// - trade_avg_10m_hop1m_live  (10分ウィンドウ、1分hop)
// - trade_avg_15m_hop1m_live  (15分ウィンドウ、1分hop)
```

**例3: リアルタイムダッシュボード**
```csharp
// 1分/5分/15分/1時間の移動平均を、全て1分ごとに更新
modelBuilder.Entity<TradingStats>()
    .ToQuery(q => q.From<Trade>()
        .Hopping(
            time: t => t.Timestamp,
            windows: new HoppingWindows
            {
                Minutes = new[] { 1, 5, 15 },
                Hours = new[] { 1 },
                HopInterval = TimeSpan.FromMinutes(1)
            })
        .GroupBy(t => new { t.Exchange, t.Symbol })
        .Select(g => new TradingStats
        {
            Exchange = g.Key.Exchange,
            Symbol = g.Key.Symbol,
            BucketStart = g.WindowStart(),
            AvgPrice = g.Average(t => t.Price),
            TotalVolume = g.Sum(t => t.Volume),
            TradeCount = g.Count()
        }));

// 生成される4つのストリーム:
// - trading_stats_1m_hop1m
// - trading_stats_5m_hop1m
// - trading_stats_15m_hop1m
// - trading_stats_1h_hop1m
```

### Phase 3: ウィンドウ管理ロジック

#### 3.1 `HoppingWindowManager<TSource, TKey>` 新規実装

```csharp
namespace Ksql.Linq.Window;

/// <summary>
/// Manages overlapping hopping windows with grace period support
/// </summary>
internal sealed class HoppingWindowManager<TSource, TKey>
{
    private readonly TKey _key;
    private readonly TimeSpan _windowSize;
    private readonly TimeSpan _hopInterval;
    private readonly TimeSpan _gracePeriod;
    private readonly Func<TSource, object?>? _deduplicationKeySelector;
    private readonly object _sync = new();

    // 複数の重複ウィンドウを管理
    private readonly Dictionary<DateTime, HoppingWindowBucket> _openWindows = new();
    private readonly HashSet<DateTime> _sealedWindows = new();

    public HoppingWindowManager(
        TKey key,
        TimeSpan windowSize,
        TimeSpan hopInterval,
        TimeSpan gracePeriod,
        DateTime initialUtc,
        Func<TSource, object?>? deduplicationKeySelector)
    {
        _key = key;
        _windowSize = windowSize;
        _hopInterval = hopInterval;
        _gracePeriod = gracePeriod;
        _deduplicationKeySelector = deduplicationKeySelector;
    }

    /// <summary>
    /// 1つのメッセージを複数の重複ウィンドウに追加
    /// </summary>
    public HoppingAppendStatus AddMessage(DateTime messageTimestamp, TSource message, DateTime nowUtc)
    {
        lock (_sync)
        {
            var affectedWindows = CalculateAffectedWindows(messageTimestamp);
            int appendedCount = 0;

            foreach (var windowStart in affectedWindows)
            {
                if (_sealedWindows.Contains(windowStart))
                    continue;

                if (!_openWindows.TryGetValue(windowStart, out var bucket))
                {
                    bucket = new HoppingWindowBucket();
                    _openWindows[windowStart] = bucket;
                }

                // 重複排除はウィンドウごとに管理
                if (_deduplicationKeySelector != null)
                {
                    var dedupKey = _deduplicationKeySelector(message);
                    if (!bucket.TryAddKey(dedupKey))
                        continue; // このウィンドウには既に存在
                }

                bucket.Messages.Add(message);
                appendedCount++;
            }

            return appendedCount > 0
                ? HoppingAppendStatus.Appended
                : HoppingAppendStatus.AllDuplicate;
        }
    }

    /// <summary>
    /// メッセージが含まれるべきウィンドウの開始時刻を計算
    /// </summary>
    private List<DateTime> CalculateAffectedWindows(DateTime messageTimestamp)
    {
        var windows = new List<DateTime>();

        // メッセージが含まれる最初のウィンドウ開始時刻を計算
        var hopTicks = _hopInterval.Ticks;
        var ticksSinceEpoch = messageTimestamp.Ticks;
        var alignedTicks = (ticksSinceEpoch / hopTicks) * hopTicks;
        var alignedTime = new DateTime(alignedTicks, DateTimeKind.Utc);

        // このメッセージが含まれる全てのウィンドウを列挙
        var candidate = alignedTime;
        while (candidate + _windowSize > messageTimestamp && candidate <= messageTimestamp)
        {
            windows.Add(candidate);
            candidate -= _hopInterval;
        }

        windows.Reverse(); // 古い順にソート
        return windows;
    }

    public IReadOnlyList<WindowGrouping<TKey, TSource>> CollectClosedWindows(DateTime nowUtc)
    {
        List<(DateTime WindowStart, HoppingWindowBucket Bucket)>? closed = null;

        lock (_sync)
        {
            foreach (var kvp in _openWindows.ToArray())
            {
                var windowEnd = kvp.Key + _windowSize;
                if (nowUtc >= windowEnd + _gracePeriod && kvp.Value.Messages.Count > 0)
                {
                    closed ??= new();
                    closed.Add((kvp.Key, kvp.Value));
                    _openWindows.Remove(kvp.Key);
                    SealWindow(kvp.Key);
                }
            }
        }

        if (closed is null)
            return Array.Empty<WindowGrouping<TKey, TSource>>();

        return closed.Select(tuple =>
                new WindowGrouping<TKey, TSource>(
                    _key,
                    tuple.WindowStart,
                    tuple.WindowStart + _windowSize,
                    tuple.Bucket.Messages))
            .ToArray();
    }

    private void SealWindow(DateTime windowStartUtc)
    {
        _sealedWindows.Add(windowStartUtc);
        // TODO: メモリ管理のため、古いsealedウィンドウを削除
    }

    private sealed class HoppingWindowBucket
    {
        public List<TSource> Messages { get; } = new();
        private HashSet<object?>? _keys;

        public bool TryAddKey(object? key)
        {
            _keys ??= new HashSet<object?>();
            return _keys.Add(key);
        }
    }
}

public enum HoppingAppendStatus
{
    Appended,
    AllDuplicate,
    LateDrop
}
```

### Phase 4: SQL生成拡張

#### 4.1 `KsqlCreateWindowedStatementBuilder`の拡張

**変更箇所** (`/src/Query/Builders/Statements/KsqlCreateWindowedStatementBuilder.cs`):

```csharp
public static string Build(
    string name,
    KsqlQueryModel model,
    string timeframe,
    string? emitOverride = null,
    string? inputOverride = null,
    RenderOptions? options = null,
    TimeSpan? hopInterval = null)  // NEW parameter
{
    // ... 既存の処理

    var window = hopInterval.HasValue
        ? FormatHoppingWindow(timeframe, hopInterval.Value)
        : FormatWindow(timeframe);

    var sql = InjectWindowAfterFrom(baseSql, window);
    return sql;
}

private static string FormatHoppingWindow(string timeframe, TimeSpan hop)
{
    var windowSize = ParseTimeframe(timeframe);
    var hopFormatted = FormatTimeSpan(hop);

    // KSQL構文: WINDOW HOPPING (SIZE <size>, ADVANCE BY <hop>)
    return $"WINDOW HOPPING (SIZE {windowSize}, ADVANCE BY {hopFormatted})";
}

private static string FormatTimeSpan(TimeSpan ts)
{
    if (ts.TotalSeconds < 60)
        return $"{(int)ts.TotalSeconds} SECONDS";
    if (ts.TotalMinutes < 60)
        return $"{(int)ts.TotalMinutes} MINUTES";
    if (ts.TotalHours < 24)
        return $"{(int)ts.TotalHours} HOURS";
    return $"{(int)ts.TotalDays} DAYS";
}

private static string ParseTimeframe(string timeframe)
{
    // 既存のFormatWindow()ロジックから抽出
    var unit = timeframe[^1];
    if (!int.TryParse(timeframe[..^1], out var val)) val = 1;
    return unit switch
    {
        's' => $"{val} SECONDS",
        'm' => $"{val} MINUTES",
        'h' => $"{val} HOURS",
        'd' => $"{val} DAYS",
        _ => $"{val} MINUTES"
    };
}
```

#### 4.2 `BuildAll()`メソッドの拡張（複数時間帯サポート）

**Tumblingと同様の機能**: 複数のウィンドウサイズごとに独立したSQL文を生成

```csharp
public static Dictionary<string, string> BuildAllHopping(
    string namePrefix,
    KsqlQueryModel model,
    TimeSpan hopInterval,
    Func<string, TimeSpan, string> nameFormatter)
{
    if (model is null) throw new ArgumentNullException(nameof(model));
    if (nameFormatter is null) throw new ArgumentNullException(nameof(nameFormatter));

    var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    // model.Windowsには "5m:hop1m", "10m:hop1m" のような形式で格納されている
    foreach (var windowSpec in model.Windows)
    {
        // "5m:hop1m" から "5m" を抽出
        var windowSize = windowSpec.Split(':')[0];
        var streamName = nameFormatter(windowSize, hopInterval);

        var sql = Build(
            name: streamName,
            model: model,
            timeframe: windowSize,
            hopInterval: hopInterval);

        result[windowSpec] = sql;
    }

    return result;
}
```

**使用例**:
```csharp
// 複数ウィンドウサイズのSQL一括生成
var model = new KsqlQueryRoot()
    .From<Trade>()
    .Hopping(
        t => t.Timestamp,
        windows: new HoppingWindows
        {
            Minutes = new[] { 5, 10, 15 },
            HopInterval = TimeSpan.FromMinutes(1)
        })
    .GroupBy(t => t.Symbol)
    .Select(g => new { Symbol = g.Key, Avg = g.Average(x => x.Price) })
    .Build();

var sqlMap = KsqlCreateWindowedStatementBuilder.BuildAllHopping(
    namePrefix: "trade_avg",
    model: model,
    hopInterval: TimeSpan.FromMinutes(1),
    nameFormatter: (size, hop) => $"trade_avg_{size}_hop{(int)hop.TotalMinutes}m_live");

// 結果:
// sqlMap["5m:hop1m"]  = "CREATE STREAM trade_avg_5m_hop1m_live ... WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)"
// sqlMap["10m:hop1m"] = "CREATE STREAM trade_avg_10m_hop1m_live ... WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTES)"
// sqlMap["15m:hop1m"] = "CREATE STREAM trade_avg_15m_hop1m_live ... WINDOW HOPPING (SIZE 15 MINUTES, ADVANCE BY 1 MINUTES)"
```

---

## 📝 実装計画

### マイルストーン

#### **Milestone 1: 基盤整備** (Week 1-2)
- [ ] `HoppingWindows`クラス新規作成（複数時間帯サポート）
- [ ] `KsqlQueryModel`に`HoppingWindows`フィールド追加
- [ ] ウィンドウ命名規則実装（例: `5m:hop1m`）
- [ ] ユニットテスト作成

#### **Milestone 2: DSL拡張** (Week 2-3)
- [ ] `KsqlQueryable<T1>.Hopping()`メソッド実装（複数サイズ対応）
- [ ] シンプルオーバーロード実装（単一サイズ用）
- [ ] `HoppingWindows.CreateMinutes()`ヘルパーメソッド実装
- [ ] パラメータバリデーション実装
- [ ] DSLユニットテスト作成（単一＋複数サイズ両方）

#### **Milestone 3: ウィンドウ管理** (Week 3-4)
- [ ] `HoppingWindowManager<TSource, TKey>`実装
- [ ] 重複ウィンドウ計算ロジック実装
- [ ] Grace period処理実装
- [ ] ウィンドウ管理ユニットテスト作成

#### **Milestone 4: SQL生成** (Week 4-5)
- [ ] `KsqlCreateWindowedStatementBuilder`拡張
- [ ] `FormatHoppingWindow()`実装
- [ ] `BuildAllHopping()`実装（複数時間帯対応）
- [ ] SQL生成テスト作成（単一＋複数サイズ）
- [ ] Tumbling `BuildAll()`との一貫性確認

#### **Milestone 5: 統合テスト** (Week 5-6)
- [ ] End-to-end統合テスト作成
- [ ] Kafkaとの統合テスト
- [ ] ksqlDBとの統合テスト

#### **Milestone 6: ドキュメントとサンプル** (Week 6-7)
- [ ] API documentation作成
- [ ] サンプルプロジェクト作成（`examples/hopping-windows/`）
- [ ] README更新

---

## 🧪 テスト戦略

### ユニットテスト

#### 1. ウィンドウ計算テスト
```csharp
[Test]
public void CalculateAffectedWindows_5MinWindow_1MinHop_ReturnsCorrectWindows()
{
    // 5分ウィンドウ、1分Hop
    var manager = new HoppingWindowManager<Trade, string>(
        key: "AAPL",
        windowSize: TimeSpan.FromMinutes(5),
        hopInterval: TimeSpan.FromMinutes(1),
        gracePeriod: TimeSpan.Zero,
        initialUtc: DateTime.UtcNow,
        deduplicationKeySelector: null);

    // 10:03のメッセージは、以下のウィンドウに含まれるべき:
    // 09:59-10:04, 10:00-10:05, 10:01-10:06, 10:02-10:07, 10:03-10:08
    var timestamp = new DateTime(2025, 1, 1, 10, 3, 0, DateTimeKind.Utc);
    var windows = manager.CalculateAffectedWindows(timestamp);

    Assert.That(windows.Count, Is.EqualTo(5));
    Assert.That(windows[0], Is.EqualTo(new DateTime(2025, 1, 1, 9, 59, 0, DateTimeKind.Utc)));
    Assert.That(windows[4], Is.EqualTo(new DateTime(2025, 1, 1, 10, 3, 0, DateTimeKind.Utc)));
}
```

#### 2. SQL生成テスト
```csharp
[Test]
public void FormatHoppingWindow_5MinWindow_1MinHop_GeneratesCorrectSql()
{
    var sql = KsqlCreateWindowedStatementBuilder.FormatHoppingWindow("5m", TimeSpan.FromMinutes(1));

    Assert.That(sql, Is.EqualTo("WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)"));
}
```

### 統合テスト

#### 物理テスト（`physicalTests/OssSamples/HoppingWindowTests.cs`）

**テスト1: 単一ウィンドウサイズ**
```csharp
[TestFixture]
public class HoppingWindowTests
{
    [Test]
    public async Task HoppingWindow_SingleSize_ProducesOverlappingAggregations()
    {
        var context = new MyKsqlContext();

        var query = context.Set<Trade>()
            .Hopping(
                time: t => t.Timestamp,
                windowSize: TimeSpan.FromMinutes(5),
                hopInterval: TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new
            {
                Symbol = g.Key,
                WindowStart = g.WindowStart(),
                AvgPrice = g.Average(t => t.Price)
            });

        var results = await query.ToListAsync();

        // 重複ウィンドウが存在することを確認
        Assert.That(results.Count, Is.GreaterThan(0));
    }

    [Test]
    public async Task HoppingWindow_MultipleSize_GeneratesIndependentStreams()
    {
        var context = new MyKsqlContext();

        // Tumblingと同様に複数時間帯をテスト
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(
                t => t.Timestamp,
                windows: new HoppingWindows
                {
                    Minutes = new[] { 5, 10, 15 },
                    HopInterval = TimeSpan.FromMinutes(1)
                })
            .GroupBy(t => t.Symbol)
            .Select(g => new
            {
                Symbol = g.Key,
                WindowStart = g.WindowStart(),
                AvgPrice = g.Average(t => t.Price)
            })
            .Build();

        var sqlMap = KsqlCreateWindowedStatementBuilder.BuildAllHopping(
            namePrefix: "trade_avg",
            model: model,
            hopInterval: TimeSpan.FromMinutes(1),
            nameFormatter: (size, hop) => $"trade_avg_{size}_hop{(int)hop.TotalMinutes}m");

        // 3つのストリームが生成されることを確認
        Assert.That(sqlMap.Count, Is.EqualTo(3));
        Assert.That(sqlMap.ContainsKey("5m:hop1m"), Is.True);
        Assert.That(sqlMap.ContainsKey("10m:hop1m"), Is.True);
        Assert.That(sqlMap.ContainsKey("15m:hop1m"), Is.True);

        // 各SQLにHOPPING構文が含まれることを確認
        Assert.That(sqlMap["5m:hop1m"], Does.Contain("WINDOW HOPPING"));
        Assert.That(sqlMap["5m:hop1m"], Does.Contain("SIZE 5 MINUTES"));
        Assert.That(sqlMap["5m:hop1m"], Does.Contain("ADVANCE BY 1 MINUTES"));
    }
}
```

---

## 📚 ドキュメント計画

### 1. API Documentation
- XMLドキュメントコメント追加
- IntelliSense対応

### 2. サンプルプロジェクト
`examples/hopping-windows/`を作成:
- **シナリオ1**: リアルタイム移動平均（株価データ）
- **シナリオ2**: 異常検知（重複する時間窓でパターン監視）
- **シナリオ3**: ダッシュボード更新（頻繁に更新される長期統計）

### 3. README更新
```markdown
## Windowing Support

Ksql.Linq supports multiple windowing strategies:

### Tumbling Windows
Non-overlapping, fixed-size windows:
```csharp
context.Set<Trade>()
    .Tumbling(t => t.Timestamp, new Windows { Minutes = new[] { 5 } })
    .GroupBy(t => t.Symbol)
    .Select(g => new { Symbol = g.Key, Avg = g.Average(t => t.Price) });
```

### Hopping Windows (NEW)
Overlapping, fixed-size windows with configurable hop interval:
```csharp
context.Set<Trade>()
    .Hopping(t => t.Timestamp,
             windowSize: TimeSpan.FromMinutes(5),
             hopInterval: TimeSpan.FromMinutes(1))
    .GroupBy(t => t.Symbol)
    .Select(g => new { Symbol = g.Key, Avg = g.Average(t => t.Price) });
```
```

---

## ⚠️ 考慮事項とリスク

### パフォーマンス影響

#### 1. メモリ使用量
**問題**: 重複ウィンドウにより、同一メッセージが複数のウィンドウに保持される

**対策**:
- 参照共有による重複排除（同一メッセージへのポインタを複数ウィンドウで共有）
- Sealed windowの積極的なガベージコレクション
- メモリ使用量の監視メトリクス追加

#### 2. 計算負荷
**問題**: 1メッセージあたりの処理が増加（重複ウィンドウ数 × 集計処理）

**対策**:
- Hop間隔の最小値制限（例: 1秒未満は禁止）
- 並列処理の最適化
- ksqlDB側での処理（可能な限りDBに委譲）

### 互換性

#### 既存コードへの影響
- ✅ 既存の`Tumbling()`メソッドは変更なし
- ✅ 新しい`Hopping()`メソッドは追加のみ
- ✅ 後方互換性維持

### ksqlDB互換性
- ✅ ksqlDB 0.8+でHOPPING WINDOW構文サポート
- ⚠️ 古いksqlDBバージョンではエラー（バージョンチェック必要）

---

## 🎓 参考資料

### ksqlDB公式ドキュメント
- [Windowing](https://docs.ksqldb.io/en/latest/concepts/time-and-windows-in-ksqldb-queries/#hopping-window)
- [HOPPING WINDOW Syntax](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-push-query/#hopping-window)

### Kafka Streams
- [Hopping Time Windows](https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#hopping-time-windows)

### 学術文献
- "The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing" (Google, 2015)

---

## ✅ 成功基準

1. **機能完全性**
   - [ ] 5分ウィンドウ/1分Hopの基本シナリオが動作
   - [ ] Grace period処理が正常動作
   - [ ] 重複排除が各ウィンドウごとに機能

2. **パフォーマンス**
   - [ ] Tumbling比でメモリ使用量が2倍以内
   - [ ] スループット劣化が20%以内

3. **品質**
   - [ ] ユニットテストカバレッジ90%以上
   - [ ] 統合テストで実際のKafka/ksqlDBと動作確認

4. **ドキュメント**
   - [ ] 動作するサンプルプロジェクト3つ以上
   - [ ] API docsが完備

---

## 📅 リリース計画

### v1.0 (MVP)
- 基本的なHopping Window機能
- 単一ウィンドウサイズ/Hop間隔
- Grace period対応

### v1.1 (拡張)
- 複数ウィンドウサイズの同時サポート（Tumblingと同様）
- パフォーマンス最適化
- 追加のサンプルとドキュメント

### v2.0 (将来)
- Session Windows対応
- カスタムウィンドウ戦略対応

---

## 👥 貢献者向けガイド

この提案の実装に参加したい場合:

1. **Issue作成**: GitHub Issueで「Hopping Windows Support」を作成
2. **ブランチ戦略**: `feature/hopping-windows`ブランチで開発
3. **PR要件**:
   - ユニットテスト含む
   - XMLドキュメントコメント付き
   - CHANGELOG.md更新

---

**提案者**: Claude AI (Anthropic)
**日付**: 2025-11-22
**バージョン**: 1.0
**ステータス**: 提案中 (Proposal)
