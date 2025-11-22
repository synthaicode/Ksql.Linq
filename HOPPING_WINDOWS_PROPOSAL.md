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
// sqlMap["5m:hop1m"]  = "CREATE TABLE trade_avg_5m_hop1m_live ... WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)"
// sqlMap["10m:hop1m"] = "CREATE TABLE trade_avg_10m_hop1m_live ... WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTES)"
// sqlMap["15m:hop1m"] = "CREATE TABLE trade_avg_15m_hop1m_live ... WINDOW HOPPING (SIZE 15 MINUTES, ADVANCE BY 1 MINUTES)"
```

### Phase 5: ランタイムパイプライン（複数時間帯処理）

**Tumblingと同様のC#側処理が必要**: 複数のウィンドウサイズを自動的に複数のKSQLストリームに展開

#### 5.1 処理フロー概要

```
ユーザーDSL呼び出し
    ↓
.Hopping(windows: new HoppingWindows { Minutes = [5, 10, 15], HopInterval = 1m })
    ↓
KsqlQueryModel構築 (model.Windows = ["5m:hop1m", "10m:hop1m", "15m:hop1m"])
    ↓
HoppingQao生成 (クエリ分析オブジェクト)
    ↓
HoppingDerivationPlanner.Plan() ← 複数のDerivedEntity生成
    ↓
DerivedHoppingPipeline.RunAsync() ← 各エンティティのDDL実行
    ↓
3つの独立したksqlDB TABLEが作成される:
- trade_bar_5m_hop1m_live (TABLE)
- trade_bar_10m_hop1m_live (TABLE)
- trade_bar_15m_hop1m_live (TABLE)

**注**: WINDOW句を使った集計は、ksqlDBでは常にTABLE
```

#### 5.2 新クラス: `HoppingQao` (Query Analysis Object)

**役割**: DSLから抽出したHopping Window情報を保持

```csharp
namespace Ksql.Linq.Query.Analysis;

/// <summary>
/// Hopping window query analysis object
/// Parallel to TumblingQao for hopping windows
/// </summary>
internal class HoppingQao
{
    /// <summary>
    /// Timestamp column name
    /// </summary>
    public string TimeKey { get; init; } = string.Empty;

    /// <summary>
    /// Multiple window sizes (e.g., [5m, 10m, 15m])
    /// </summary>
    public IReadOnlyList<Timeframe> Windows { get; init; } = new List<Timeframe>();

    /// <summary>
    /// Shared hop interval for all windows
    /// </summary>
    public TimeSpan HopInterval { get; init; }

    /// <summary>
    /// GROUP BY keys
    /// </summary>
    public IReadOnlyList<string> Keys { get; init; } = new List<string>();

    /// <summary>
    /// SELECT projection
    /// </summary>
    public IReadOnlyList<string> Projection { get; init; } = new List<string>();

    /// <summary>
    /// POCO shape (column metadata)
    /// </summary>
    public IReadOnlyList<ColumnShape> PocoShape { get; init; } = new List<ColumnShape>();

    /// <summary>
    /// Grace period in seconds
    /// </summary>
    public int? GraceSeconds { get; init; }

    /// <summary>
    /// Per-timeframe grace overrides
    /// </summary>
    public Dictionary<string, int> GracePerTimeframe { get; } = new();
}
```

#### 5.3 新クラス: `HoppingDerivationPlanner`

**役割**: 複数のウィンドウサイズから派生エンティティを計画

```csharp
namespace Ksql.Linq.Query.Analysis;

/// <summary>
/// Plans derived entities for hopping windows
/// Parallel to DerivationPlanner for tumbling windows
/// </summary>
internal static class HoppingDerivationPlanner
{
    /// <summary>
    /// Generate one DerivedEntity per window size
    /// </summary>
    public static IReadOnlyList<DerivedEntity> Plan(HoppingQao qao, EntityModel model)
    {
        var entities = new List<DerivedEntity>();
        var baseId = ModelNaming.GetBaseId(model);

        var keyShapes = qao.Keys.Select(k =>
        {
            var match = qao.PocoShape.FirstOrDefault(p => p.Name == k)
                ?? throw new InvalidOperationException($"Key property '{k}' not found");
            return match;
        }).ToArray();

        var valueShapes = qao.PocoShape.ToArray();

        // 各ウィンドウサイズごとにエンティティを生成
        foreach (var tf in qao.Windows)
        {
            var tfStr = $"{tf.Value}{tf.Unit}";
            var hopStr = FormatHopInterval(qao.HopInterval);
            var liveId = $"{baseId}_{tfStr}_hop{hopStr}_live";

            var live = new DerivedEntity
            {
                Id = liveId,
                Role = Role.HoppingLive,  // 新しいRole列挙値
                Timeframe = tf,
                HopInterval = qao.HopInterval,  // NEW: hop間隔を保持
                KeyShape = keyShapes,
                ValueShape = valueShapes,
                InputHint = null,  // Hoppingは元ストリームから直接読む
                TimeKey = qao.TimeKey,
                GraceSeconds = qao.GraceSeconds ?? 1
            };

            entities.Add(live);
        }

        return entities;
    }

    private static string FormatHopInterval(TimeSpan hop)
    {
        if (hop.TotalMinutes < 60 && hop.TotalMinutes == (int)hop.TotalMinutes)
            return $"{(int)hop.TotalMinutes}m";
        if (hop.TotalHours < 24 && hop.TotalHours == (int)hop.TotalHours)
            return $"{(int)hop.TotalHours}h";
        return $"{(int)hop.TotalDays}d";
    }
}
```

#### 5.4 新クラス: `DerivedHoppingPipeline`

**役割**: 複数のエンティティに対してDDL実行を orchestrate

```csharp
namespace Ksql.Linq.Query.Analysis;

/// <summary>
/// Orchestrates execution of hopping window DDL statements
/// Parallel to DerivedTumblingPipeline
/// </summary>
internal static class DerivedHoppingPipeline
{
    public static async Task<IReadOnlyList<ExecutionResult>> RunAsync(
        HoppingQao qao,
        EntityModel baseModel,
        KsqlQueryModel queryModel,
        Func<EntityModel, string, Task<KsqlDbResponse>> execute,
        Func<string, Type> resolveType,
        MappingRegistry mapping,
        ConcurrentDictionary<Type, EntityModel> registry,
        ILogger logger,
        Func<ExecutionResult, Task>? afterExecuteAsync = null,
        Action<EntityModel>? applyTopicSettings = null)
    {
        var executions = new List<ExecutionResult>();
        var baseName = ModelNaming.GetBaseId(baseModel);

        // Step 1: 派生エンティティの計画
        var entities = HoppingDerivationPlanner.Plan(qao, baseModel);
        var models = EntityModelAdapter.Adapt(entities);

        // Step 2: 各エンティティに対してDDL実行
        foreach (var m in models)
        {
            var metadata = m.GetOrCreateMetadata();

            // トピック設定適用
            if (string.IsNullOrWhiteSpace(m.TopicName) && !string.IsNullOrWhiteSpace(metadata.Identifier))
                m.TopicName = metadata.Identifier;

            applyTopicSettings?.Invoke(m);

            if (m.AdditionalSettings.Count > 0)
            {
                var refreshed = QueryMetadataFactory.FromAdditionalSettings(m.AdditionalSettings);
                m.SetMetadata(refreshed);
            }

            metadata = m.GetOrCreateMetadata();
            var role = metadata.Role == "HoppingLive" ? Role.HoppingLive : Role.HoppingLive;
            var tf = metadata.TimeframeRaw ?? "1m";

            // Step 3: DDL生成
            var (ddl, dt, ns, inputOverride, shouldExecute) =
                HoppingEntityDdlPlanner.Build(
                    baseName,
                    queryModel,
                    m,
                    role,
                    qao.HopInterval,  // hop間隔を渡す
                    resolveType);

            if (!shouldExecute || string.IsNullOrWhiteSpace(ddl))
            {
                registry[dt] = m;
                continue;
            }

            // Step 4: DDL実行
            logger.LogInformation("KSQL DDL (hopping {Entity}): {Sql}", m.TopicName, ddl);
            var response = await execute(m, ddl);
            var queryId = QueryIdUtils.ExtractQueryId(response);

            // Step 5: TimeBucket型マッピング登録
            try
            {
                var period = TimeframeUtils.ToPeriod(tf);
                TimeBucketTypes.RegisterHoppingRead(
                    baseModel.EntityType,
                    period,
                    qao.HopInterval,
                    dt);
            }
            catch { /* best-effort */ }

            // Step 6: Avro Schema Registry登録
            if (role == Role.HoppingLive)
            {
                try
                {
                    var derivedMeta = m.GetOrCreateMetadata();
                    var keyNames = derivedMeta.Keys.Names ?? Array.Empty<string>();
                    var keyTypes = derivedMeta.Keys.Types ?? Array.Empty<Type>();
                    var valNames = derivedMeta.Projection.Names ?? Array.Empty<string>();
                    var valTypes = derivedMeta.Projection.Types ?? Array.Empty<Type>();

                    // Key/Value メタデータ構築して登録
                    var kvMapping = mapping.RegisterMeta(dt, (keyMeta, valMeta), m.TopicName,
                        genericKey: true,
                        genericValue: true,
                        overrideNamespace: ns);
                }
                catch { }
            }

            var result = new ExecutionResult(m, role, ddl, inputOverride, response, queryId);
            executions.Add(result);
            registry[dt] = m;

            if (afterExecuteAsync != null)
                await afterExecuteAsync(result).ConfigureAwait(false);
        }

        return executions;
    }
}
```

#### 5.5 新クラス: `HoppingEntityDdlPlanner`

**役割**: HOPPING構文のDDLを生成

```csharp
namespace Ksql.Linq.Query.Builders.Planners;

/// <summary>
/// Generates DDL for hopping window entities
/// </summary>
internal static class HoppingEntityDdlPlanner
{
    public static (string Ddl, Type DerivedType, string Namespace, string? InputOverride, bool ShouldExecute)
        Build(
            string baseName,
            KsqlQueryModel queryModel,
            EntityModel entityModel,
            Role role,
            TimeSpan hopInterval,
            Func<string, Type> resolveType)
    {
        var metadata = entityModel.GetOrCreateMetadata();
        var timeframe = metadata.TimeframeRaw ?? "1m";

        // 派生型を動的生成または解決
        var derivedType = ResolveDerivedType(entityModel, timeframe, resolveType);

        // HOPPING構文のDDL生成
        var ddl = KsqlCreateWindowedStatementBuilder.Build(
            name: entityModel.TopicName,
            model: queryModel,
            timeframe: timeframe,
            hopInterval: hopInterval,  // NEW: hop間隔を渡す
            emitOverride: "EMIT CHANGES",
            inputOverride: null);

        return (ddl, derivedType, entityModel.Namespace, null, true);
    }

    private static Type ResolveDerivedType(EntityModel model, string timeframe, Func<string, Type> resolver)
    {
        // 型名の例: "Trade_5m_hop1m_live"
        var typeName = $"{model.EntityType.Name}_{timeframe}_hop{FormatHop(model)}";
        return resolver(typeName) ?? model.EntityType;
    }
}
```

#### 5.6 `Role`列挙型の拡張

```csharp
namespace Ksql.Linq.Query.Analysis;

internal enum Role
{
    Final1sStream,    // Tumbling用: 1s hub stream
    Live,             // Tumbling用: Live table
    HoppingLive,      // NEW: Hopping用 live TABLE (aggregateクエリはTABLEになる)
    // ... その他
}
```

#### 5.7 `TimeBucketTypes`の拡張

**既存**: Tumbling用のマッピング
```csharp
TimeBucketTypes.RegisterRead(baseType, period, derivedType);
// TimeBucket<Trade>.Read(Period.Min5) → Trade_5m_live型
```

**新規**: Hopping用のマッピング
```csharp
namespace Ksql.Linq.Runtime;

public static class TimeBucketTypes
{
    // NEW: Hopping用マッピング
    public static void RegisterHoppingRead(
        Type baseType,
        Period period,
        TimeSpan hopInterval,
        Type derivedType)
    {
        var key = $"{baseType.FullName}:{period}:hop{FormatHop(hopInterval)}";
        _readMappings[key] = derivedType;
    }

    // 使用例: TimeBucket<Trade>.ReadHopping(Period.Min5, TimeSpan.FromMinutes(1))
    public static Type ResolveHoppingRead(Type baseType, Period period, TimeSpan hopInterval)
    {
        var key = $"{baseType.FullName}:{period}:hop{FormatHop(hopInterval)}";
        return _readMappings.TryGetValue(key, out var type) ? type : baseType;
    }
}
```

#### 5.8 実行例の完全なフロー

```csharp
// ========================================
// 1. ユーザーコード
// ========================================
modelBuilder.Entity<TradingStats>()
    .ToQuery(q => q.From<Trade>()
        .Hopping(
            time: t => t.Timestamp,
            windows: new HoppingWindows
            {
                Minutes = new[] { 5, 10, 15 },
                HopInterval = TimeSpan.FromMinutes(1)
            })
        .GroupBy(t => new { t.Symbol })
        .Select(g => new TradingStats
        {
            Symbol = g.Key.Symbol,
            BucketStart = g.WindowStart(),
            AvgPrice = g.Average(t => t.Price)
        }));

// ========================================
// 2. C#ランタイム処理
// ========================================

// 2.1 DSL → KsqlQueryModel
var queryModel = new KsqlQueryModel
{
    Windows = ["5m:hop1m", "10m:hop1m", "15m:hop1m"],
    HoppingWindows = new HoppingWindows { ... },
    // ...
};

// 2.2 KsqlQueryModel → HoppingQao
var qao = new HoppingQao
{
    TimeKey = "Timestamp",
    Windows = [
        new Timeframe(5, "m"),
        new Timeframe(10, "m"),
        new Timeframe(15, "m")
    ],
    HopInterval = TimeSpan.FromMinutes(1),
    Keys = ["Symbol"],
    Projection = ["Symbol", "BucketStart", "AvgPrice"],
    // ...
};

// 2.3 HoppingQao → DerivedEntity[]
var entities = HoppingDerivationPlanner.Plan(qao, baseModel);
// 結果:
// - DerivedEntity { Id = "trading_stats_5m_hop1m_live", ... }
// - DerivedEntity { Id = "trading_stats_10m_hop1m_live", ... }
// - DerivedEntity { Id = "trading_stats_15m_hop1m_live", ... }

// 2.4 DerivedEntity[] → DDL実行
await DerivedHoppingPipeline.RunAsync(qao, baseModel, queryModel, execute, ...);
// 実行内容:
// - CREATE TABLE trading_stats_5m_hop1m_live AS ... WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES);
// - CREATE TABLE trading_stats_10m_hop1m_live AS ... WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTES);
// - CREATE TABLE trading_stats_15m_hop1m_live AS ... WINDOW HOPPING (SIZE 15 MINUTES, ADVANCE BY 1 MINUTES);

// ========================================
// 3. ksqlDB側の結果
// ========================================
// 3つの独立したTABLEが作成される:
// - trading_stats_5m_hop1m_live (TABLE: 5分ウィンドウ、1分hop)
// - trading_stats_10m_hop1m_live (TABLE: 10分ウィンドウ、1分hop)
// - trading_stats_15m_hop1m_live (TABLE: 15分ウィンドウ、1分hop)
```

### Phase 6: 読み取りAPI（C#消費者向け）

**重要**: KSQL生成だけでなく、生成されたHoppingストリームを**C#から読み取るAPI**も必要

#### 6.1 既存のTumbling読み取りAPI

```csharp
// Tumbling: TimeBucket<T>を使った読み取り
var trades5m = await TimeBucket.Get<Trade>(_context, Period.Min5).ToListAsync();
// → trade_5m_live ストリームから読み取り
```

**内部動作**:
1. `Period.Min5` → `trade_5m_live`トピック名を解決
2. `TimeBucketTypes.ResolveRead(typeof(Trade), Period.Min5)` → `Trade_5m_live`型を解決
3. TableCacheまたはksqlDB pull queryで読み取り

#### 6.2 新しいHopping読み取りAPI設計

**Option A: 既存APIの拡張（推奨）**

```csharp
namespace Ksql.Linq.Runtime;

public static class TimeBucket
{
    // 既存: Tumbling用
    public static TimeBucket<T> Get<T>(KsqlContext ctx, Period period) where T : class
        => new(ctx, period, hopInterval: null);

    // NEW: Hopping用オーバーロード
    public static HoppingTimeBucket<T> GetHopping<T>(
        KsqlContext ctx,
        Period period,
        TimeSpan hopInterval) where T : class
        => new(ctx, period, hopInterval);
}

/// <summary>
/// Hopping window time bucket reader
/// </summary>
public sealed class HoppingTimeBucket<T> where T : class
{
    private readonly KsqlContext _ctx;
    private readonly Period _period;
    private readonly TimeSpan _hopInterval;
    private readonly string _liveTopic;
    private readonly Type _readType;

    internal HoppingTimeBucket(KsqlContext ctx, Period period, TimeSpan hopInterval)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        _period = period;
        _hopInterval = hopInterval;

        // トピック名解決: trade_5m_hop1m_live
        _liveTopic = TimeBucketTypes.GetHoppingLiveTopicName(typeof(T), period, hopInterval);

        // 型解決: Trade_5m_hop1m_live
        _readType = TimeBucketTypes.ResolveHoppingRead(typeof(T), period, hopInterval) ?? typeof(T);
    }

    /// <summary>
    /// Read all records from the hopping window stream
    /// </summary>
    public async Task<List<T>> ToListAsync(CancellationToken ct = default)
    {
        // TableCacheから読み取り（Tumblingと同じロジック）
        var cache = GetTableCache(_ctx, _readType);
        var resultEnum = await cache.ToListAsync(filter: null, timeout: null);

        // 型マッピングして返す
        return MapResults(resultEnum);
    }

    /// <summary>
    /// Read records filtered by primary key
    /// </summary>
    public Task<List<T>> ToListAsync(IReadOnlyList<string> pkFilter, CancellationToken ct = default)
    {
        // pkFilterでフィルタリングして読み取り
        var cache = GetTableCache(_ctx, _readType);
        return MapFilteredResults(cache, pkFilter);
    }

    /// <summary>
    /// Read records for a specific time range
    /// </summary>
    public async Task<List<T>> ReadRangeAsync(
        DateTime startUtc,
        DateTime endUtc,
        CancellationToken ct = default)
    {
        // WindowStart/WindowEnd でフィルタリング
        var allRecords = await ToListAsync(ct);

        return allRecords
            .Where(r =>
            {
                var windowStart = GetWindowStart(r);
                return windowStart >= startUtc && windowStart < endUtc;
            })
            .ToList();
    }

    private static DateTime GetWindowStart(T record)
    {
        // WindowStart, BucketStart などのプロパティから抽出
        var prop = typeof(T).GetProperty("WindowStart") ?? typeof(T).GetProperty("BucketStart");
        return prop != null ? (DateTime)prop.GetValue(record)! : DateTime.MinValue;
    }
}
```

#### 6.3 `TimeBucketTypes`の拡張（トピック名解決）

```csharp
namespace Ksql.Linq.Runtime;

public static class TimeBucketTypes
{
    private static readonly Dictionary<string, string> _hoppingTopicNames = new();

    /// <summary>
    /// Get hopping live topic name (e.g., "trade_5m_hop1m_live")
    /// </summary>
    public static string GetHoppingLiveTopicName(Type baseType, Period period, TimeSpan hopInterval)
    {
        var periodStr = FormatPeriod(period);  // "5m"
        var hopStr = FormatHop(hopInterval);    // "1m"
        var baseId = baseType.Name.ToLowerInvariant();  // "trade"

        var key = $"{baseType.FullName}:{period}:hop{hopStr}";

        if (_hoppingTopicNames.TryGetValue(key, out var cached))
            return cached;

        // デフォルト命名: {base}_{period}_hop{hop}_live
        var topicName = $"{baseId}_{periodStr}_hop{hopStr}_live";
        _hoppingTopicNames[key] = topicName;
        return topicName;
    }

    private static string FormatPeriod(Period period)
    {
        return period.Unit switch
        {
            PeriodUnit.Minutes => $"{period.Value}m",
            PeriodUnit.Hours => $"{period.Value}h",
            PeriodUnit.Days => $"{period.Value}d",
            _ => $"{period.Value}s"
        };
    }

    private static string FormatHop(TimeSpan hop)
    {
        if (hop.TotalMinutes < 60 && hop.TotalMinutes == (int)hop.TotalMinutes)
            return $"{(int)hop.TotalMinutes}m";
        if (hop.TotalHours < 24 && hop.TotalHours == (int)hop.TotalHours)
            return $"{(int)hop.TotalHours}h";
        return $"{(int)hop.TotalDays}d";
    }
}
```

#### 6.4 使用例: C#からHoppingストリームを読み取る

```csharp
// ========================================
// 例1: 基本的な読み取り
// ========================================

// 5分ウィンドウ、1分hopのストリームから全件取得
var trades5m = await TimeBucket
    .GetHopping<Trade>(_context, Period.Min5, TimeSpan.FromMinutes(1))
    .ToListAsync();

Console.WriteLine($"Retrieved {trades5m.Count} hopping window records");

// ========================================
// 例2: プライマリキーでフィルタリング
// ========================================

// 特定のシンボルのみ取得
var appleRecords = await TimeBucket
    .GetHopping<Trade>(_context, Period.Min5, TimeSpan.FromMinutes(1))
    .ToListAsync(pkFilter: new[] { "AAPL" });

// ========================================
// 例3: 時間範囲でフィルタリング
// ========================================

// 過去1時間分のHoppingウィンドウデータを取得
var recent = await TimeBucket
    .GetHopping<Trade>(_context, Period.Min5, TimeSpan.FromMinutes(1))
    .ReadRangeAsync(
        startUtc: DateTime.UtcNow.AddHours(-1),
        endUtc: DateTime.UtcNow);

// ========================================
// 例4: 複数のウィンドウサイズを並行読み取り
// ========================================

var hop1m = TimeSpan.FromMinutes(1);

var (data5m, data10m, data15m) = await (
    TimeBucket.GetHopping<Trade>(_context, Period.Min5, hop1m).ToListAsync(),
    TimeBucket.GetHopping<Trade>(_context, Period.Min10, hop1m).ToListAsync(),
    TimeBucket.GetHopping<Trade>(_context, Period.Min15, hop1m).ToListAsync()
);

// 3つの異なる粒度のデータを同時取得
Console.WriteLine($"5m: {data5m.Count}, 10m: {data10m.Count}, 15m: {data15m.Count}");

// ========================================
// 例5: リアルタイムダッシュボード
// ========================================

public class DashboardService
{
    private readonly KsqlContext _context;

    public async Task<MultiScaleStats> GetLatestStatsAsync(string symbol)
    {
        var hop1m = TimeSpan.FromMinutes(1);
        var now = DateTime.UtcNow;
        var oneHourAgo = now.AddHours(-1);

        // 複数のタイムスケールのデータを並行取得
        var tasks = new[]
        {
            Period.Min1,
            Period.Min5,
            Period.Min15,
            Period.Hour1
        }.Select(period => TimeBucket
            .GetHopping<TradingStats>(_context, period, hop1m)
            .ReadRangeAsync(oneHourAgo, now))
         .ToArray();

        var results = await Task.WhenAll(tasks);

        return new MultiScaleStats
        {
            OneMinute = results[0].FirstOrDefault(r => r.Symbol == symbol),
            FiveMinute = results[1].FirstOrDefault(r => r.Symbol == symbol),
            FifteenMinute = results[2].FirstOrDefault(r => r.Symbol == symbol),
            OneHour = results[3].FirstOrDefault(r => r.Symbol == symbol)
        };
    }
}
```

#### 6.5 **Option B: 統合API（Tumbling/Hopping両対応）**

```csharp
public static class TimeBucket
{
    /// <summary>
    /// Get time bucket reader (auto-detects Tumbling vs Hopping)
    /// </summary>
    public static ITimeBucketReader<T> Get<T>(
        KsqlContext ctx,
        Period period,
        TimeSpan? hopInterval = null) where T : class
    {
        if (hopInterval.HasValue)
            return new HoppingTimeBucket<T>(ctx, period, hopInterval.Value);
        else
            return new TumblingTimeBucket<T>(ctx, period);
    }
}

public interface ITimeBucketReader<T>
{
    Task<List<T>> ToListAsync(CancellationToken ct = default);
    Task<List<T>> ToListAsync(IReadOnlyList<string> pkFilter, CancellationToken ct = default);
}
```

**使用例**:
```csharp
// Tumbling
var tumbling = await TimeBucket.Get<Trade>(_ctx, Period.Min5).ToListAsync();

// Hopping
var hopping = await TimeBucket.Get<Trade>(_ctx, Period.Min5, hop: TimeSpan.FromMinutes(1)).ToListAsync();
```

#### 6.6 `EventSet<T>`の拡張（オプション）

**ストリーミング読み取り**も追加可能:

```csharp
public class EventSet<T> where T : class
{
    // NEW: Hoppingストリームをリアルタイム消費
    public IAsyncEnumerable<T> ConsumeHoppingAsync(
        Period period,
        TimeSpan hopInterval,
        CancellationToken ct = default)
    {
        var topicName = TimeBucketTypes.GetHoppingLiveTopicName(typeof(T), period, hopInterval);
        return ConsumeFromTopicAsync(topicName, ct);
    }
}
```

**使用例**:
```csharp
// リアルタイムでHoppingウィンドウの更新を受信
await foreach (var trade in _context.Set<Trade>()
    .ConsumeHoppingAsync(Period.Min5, TimeSpan.FromMinutes(1)))
{
    Console.WriteLine($"New 5m window: {trade.Symbol} @ {trade.AvgPrice}");
}
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

#### **Milestone 5: ランタイムパイプライン** (Week 5-6)
- [ ] `HoppingQao`クラス実装
- [ ] `HoppingDerivationPlanner`実装（複数エンティティ計画）
- [ ] `DerivedHoppingPipeline`実装（DDL実行オーケストレーション）
- [ ] `HoppingEntityDdlPlanner`実装
- [ ] `Role.HoppingLive`列挙値追加
- [ ] `TimeBucketTypes.RegisterHoppingRead()`実装
- [ ] パイプライン統合テスト作成

#### **Milestone 6: 読み取りAPI** (Week 6-7)
- [ ] `HoppingTimeBucket<T>`クラス実装
- [ ] `TimeBucket.GetHopping()`メソッド追加
- [ ] `TimeBucketTypes.GetHoppingLiveTopicName()`実装
- [ ] `ReadRangeAsync()`時間範囲フィルタリング実装
- [ ] `EventSet<T>.ConsumeHoppingAsync()`実装（オプション）
- [ ] 読み取りAPIユニットテスト作成

#### **Milestone 7: 統合テスト** (Week 7-8)
- [ ] End-to-end統合テスト作成
- [ ] Kafkaとの統合テスト
- [ ] ksqlDBとの統合テスト
- [ ] 複数時間帯の並行読み取りテスト
- [ ] リアルタイムストリーミング消費テスト

#### **Milestone 8: ドキュメントとサンプル** (Week 8-9)
- [ ] API documentation作成
- [ ] サンプルプロジェクト作成（`examples/hopping-windows/`）
  - [ ] 基本的なHopping例
  - [ ] 複数時間帯のダッシュボード例
  - [ ] リアルタイム移動平均例
- [ ] README更新
- [ ] リリースノート作成

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

#### 3. Change Event頻度（重要）

**✅ Change eventは発生します**

HoppingウィンドウはTumblingと同様に`EMIT CHANGES`を使用し、changelog topicが自動生成されます：

```sql
CREATE TABLE trade_5m_hop1m_live AS
SELECT ...
FROM trades
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)
GROUP BY symbol
EMIT CHANGES;  -- ← ウィンドウ更新ごとにchange event発行
```

**⚠️ Hoppingの特性：change eventの増幅**

重複ウィンドウのため、**1メッセージで複数のchange eventが発生**：

```
例：5分ウィンドウ、1分hop

時刻10:03にメッセージ到着
↓
以下の5つのウィンドウが更新される：
- 09:59-10:04
- 10:00-10:05
- 10:01-10:06
- 10:02-10:07
- 10:03-10:08
↓
5つのchange eventが発行される（同一メッセージで）
```

**影響**:
- **Changelog topic**: Tumblingの (window_size / hop_interval) 倍のイベント数
  - 例：5分ウィンドウ、1分hop → 5倍のイベント
  - 例：1時間ウィンドウ、5分hop → 12倍のイベント
- **下流コンシューマー**: より高頻度のイベント処理が必要
- **ネットワーク帯域**: イベント増加に伴うトラフィック増大

**対策**:
- Hop間隔を適切に設定（過度に小さくしない）
- 下流で必要なウィンドウサイズのみを購読（複数サイズを全て購読しない）
- Changelog topic retention設定の見直し（ディスク使用量管理）
- コンシューマー側のバックプレッシャー対策

**Changelog topic命名規則**（ksqlDB自動生成）:
```
{queryId}-changelog

例：CTAS_TRADE_5M_HOP1M_123-changelog
```

**証拠**（既存実装）:
- `src/Infrastructure/Ksql/KsqlPersistentQueryMonitor.cs:437` - changelog topic生成
- `physicalTests/OssSamples/TumblingCtasCachePocTests.cs:125` - Tumblingでもchangelog確認済み

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

### v1.0 (MVP) - 早期動作確認重視

**スコープ**: 単一サイズHopping＋DSL/SQL生成＋最小ユニットテスト

**含まれるもの**:
- ✅ 単一ウィンドウサイズ/Hop間隔のみ（`Minutes = new[] { 5 }`形式は**後回し**）
- ✅ DSL API: `Hopping(time, windowSize, hopInterval)`（シンプルオーバーロード）
- ✅ SQL生成: `WINDOW HOPPING (SIZE X, ADVANCE BY Y)`構文
- ✅ 最小限のユニットテスト（既存Tumblingテストを焼き直し）
- ✅ `EMIT CHANGES`対応
- ✅ Grace period基本対応

**意図的に除外**（後続バージョンへ）:
- ❌ 複数ウィンドウサイズの同時サポート（`Minutes = new[] { 5, 10, 15 }`）
- ❌ ランタイムパイプライン（DerivedHoppingPipeline等）
- ❌ 読み取りAPI（HoppingTimeBucket等）
- ❌ 重複ウィンドウ管理ロジック（HoppingWindowManager）
- ❌ 統合テスト・物理テスト

**実装戦略**: 既存Tumblingテストを最小限変更して動作確認
- `tests/Query/Builders/KsqlCreateWindowedStatementBuilderTests.cs`の焼き直し
- SQL文字列生成の正しさのみ検証
- ksqlDB実行は手動確認で可

**成功基準**:
- [ ] `.Hopping(t => t.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))`が動作
- [ ] SQL生成: `WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)`が正しく生成
- [ ] `CREATE TABLE ... EMIT CHANGES`が生成される
- [ ] ユニットテストが全てパス

**所要時間**: 1-2日（実装）+ 1日（テスト/修正）

---

### v1.1 (拡張) - 複数時間帯サポート
- 複数ウィンドウサイズの同時サポート（`HoppingWindows { Minutes = new[] { 5, 10, 15 }, HopInterval = ... }`）
- ランタイムパイプライン実装（HoppingQao、DerivedHoppingPipeline等）
- 読み取りAPI実装（HoppingTimeBucket<T>）
- パフォーマンス最適化
- 統合テスト追加

### v1.2 (プロダクション対応)
- 重複ウィンドウ管理ロジック（HoppingWindowManager）
- メモリ最適化
- Change event頻度の監視メトリクス
- 追加のサンプルとドキュメント

### v2.0 (将来)
- Session Windows対応
- カスタムウィンドウ戦略対応

---

## 🚀 MVP実装ガイド（v1.0）

### 実装の優先順位

**Phase 1**: SQL生成ロジック（1日）
**Phase 2**: DSL API（半日）
**Phase 3**: ユニットテスト（半日）
**Phase 4**: 手動動作確認（半日）

---

### Phase 1: SQL生成ロジック拡張

#### 1.1 `KsqlCreateWindowedStatementBuilder.cs`の拡張

**ファイル**: `/src/Query/Builders/Statements/KsqlCreateWindowedStatementBuilder.cs`

**変更内容**:

```csharp
// 新しいメソッド追加
public static string Build(
    string name,
    KsqlQueryModel model,
    string timeframe,
    string? emitOverride = null,
    string? inputOverride = null,
    RenderOptions? options = null,
    TimeSpan? hopInterval = null)  // ← NEW: オプショナルパラメータ
{
    // 既存のBuild()と同じロジック
    var baseSql = /* ... */;

    // NEW: hopInterval指定時はHOPPING構文を使用
    var window = hopInterval.HasValue
        ? FormatHoppingWindow(timeframe, hopInterval.Value)
        : FormatWindow(timeframe);  // 既存のTUMBLING構文

    var sql = InjectWindowAfterFrom(baseSql, window);
    return sql;
}

// 新しいヘルパーメソッド追加
private static string FormatHoppingWindow(string timeframe, TimeSpan hop)
{
    var (windowValue, windowUnit) = ParseTimeframe(timeframe);
    var (hopValue, hopUnit) = FormatTimeSpan(hop);

    return $"WINDOW HOPPING (SIZE {windowValue} {windowUnit}, ADVANCE BY {hopValue} {hopUnit})";
}

private static (int Value, string Unit) ParseTimeframe(string tf)
{
    var unit = tf[^1];
    if (!int.TryParse(tf[..^1], out var val)) val = 1;

    var unitName = unit switch
    {
        's' => "SECONDS",
        'm' => "MINUTES",
        'h' => "HOURS",
        'd' => "DAYS",
        _ => "MINUTES"
    };

    return (val, unitName);
}

private static (int Value, string Unit) FormatTimeSpan(TimeSpan ts)
{
    if (ts.TotalSeconds < 60 && ts.TotalSeconds == (int)ts.TotalSeconds)
        return ((int)ts.TotalSeconds, "SECONDS");
    if (ts.TotalMinutes < 60 && ts.TotalMinutes == (int)ts.TotalMinutes)
        return ((int)ts.TotalMinutes, "MINUTES");
    if (ts.TotalHours < 24 && ts.TotalHours == (int)ts.TotalHours)
        return ((int)ts.TotalHours, "HOURS");
    return ((int)ts.TotalDays, "DAYS");
}
```

**影響範囲**: このファイルのみ（既存メソッドシグネチャは変更なし）

---

#### 1.2 `KsqlQueryModel.cs`の拡張（オプション）

**ファイル**: `/src/Query/Dsl/KsqlQueryModel.cs`

**変更内容**（最小限）:

```csharp
public class KsqlQueryModel
{
    // 既存フィールド
    public List<string> Windows { get; } = new();

    // NEW: Hop間隔を保持（MVPではオプション）
    public TimeSpan? HopInterval { get; set; }

    // 既存のIsAggregateQuery()は変更なし
    // → Hoppingもaggregate扱いなので既存ロジックで動作
}
```

---

### Phase 2: DSL API追加

#### 2.1 `KsqlQueryable<T1>`への新メソッド

**ファイル**: `/src/Query/Dsl/KsqlQueryable.cs`

**追加内容**（シンプルオーバーロードのみ）:

```csharp
/// <summary>
/// Apply hopping window with fixed size and advance interval (MVP: single window only)
/// </summary>
public KsqlQueryable<T1> Hopping(
    Expression<Func<T1, DateTime>> time,
    TimeSpan windowSize,
    TimeSpan hopInterval,
    TimeSpan? grace = null,
    bool continuation = false)
{
    // 検証
    if (hopInterval > windowSize)
        throw new ArgumentException("Hop interval cannot exceed window size");

    // Tumblingと同様のロジック
    _model.Extras["WindowType"] = "HOPPING";
    _model.HopInterval = hopInterval;

    if (time.Body is MemberExpression me)
        _model.TimeKey = me.Member.Name;
    else if (time.Body is UnaryExpression ue && ue.Operand is MemberExpression me2)
        _model.TimeKey = me2.Member.Name;

    // MVP: 単一ウィンドウのみなので、1つのみ追加
    var windowStr = FormatWindow(windowSize);
    _model.Windows.Add(windowStr);

    if (grace.HasValue)
        _model.GraceSeconds = (int)Math.Ceiling(grace.Value.TotalSeconds);

    _model.Continuation = continuation;
    _stage = QueryBuildStage.Window;
    return this;
}

private static string FormatWindow(TimeSpan ts)
{
    if (ts.TotalMinutes < 60 && ts.TotalMinutes == (int)ts.TotalMinutes)
        return $"{(int)ts.TotalMinutes}m";
    if (ts.TotalHours < 24 && ts.TotalHours == (int)ts.TotalHours)
        return $"{(int)ts.TotalHours}h";
    if (ts.TotalDays == (int)ts.TotalDays)
        return $"{(int)ts.TotalDays}d";
    return $"{(int)ts.TotalSeconds}s";
}
```

---

### Phase 3: ユニットテスト（既存Tumblingテストを焼き直し）

#### 3.1 新テストファイル作成

**ファイル**: `/tests/Query/Builders/KsqlCreateHoppingStatementBuilderTests.cs`

**内容**: 既存の`KsqlCreateWindowedStatementBuilderTests.cs`をコピーして変更

```csharp
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using System;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", TestLevel.L3)]
public class KsqlCreateHoppingStatementBuilderTests
{
    private class Trade
    {
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public double Price { get; set; }
    }

    // ========================================
    // Tumbling焼き直しテスト #1
    // 元: Build_Includes_Window_Tumbling_1m()
    // ========================================
    [Fact]
    public void Build_Includes_Window_Hopping_5m_1m()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(
                time: t => t.Timestamp,
                windowSize: TimeSpan.FromMinutes(5),
                hopInterval: TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new
            {
                g.Key,
                WindowStart = g.WindowStart(),
                AvgPrice = g.Average(x => x.Price)
            })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_avg_5m_hop1m",
            model: model,
            timeframe: "5m",
            hopInterval: TimeSpan.FromMinutes(1));

        // 検証: HOPPING構文が含まれる
        SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING");
        SqlAssert.ContainsNormalized(sql, "SIZE 5 MINUTES");
        SqlAssert.ContainsNormalized(sql, "ADVANCE BY 1 MINUTES");

        // 検証: TABLE生成（aggregateなのでTABLE）
        SqlAssert.StartsWithNormalized(sql, "CREATE TABLE IF NOT EXISTS trade_avg_5m_hop1m");
    }

    // ========================================
    // Tumbling焼き直しテスト #2
    // 元: Build_Live_Table_Uses_EmitChanges()
    // ========================================
    [Fact]
    public void Build_Hopping_Live_Table_Uses_EmitChanges()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(t => t.Timestamp, TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(2))
            .GroupBy(t => t.Symbol)
            .Select(g => new { g.Key, Avg = g.Average(x => x.Price) })
            .Build();

        var sql = KsqlCreateWindowedStatementBuilder.Build(
            name: "trade_10m_hop2m_live",
            model: model,
            timeframe: "10m",
            emitOverride: "EMIT CHANGES",
            inputOverride: null,
            hopInterval: TimeSpan.FromMinutes(2));

        SqlAssert.ContainsNormalized(sql, "EMIT CHANGES");
        SqlAssert.ContainsNormalized(sql, "WINDOW HOPPING");
    }

    // ========================================
    // Tumbling焼き直しテスト #3
    // 元: DetermineType_Tumbling_Returns_Table()
    // ========================================
    [Fact]
    public void DetermineType_Hopping_Returns_Table()
    {
        var model = new KsqlQueryRoot()
            .From<Trade>()
            .Hopping(t => t.Timestamp, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
            .GroupBy(t => t.Symbol)
            .Select(g => new { g.Key, Avg = g.Average(x => x.Price) })
            .Build();

        // Hoppingもaggregateなので、TABLEを返すはず
        Assert.Equal(StreamTableType.Table, model.DetermineType());
    }

    // ========================================
    // 新規テスト: Hop > Windowの検証
    // ========================================
    [Fact]
    public void Hopping_HopGreaterThanWindow_ThrowsException()
    {
        var query = new KsqlQueryRoot().From<Trade>();

        Assert.Throws<ArgumentException>(() =>
            query.Hopping(
                time: t => t.Timestamp,
                windowSize: TimeSpan.FromMinutes(5),
                hopInterval: TimeSpan.FromMinutes(10)));  // ← Hop > Window
    }
}
```

**焼き直すべきテスト**（優先度順）:

| 元テスト（Tumbling） | 新テスト（Hopping） | 検証内容 |
|-------------------|------------------|---------|
| `Build_Includes_Window_Tumbling_1m` | `Build_Includes_Window_Hopping_5m_1m` | SQL構文生成 |
| `Build_Live_Table_Uses_EmitChanges` | `Build_Hopping_Live_Table_Uses_EmitChanges` | EMIT CHANGES |
| `Build_WithWindow_Creates_Table` | `Build_Hopping_Creates_Table` | TABLE生成 |
| `DetermineType_Tumbling_Returns_Table` | `DetermineType_Hopping_Returns_Table` | 型判定 |

---

### Phase 4: 手動動作確認

#### 4.1 ksqlDBでの手動実行

```sql
-- 手動で生成されたSQLをksqlDBで実行して確認

CREATE TABLE trade_avg_5m_hop1m AS
SELECT
  Symbol,
  WINDOWSTART AS WindowStart,
  AVG(Price) AS AvgPrice
FROM trades
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)
GROUP BY Symbol
EMIT CHANGES;

-- 確認クエリ
SELECT * FROM trade_avg_5m_hop1m EMIT CHANGES LIMIT 10;
```

#### 4.2 動作確認チェックリスト

- [ ] SQL生成が正しい（`WINDOW HOPPING (SIZE X, ADVANCE BY Y)`）
- [ ] `CREATE TABLE`が生成される（STREAMではない）
- [ ] `EMIT CHANGES`が含まれる
- [ ] ksqlDBで実際に実行してエラーなし
- [ ] Change eventが発行される（changelog topicを確認）

---

### 実装時の注意点

1. **複数ウィンドウサイズは後回し**
   - `HoppingWindows { Minutes = new[] { 5, 10 } }`形式は実装しない
   - シンプルオーバーロードのみ

2. **ランタイムパイプラインは後回し**
   - `DerivedHoppingPipeline`等は実装しない
   - SQL生成のみに集中

3. **読み取りAPIは後回し**
   - `HoppingTimeBucket<T>`は実装しない
   - v1.1で対応

4. **既存コードへの影響最小化**
   - 既存メソッドシグネチャは変更しない
   - オプショナルパラメータで拡張

5. **テストはコピー＆変更**
   - Tumblingテストを焼き直すだけ
   - 新規ロジック最小限

---

### 期待される実装サイズ

- **新規コード**: 約100-150行
  - `KsqlCreateWindowedStatementBuilder.cs`: +50行
  - `KsqlQueryable.cs`: +30行
  - `KsqlQueryModel.cs`: +5行
  - テスト: +60行

- **変更コード**: ほぼなし（既存メソッドシグネチャ維持）

- **実装時間**: 2-3日（テスト含む）

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
