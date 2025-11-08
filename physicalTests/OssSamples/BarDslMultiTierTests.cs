using Ksql.Linq;
using Ksql.Linq.Runtime;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Xunit;
using Xunit.Sdk;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using System.Threading;

namespace Ksql.Linq.Tests.Integration;

public class BarDslMultiTierTests
{
    [KsqlTopic("physical-test-events")]
    public class IncidentEvent
    {
        public string Name { get; set; } = string.Empty;
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public string Period { get; set; } = string.Empty;
        public DateTime TargetBucketStartUtc { get; set; }
        public int ObservedCount { get; set; }
        public DateTime TimestampUtc { get; set; }
    }
    [KsqlTopic("deduprates")] 
    public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        [KsqlDecimal(18,2)]
        public decimal Bid { get; set; }
    }

    public class Bar
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)]
        [KsqlTimestamp]
        public DateTime BucketStart { get; set; }
        [KsqlDecimal(18,4)] public decimal Open { get; set; }
        [KsqlDecimal(18,4)] public decimal High { get; set; }
        [KsqlDecimal(18,4)] public decimal Low { get; set; }
        [KsqlDecimal(18,4)] public decimal KsqlTimeFrameClose { get; set; }
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            // Keep our library logs at Debug for diagnostics, but suppress Streamiz debug noise
            b.SetMinimumLevel(LogLevel.Debug);
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
            b.AddFilter("Ksql.Linq", LogLevel.Debug);
        });
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            // 多段（1m, 5m, 15m, 60m(=60m)）
            modelBuilder.Entity<Bar>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1, 5, 15, 60 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Bar
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                    }));
            // Incident events for diagnostics
            modelBuilder.Entity<IncidentEvent>();
        }
    }

    private static async Task<bool> SrDeleteSubjectAsync(string subject)
    {
        try
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18081") };
            using var req = new HttpRequestMessage(HttpMethod.Delete, $"/subjects/{Uri.EscapeDataString(subject)}?permanent=true");
            using var resp = await http.SendAsync(req);
            return resp.IsSuccessStatusCode;
        }
        catch { return false; }
    }
    private static async Task<bool> SrSetCompatNoneAsync(string subject)
    {
        try
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18081") };
            var payload = new { compatibility = "NONE" };
            var json = JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var resp = await http.PutAsync($"/config/{Uri.EscapeDataString(subject)}", content);
            return resp.IsSuccessStatusCode;
        }
        catch { return false; }
    }

    private static async Task<int> QueryStreamCountHttpAsync(string sql, int limit, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new
        {
            sql,
            properties = new Dictionary<string, object>
            {
                ["ksql.streams.auto.offset.reset"] = "earliest"
            }
        };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var req = new HttpRequestMessage(HttpMethod.Post, "/query-stream") { Content = content };
        using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        resp.EnsureSuccessStatusCode();
        int count = 0;
        await using var stream = await resp.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new System.IO.StreamReader(stream, Encoding.UTF8);
        while (!reader.EndOfStream && !cts.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) break;
            if (line.IndexOf("\"row\"", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                count++;
                if (count >= limit) break;
            }
        }
        return count;
    }

    private static async Task<List<object?[]>> QueryRowsHttpAsync(string sql, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new { sql, properties = new Dictionary<string, object>() };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var resp = await http.PostAsync("/query", content, cts.Token);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(cts.Token);
        var rows = new List<object?[]>();
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty("row", out var rowEl))
                    {
                        if (rowEl.TryGetProperty("columns", out var cols) && cols.ValueKind == JsonValueKind.Array)
                        {
                            var arr = new object?[cols.GetArrayLength()];
                            int idx = 0;
                            foreach (var c in cols.EnumerateArray())
                            {
                                arr[idx++] = c.ValueKind switch
                                {
                                    JsonValueKind.Number => c.TryGetInt64(out var l) ? l : c.GetDouble(),
                                    JsonValueKind.String => c.GetString(),
                                    JsonValueKind.True => true,
                                    JsonValueKind.False => false,
                                    JsonValueKind.Null => null,
                                    _ => c.ToString()
                                };
                            }
                            rows.Add(arr);
                        }
                    }
                }
            }
        }
        catch { }
        return rows;
    }


    private static async Task<int> WaitPullCountHttpAsync(string table, int min, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        int last = 0;
        while (DateTime.UtcNow < deadline)
        {
            var rows = await QueryRowsHttpAsync($"SELECT BucketStart, Open, High, Low, KsqlTimeFrameClose FROM {table} WHERE Broker='B1' AND Symbol='S1';", TimeSpan.FromSeconds(15));
            last = rows.Count;
            if (last >= min)
            {
                return last;
            }
            await Task.Delay(1000);
        }

        return last;
    }

    // TimeBucket ベースの待機（pull query 非依存）
    private static async Task<int> WaitBucketCountAsync(KsqlContext ctx, Period period, int min, TimeSpan timeout, string broker, string symbol)
    {
        var bucket = TimeBucket.Get<Bar>(ctx, period);
        var deadline = DateTime.UtcNow + timeout;
        int last = 0;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var list = await bucket.ToListAsync(new[] { broker, symbol }, CancellationToken.None);
                last = list.Count;
                if (last >= min) return last;
            }
            catch (Exception)
            {
                // Cache not ready yet; retry shortly
            }
            await Task.Delay(1000);
        }
        return last;
    }

    // 指定した BucketStart(±tolerance 秒) の行が現れるまで待機
    private static async Task<bool> WaitBucketRowAsync(KsqlContext ctx, Period period, string broker, string symbol, DateTime bucketStartUtc, TimeSpan timeout, double toleranceSeconds = 0)
    {
        var bucket = TimeBucket.Get<Bar>(ctx, period);
        var deadline = DateTime.UtcNow + timeout;
        List<Bar>? last = null;
        static DateTime NormalizeMsUtc(DateTime dt)
        {
            var k = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return new DateTime(k.Year, k.Month, k.Day, k.Hour, k.Minute, k.Second, DateTimeKind.Utc);
        }
        static DateTime NormalizeWithEpsilon(DateTime dt)
        {
            // Shift by -10ms to tolerate strict "<" vs "<=" style boundary evaluations at 1s precision
            var k = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            var shifted = k.AddMilliseconds(-10);
            return new DateTime(shifted.Year, shifted.Month, shifted.Day, shifted.Hour, shifted.Minute, shifted.Second, DateTimeKind.Utc);
        }
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var list = await bucket.ToListAsync(new[] { broker, symbol }, CancellationToken.None);
                last = list;
                // 範囲ベースの待機: 足の定義は期間に含まれる出来事の集約なので [start, start+span) に入っていればOK
                var span = period.Unit switch
                {
                    PeriodUnit.Minutes => TimeSpan.FromMinutes(period.Value),
                    PeriodUnit.Hours => TimeSpan.FromHours(period.Value),
                    PeriodUnit.Days => TimeSpan.FromDays(period.Value),
                    _ => TimeSpan.FromMinutes(1)
                };
                var start = NormalizeMsUtc(bucketStartUtc);
                var end = start + span;
                if (list.Any(b => NormalizeWithEpsilon(b.BucketStart) >= start && NormalizeWithEpsilon(b.BucketStart) < end)) return true;
                // 追加のゆらぎ許容（+/- toleranceSeconds）
                if (toleranceSeconds > 0)
                {
                    if (list.Any(b => Math.Abs((NormalizeWithEpsilon(b.BucketStart) - start).TotalSeconds) <= toleranceSeconds)) return true;
                }
            }
            catch { }
            await Task.Delay(1000);
        }
        try
        {
            // emit diagnostic event file under reports/physical/events
            var root = Environment.CurrentDirectory;
            var dir = System.IO.Path.Combine(root, "reports", "physical", "events");
            System.IO.Directory.CreateDirectory(dir);
            var file = System.IO.Path.Combine(dir, $"timebucket_timeout_{period.Value}{period.Unit}_B1_S1_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json");
            var payload = new
            {
                kind = "timebucket_timeout",
                period = new { value = period.Value, unit = period.Unit.ToString() },
                broker,
                symbol,
                targetBucketStartUtc = bucketStartUtc,
                toleranceSeconds,
                liveTopic = bucket.LiveTopicName,
                observedCount = last?.Count ?? 0,
                observedStarts = last?.Select(b => b.BucketStart).Take(10).ToArray() ?? Array.Empty<DateTime>(),
                timestampUtc = DateTime.UtcNow
            };
            var json = System.Text.Json.JsonSerializer.Serialize(payload, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            System.IO.File.WriteAllText(file, json);
            Console.WriteLine($"[diag] timebucket timeout event written: {file}");
            // Also register incident event to Kafka (physical-test-events)
            try
            {
                await ctx.Set<IncidentEvent>().AddAsync(new IncidentEvent
                {
                    Name = "timebucket_timeout",
                    Broker = broker,
                    Symbol = symbol,
                    Period = $"{period.Value}{period.Unit}",
                    TargetBucketStartUtc = bucketStartUtc,
                    ObservedCount = last?.Count ?? 0,
                    TimestampUtc = DateTime.UtcNow
                });
                Console.WriteLine("[diag] incident event produced to topic 'physical-test-events'.");
            }
            catch { }
        }
        catch { }
        return false;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultiTier_1m_5m_15m_60m_Create_And_Ohlc_Sanity()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        await using var ctx = new TestContext();

        // Schema Registry subjects cleanup for decimal schemas (idempotent, best-effort)
        var subjects = new[]
        {
            "deduprates-value",
            "bar_1m_live-value",
            "bar_5m_live-value",
            "bar_15m_live-value",
            "bar_60m_live-value"
        };
        foreach (var s in subjects)
        {
            await SrSetCompatNoneAsync(s);
            await SrDeleteSubjectAsync(s);
        }

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "deduprates", 1, 1, TimeSpan.FromSeconds(10));
        }
        await ctx.WaitForEntityReadyAsync<Rate>(TimeSpan.FromSeconds(60));

        // Phase A: TimeBucket waits（生成後に待機する方針に変更）
        var buckets = new[]
        {
            (Period.Minutes(1),  "bar_1m_live"),
            (Period.Minutes(5),  "bar_5m_live"),
            (Period.Minutes(15), "bar_15m_live"),
            (Period.Minutes(60), "bar_60m_live")
        };
        // 後段で逐次待機するため、この段階では起動しない


        // 基準時刻（決定論）: 現在時刻や環境変数に依存しない固定UTCのみを用いる
        // 再現性のため PHYS_BASE_UTC は無視する
        DateTime baseUtc = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var T0 = baseUtc.AddSeconds(5);
        // バケット境界（常に過去・決定論）
        DateTime M1  = new DateTime((T0.Ticks / TimeSpan.TicksPerMinute) * TimeSpan.TicksPerMinute, DateTimeKind.Utc);
        DateTime M5  = new DateTime((T0.Ticks / (TimeSpan.TicksPerMinute*5)) * (TimeSpan.TicksPerMinute*5), DateTimeKind.Utc);
        DateTime M15 = new DateTime((T0.Ticks / (TimeSpan.TicksPerMinute*15)) * (TimeSpan.TicksPerMinute*15), DateTimeKind.Utc);
        DateTime W0  = new DateTime((baseUtc.Ticks / TimeSpan.TicksPerHour) * TimeSpan.TicksPerHour, DateTimeKind.Utc);
        DateTime W1  = W0.AddHours(1);

        // Producer routine without Task.Run
        int GetDupCount()
        {
            try
            {
                var s = Environment.GetEnvironmentVariable("PHYS_DUP")
                        ?? Environment.GetEnvironmentVariable("PHYS_RETRY")
                        ?? Environment.GetEnvironmentVariable("PHYS_RETRY_COUNT");
                if (!string.IsNullOrWhiteSpace(s) && int.TryParse(s, out var n) && n > 0 && n <= 100)
                    return n;
            }
            catch { }
            return 1;
        }

        // 入力イベント集合（検証に再利用する）
        var inputEvents = new System.Collections.Generic.List<(DateTime ts, decimal bid)>();

        async Task ProduceAsync()
        {
            // 定義した入力を一元化し、イベント時刻昇順で投入（単調性ガードとCLOSE安定の両立）
            var events = new System.Collections.Generic.List<(DateTime ts, decimal bid)>();
            // 事前シード（M1より前の3分）も本投入と同一ルートで扱う（可視化レースを避ける）
            events.Add((M1.AddMinutes(-3).AddSeconds(10), 43m));
            events.Add((M1.AddMinutes(-2).AddSeconds(10), 42m));
            events.Add((M1.AddMinutes(-1).AddSeconds(10), 41m));
            // 1m
            events.Add((M1.AddSeconds(5), 100m));
            events.Add((M1.AddSeconds(15), 103m));
            events.Add((M1.AddSeconds(30), 99m));
            events.Add((M1.AddSeconds(55), 102m));
            // 5m（0..4分の10秒）
            var vals5 = new[] { 101m, 120m, 95m, 104m, 103m };
            for (int i = 0; i < 5; i++) events.Add((M5.AddMinutes(i).AddSeconds(10), vals5[i]));
            // 15m
            events.Add((M15.AddSeconds(3), 200m));
            events.Add((M15.AddMinutes(6).AddSeconds(10), 230m));
            events.Add((M15.AddMinutes(12).AddSeconds(20), 190m));
            events.Add((M15.AddMinutes(14).AddSeconds(55), 205m));
            // 60m W0/W1
            events.Add((W0.AddMinutes(1), 300m));
            events.Add((W0.AddMinutes(10), 340m));
            events.Add((W0.AddMinutes(40), 280m));
            events.Add((W0.AddMinutes(59).AddSeconds(50), 310m));
            events.Add((W1.AddSeconds(-1), 999m));
            events.Add((W1.AddSeconds(1), 1m));
            events.Add((W1.AddMinutes(1), 400m));
            events.Add((W1.AddMinutes(5), 360m));
            events.Add((W1.AddMinutes(50), 450m));
            events.Add((W1.AddMinutes(59).AddSeconds(50), 420m));

            events.Sort((a, b) => a.ts.CompareTo(b.ts));
            inputEvents.AddRange(events);
            var dup = GetDupCount();
            foreach (var e in events)
            {
                for (int i = 0; i < dup; i++)
                {
                    await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = e.ts, Bid = e.bid });
                    if (i + 1 < dup) await Task.Delay(5);
                }
                await Task.Delay(50);
            }
        }

        await ProduceAsync();

        // 別シード投入は廃止（本投入ルートで一括投入済み）

        // 逐次の存在確認はOHLC検証ブロックで行うため、ここでの一括待機は省略


        // Phase B — 厳密OHLC: 1m/5m/15m を TimeBucket で検証（pull を使用しない）
        var bucket1m  = TimeBucket.Get<Bar>(ctx, Period.Minutes(1));
        var bucket5m  = TimeBucket.Get<Bar>(ctx, Period.Minutes(5));
        var bucket15m = TimeBucket.Get<Bar>(ctx, Period.Minutes(15));

        // 観測系（診断機能）は削除：テストは入力→期待出力のみで評価する

        // 観測機能（foreach/query-stream）はテスト責務外のため撤去

        // DESCRIBE/BUCKETSTART スナップショットも削除（診断は別系統で実施）

        // lag warm-up も削除（観測はテスト外）

        // 初回Pull待ちも削除（TimeBucketの待機で十分）

        // rows_last 監視はテスト責務外のため撤去（結果確認は TimeBucket のみで行う）

        // 観測系や到来待ちを行わず、TimeBucket の結果のみで判定する

        static Bar? FindRowForStart(List<Bar> bars, Period period, DateTime startUtc)
        {
            DateTime Normalize(DateTime dt)
                => dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            var start = Normalize(startUtc);
            var span = period.Unit switch
            {
                PeriodUnit.Minutes => TimeSpan.FromMinutes(period.Value),
                PeriodUnit.Hours => TimeSpan.FromHours(period.Value),
                PeriodUnit.Days => TimeSpan.FromDays(period.Value),
                _ => TimeSpan.FromMinutes(1)
            };
            var end = start + span;
            // 許容: WindowStart が厳密秒境界と±1秒以内に落ちる場合も拾う
            return bars
                .Where(b =>
                {
                    var t = Normalize(b.BucketStart).AddMilliseconds(-10);
                    return t >= start && t < end;
                })
                .OrderBy(b => b.BucketStart)
                .FirstOrDefault();
        }

        static List<Bar> RowsInWindow(List<Bar> bars, Period period, DateTime startUtc)
        {
            DateTime Normalize(DateTime dt)
                => dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            var start = Normalize(startUtc);
            var span = period.Unit switch
            {
                PeriodUnit.Minutes => TimeSpan.FromMinutes(period.Value),
                PeriodUnit.Hours => TimeSpan.FromHours(period.Value),
                PeriodUnit.Days => TimeSpan.FromDays(period.Value),
                _ => TimeSpan.FromMinutes(1)
            };
            var end = start + span;
            return bars
                .Where(b =>
                {
                    var t = Normalize(b.BucketStart).AddMilliseconds(-10);
                    return t >= start && t < end;
                })
                .OrderBy(b => b.BucketStart)
                .ToList();
        }

        // 補助: バケット境界関数と正規化
        static DateTime NormalizeUtcSec(DateTime dt)
        {
            var k = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return new DateTime(k.Year, k.Month, k.Day, k.Hour, k.Minute, k.Second, DateTimeKind.Utc);
        }
        static DateTime FloorToPeriod(DateTime tsUtc, Period p)
        {
            var t = NormalizeUtcSec(tsUtc);
            long ticks = t.Ticks;
            if (p.Unit == PeriodUnit.Minutes)
            {
                var span = TimeSpan.FromMinutes(p.Value).Ticks;
                return new DateTime((ticks / span) * span, DateTimeKind.Utc);
            }
            if (p.Unit == PeriodUnit.Hours)
            {
                var span = TimeSpan.FromHours(p.Value).Ticks;
                return new DateTime((ticks / span) * span, DateTimeKind.Utc);
            }
            if (p.Unit == PeriodUnit.Days)
            {
                var span = TimeSpan.FromDays(p.Value).Ticks;
                return new DateTime((ticks / span) * span, DateTimeKind.Utc);
            }
            // default minutes(1)
            var dspan = TimeSpan.FromMinutes(1).Ticks;
            return new DateTime((ticks / dspan) * dspan, DateTimeKind.Utc);
        }

        // 1m 窓数（固定基準時刻下の想定=19件で固定）
        Assert.Equal(19, expected1m.Count);
        Assert.Equal(19, list1m.Count);\r\n        // 1m: 各窓で件数=1（過不足なし）\r\n        foreach (var start in expected1m)\r\n        {\r\n            var rows = RowsInWindow(list1m, Period.Minutes(1), start);\r\n            Assert.Single(rows);\r\n        }
        // 1m: 各窓で件数=1（過不足なし）
        foreach (var start in expected1m)
        {
            var rows = RowsInWindow(list1m, Period.Minutes(1), start);
            Assert.Single(rows);
        }
        AssertSetEqual(expected5m, actual5m, "5m");
        // 5m 窓数（固定基準時刻下の想定=10件で固定）
        Assert.Equal(10, expected5m.Count);
        list5m = await bucket5m.ToListAsync(new[] { 'B1', 'S1' }, CancellationToken.None);
        Assert.Equal(10, list5m.Count);\r\n        // 5m: 各窓で件数=1\r\n        foreach (var start in expected5m)\r\n        {\r\n            var rows = RowsInWindow(list5m, Period.Minutes(5), start);\r\n            Assert.Single(rows);\r\n        }
        // 5m: 各窓で件数=1
        foreach (var start in expected5m)
        {
            var rows = RowsInWindow(list5m, Period.Minutes(5), start);
            Assert.Single(rows);
        }
        AssertSetEqual(expected15m, actual15m, "15m");
        // 15m 総件数（固定基準時刻の下で6件で決定）
        Assert.Equal(6, expected15m.Count);
        list15m = await bucket15m.ToListAsync(new[] { "B1", "S1" }, CancellationToken.None);
        Assert.Equal(6, list15m.Count);
        AssertSetEqual(expected60m, actual60m, "60m");
        // 60m 総件数（固定基準時刻の下で3件で決定）
        Assert.Equal(3, expected60m.Count);
        list60m = await bucket60m.ToListAsync(new[] { "B1", "S1" }, CancellationToken.None);
        Assert.Equal(3, list60m.Count);

        // すべての期間について、各バケットのOHLCが入力イベントから算出した期待と完全一致（即値）することを検証
        static Dictionary<DateTime, (decimal open, decimal high, decimal low, decimal close)> BuildExpected(
            IEnumerable<(DateTime ts, decimal bid)> evs, Period p)
        {
            var groups = evs
                .Select(e => (start: FloorToPeriod(e.ts, p), e.ts, e.bid))
                .GroupBy(x => NormalizeUtcSec(x.start));
            var dict = new Dictionary<DateTime, (decimal, decimal, decimal, decimal)>();
            foreach (var g in groups)
            {
                var ordered = g.OrderBy(x => x.ts).ToList();
                var open = ordered.First().bid;
                var close = ordered.Last().bid;
                var high = ordered.Max(x => x.bid);
                var low = ordered.Min(x => x.bid);
                dict[g.Key] = (open, high, low, close);
            }
            return dict;
        }

        static void AssertAllBars(List<Bar> bars, Period p, string label,
            Dictionary<DateTime, (decimal open, decimal high, decimal low, decimal close)> expected)
        {
            var byStart = bars.ToDictionary(b => NormalizeUtcSec(b.BucketStart));
            foreach (var kv in expected)
            {
                Assert.True(byStart.ContainsKey(kv.Key), $"{label}: missing bar at {kv.Key:o}");
                var b = byStart[kv.Key];
                var exp = kv.Value;
                Assert.Equal(exp.open, b.Open);
                Assert.Equal(exp.high, b.High);
                Assert.Equal(exp.low, b.Low);
                Assert.Equal(exp.close, b.KsqlTimeFrameClose);
            }
        }

        var exp1m = BuildExpected(inputEvents, Period.Minutes(1));
        var exp5m = BuildExpected(inputEvents, Period.Minutes(5));
        var exp15m = BuildExpected(inputEvents, Period.Minutes(15));
        var exp60m = BuildExpected(inputEvents, Period.Minutes(60));

        AssertAllBars(list1m, Period.Minutes(1),  "1m",  exp1m);
        AssertAllBars(list5m, Period.Minutes(5),  "5m",  exp5m);
        AssertAllBars(list15m, Period.Minutes(15), "15m", exp15m);
        AssertAllBars(list60m, Period.Minutes(60), "60m", exp60m);

        // 最終: すべての検証を通過
        // 明示的なクリーンアップ不要（IAsyncDisposable）
    }
}





