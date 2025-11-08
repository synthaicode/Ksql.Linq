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
            // 螟壽ｮｵ・・m, 5m, 15m, 60m(=60m)・・
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

    // TimeBucket 繝吶・繧ｹ縺ｮ蠕・ｩ滂ｼ・ull query 髱樔ｾ晏ｭ假ｼ・
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

    // 謖・ｮ壹＠縺・BucketStart(ﾂｱtolerance 遘・ 縺ｮ陦後′迴ｾ繧後ｋ縺ｾ縺ｧ蠕・ｩ・
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
                // 遽・峇繝吶・繧ｹ縺ｮ蠕・ｩ・ 雜ｳ縺ｮ螳夂ｾｩ縺ｯ譛滄俣縺ｫ蜷ｫ縺ｾ繧後ｋ蜃ｺ譚･莠九・髮・ｴ・↑縺ｮ縺ｧ [start, start+span) 縺ｫ蜈･縺｣縺ｦ縺・ｌ縺ｰOK
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
                // 霑ｽ蜉縺ｮ繧・ｉ縺手ｨｱ螳ｹ・・/- toleranceSeconds・・
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

        // Phase A: TimeBucket waits・育函謌仙ｾ後↓蠕・ｩ溘☆繧区婿驥昴↓螟画峩・・
        var buckets = new[]
        {
            (Period.Minutes(1),  "bar_1m_live"),
            (Period.Minutes(5),  "bar_5m_live"),
            (Period.Minutes(15), "bar_15m_live"),
            (Period.Minutes(60), "bar_60m_live")
        };
        // 蠕梧ｮｵ縺ｧ騾先ｬ｡蠕・ｩ溘☆繧九◆繧√√％縺ｮ谿ｵ髫弱〒縺ｯ襍ｷ蜍輔＠縺ｪ縺・


        // 蝓ｺ貅匁凾蛻ｻ・域ｱｺ螳夊ｫ厄ｼ・ 迴ｾ蝨ｨ譎ょ綾繧・腸蠅・､画焚縺ｫ萓晏ｭ倥＠縺ｪ縺・崋螳啅TC縺ｮ縺ｿ繧堤畑縺・ｋ
        // 蜀咲樟諤ｧ縺ｮ縺溘ａ PHYS_BASE_UTC 縺ｯ辟｡隕悶☆繧・        DateTime baseUtc = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var T0 = baseUtc.AddSeconds(5);
        // 繝舌こ繝・ヨ蠅・阜・亥ｸｸ縺ｫ驕主悉繝ｻ豎ｺ螳夊ｫ厄ｼ・        DateTime M1  = new DateTime((T0.Ticks / TimeSpan.TicksPerMinute) * TimeSpan.TicksPerMinute, DateTimeKind.Utc);
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

        // 蜈･蜉帙う繝吶Φ繝磯寔蜷茨ｼ域､懆ｨｼ縺ｫ蜀榊茜逕ｨ縺吶ｋ・・        var inputEvents = new System.Collections.Generic.List<(DateTime ts, decimal bid)>();

        async Task ProduceAsync()
        {
            // 螳夂ｾｩ縺励◆蜈･蜉帙ｒ荳蜈・喧縺励√う繝吶Φ繝域凾蛻ｻ譏・・〒謚募・・亥腰隱ｿ諤ｧ繧ｬ繝ｼ繝峨→CLOSE螳牙ｮ壹・荳｡遶具ｼ・            var events = new System.Collections.Generic.List<(DateTime ts, decimal bid)>();
            // 莠句燕繧ｷ繝ｼ繝会ｼ・1繧医ｊ蜑阪・3蛻・ｼ峨ｂ譛ｬ謚募・縺ｨ蜷御ｸ繝ｫ繝ｼ繝医〒謇ｱ縺・ｼ亥庄隕門喧繝ｬ繝ｼ繧ｹ繧帝∩縺代ｋ・・            events.Add((M1.AddMinutes(-3).AddSeconds(10), 43m));
            events.Add((M1.AddMinutes(-2).AddSeconds(10), 42m));
            events.Add((M1.AddMinutes(-1).AddSeconds(10), 41m));
            // 1m
            events.Add((M1.AddSeconds(5), 100m));
            events.Add((M1.AddSeconds(15), 103m));
            events.Add((M1.AddSeconds(30), 99m));
            events.Add((M1.AddSeconds(55), 102m));
            // 5m・・..4蛻・・10遘抵ｼ・            var vals5 = new[] { 101m, 120m, 95m, 104m, 103m };
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

        // 蛻･繧ｷ繝ｼ繝画兜蜈･縺ｯ蟒・ｭ｢・域悽謚募・繝ｫ繝ｼ繝医〒荳諡ｬ謚募・貂医∩・・
        // 騾先ｬ｡縺ｮ蟄伜惠遒ｺ隱阪・OHLC讀懆ｨｼ繝悶Ο繝・け縺ｧ陦後≧縺溘ａ縲√％縺薙〒縺ｮ荳諡ｬ蠕・ｩ溘・逵∫払


        // Phase B 窶・蜴ｳ蟇・HLC: 1m/5m/15m 繧・TimeBucket 縺ｧ讀懆ｨｼ・・ull 繧剃ｽｿ逕ｨ縺励↑縺・ｼ・
        var bucket1m  = TimeBucket.Get<Bar>(ctx, Period.Minutes(1));
        var bucket5m  = TimeBucket.Get<Bar>(ctx, Period.Minutes(5));
        var bucket15m = TimeBucket.Get<Bar>(ctx, Period.Minutes(15));

        // 隕ｳ貂ｬ邉ｻ・郁ｨｺ譁ｭ讖溯・・峨・蜑企勁・壹ユ繧ｹ繝医・蜈･蜉帚・譛溷ｾ・・蜉帙・縺ｿ縺ｧ隧穂ｾ｡縺吶ｋ

        // 隕ｳ貂ｬ讖溯・・・oreach/query-stream・峨・繝・せ繝郁ｲｬ蜍吝､悶・縺溘ａ謦､蜴ｻ

        // DESCRIBE/BUCKETSTART 繧ｹ繝翫ャ繝励す繝ｧ繝・ヨ繧ょ炎髯､・郁ｨｺ譁ｭ縺ｯ蛻･邉ｻ邨ｱ縺ｧ螳滓命・・
        // lag warm-up 繧ょ炎髯､・郁ｦｳ貂ｬ縺ｯ繝・せ繝亥､厄ｼ・
        // 蛻晏屓Pull蠕・■繧ょ炎髯､・・imeBucket縺ｮ蠕・ｩ溘〒蜊∝・・・
        // rows_last 逶｣隕悶・繝・せ繝郁ｲｬ蜍吝､悶・縺溘ａ謦､蜴ｻ・育ｵ先棡遒ｺ隱阪・ TimeBucket 縺ｮ縺ｿ縺ｧ陦後≧・・
        // 隕ｳ貂ｬ邉ｻ繧・芦譚･蠕・■繧定｡後ｏ縺壹ゝimeBucket 縺ｮ邨先棡縺ｮ縺ｿ縺ｧ蛻､螳壹☆繧・
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
            // 險ｱ螳ｹ: WindowStart 縺悟宍蟇・ｧ貞｢・阜縺ｨﾂｱ1遘剃ｻ･蜀・↓關ｽ縺｡繧句ｴ蜷医ｂ諡ｾ縺・            return bars
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

        // 陬懷勧: 繝舌こ繝・ヨ蠅・阜髢｢謨ｰ縺ｨ豁｣隕丞喧
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

        // 1m 遯捺焚・亥崋螳壼渕貅匁凾蛻ｻ荳九・諠ｳ螳・19莉ｶ縺ｧ蝗ｺ螳夲ｼ・
        Assert.Equal(19, expected1m.Count);
        Assert.Equal(19, list1m.Count);\r\n        // 1m: 蜷・ｪ薙〒莉ｶ謨ｰ=1・磯℃荳崎ｶｳ縺ｪ縺暦ｼ噂r\n        foreach (var start in expected1m)\r\n        {\r\n            var rows = RowsInWindow(list1m, Period.Minutes(1), start);\r\n            Assert.Single(rows);\r\n        }
        // 1m: 蜷・ｪ薙〒莉ｶ謨ｰ=1・磯℃荳崎ｶｳ縺ｪ縺暦ｼ・
        foreach (var start in expected1m)
        {
            var rows = RowsInWindow(list1m, Period.Minutes(1), start);
            Assert.Single(rows);
        }
        AssertSetEqual(expected5m, actual5m, "5m");
        // 5m 遯捺焚・亥崋螳壼渕貅匁凾蛻ｻ荳九・諠ｳ螳・10莉ｶ縺ｧ蝗ｺ螳夲ｼ・
        Assert.Equal(10, expected5m.Count);
        list5m = await bucket5m.ToListAsync(new[] { 'B1', 'S1' }, CancellationToken.None);
        Assert.Equal(10, list5m.Count);\r\n        // 5m: 蜷・ｪ薙〒莉ｶ謨ｰ=1\r\n        foreach (var start in expected5m)\r\n        {\r\n            var rows = RowsInWindow(list5m, Period.Minutes(5), start);\r\n            Assert.Single(rows);\r\n        }
        // 5m: 蜷・ｪ薙〒莉ｶ謨ｰ=1
        foreach (var start in expected5m)
        {
            var rows = RowsInWindow(list5m, Period.Minutes(5), start);
            Assert.Single(rows);
        }
        AssertSetEqual(expected15m, actual15m, "15m");
        // 15m 邱丈ｻｶ謨ｰ・亥崋螳壼渕貅匁凾蛻ｻ縺ｮ荳九〒6莉ｶ縺ｧ豎ｺ螳夲ｼ・        Assert.Equal(6, expected15m.Count);
        list15m = await bucket15m.ToListAsync(new[] { "B1", "S1" }, CancellationToken.None);
        Assert.Equal(6, list15m.Count);
        AssertSetEqual(expected60m, actual60m, "60m");
        // 60m 邱丈ｻｶ謨ｰ・亥崋螳壼渕貅匁凾蛻ｻ縺ｮ荳九〒3莉ｶ縺ｧ豎ｺ螳夲ｼ・        Assert.Equal(3, expected60m.Count);
        list60m = await bucket60m.ToListAsync(new[] { "B1", "S1" }, CancellationToken.None);
        Assert.Equal(3, list60m.Count);

        // 縺吶∋縺ｦ縺ｮ譛滄俣縺ｫ縺､縺・※縲∝推繝舌こ繝・ヨ縺ｮOHLC縺悟・蜉帙う繝吶Φ繝医°繧臥ｮ怜・縺励◆譛溷ｾ・→螳悟・荳閾ｴ・亥叉蛟､・峨☆繧九％縺ｨ繧呈､懆ｨｼ
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

        // 譛邨・ 縺吶∋縺ｦ縺ｮ讀懆ｨｼ繧帝夐℃
        // 譏守､ｺ逧・↑繧ｯ繝ｪ繝ｼ繝ｳ繧｢繝・・荳崎ｦ・ｼ・AsyncDisposable・・    }
}




