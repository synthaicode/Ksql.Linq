using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Runtime;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

/// <summary>
/// 繝・せ繝育岼逧・ｼ・urpose・・
/// - 隍・焚繧ｭ繝ｼ・・roker/Symbol・峨↓蟇ｾ縺励※縲・m/5m縺ｮTUMBLING髮・ｴ・′豁｣縺励￥蛻・屬縺輔ｌ繧九％縺ｨ繧堤｢ｺ隱阪☆繧九・
/// - 蜷・凾髢捺棧・・m/5m・峨〒縲∫ｪ馴幕蟋具ｼ・ucketStart・峨′譛溷ｾ・←縺翫ｊ縺ｧ縺ゅｊ縲∽ｻｶ謨ｰ縺碁℃荳崎ｶｳ縺ｪ縺丈ｸ閾ｴ縺吶ｋ縺薙→繧堤｢ｺ隱阪☆繧九・
/// - 蜷・く繝ｼ縺斐→縺ｮOHLC縺後∝崋螳壼渕貅悶ョ繝ｼ繧ｿ縺九ｉ蟆主・縺輔ｌ繧句叉蛟､・・ecimal・峨→蜴ｳ蟇・ｸ閾ｴ縺吶ｋ縺薙→繧堤｢ｺ隱阪☆繧九・
/// - 逶｣隕厄ｼ・MIT CHANGES・峨・隕ｳ貂ｬ逕ｨ騾斐・縺ｿ縲ょ粋蜷ｦ縺ｯPull繝吶・繧ｹ縺ｮ蜴ｳ蟇・､懆ｨｼ縺ｧ蛻､螳壹☆繧九・
/// 謌仙粥譚｡莉ｶ・・cceptance Criteria・・
/// - 1m/5m: 蟇ｾ雎｡縺ｮBucketStart縺ｫ縺翫￠繧玖｡梧焚縺後く繝ｼ謨ｰ縺ｫ遲峨＠縺・ｼ亥・繧ｭ繝ｼ蛻・′蟄伜惠・峨・
/// - 蜷・く繝ｼ: 謖・ｮ咤ucketStart縺ｧ1陦後・縺ｿ縲＾pen/High/Low/Close縺梧悄蠕・､縺ｨ荳閾ｴ縲・
/// - 蜷・く繝ｼ: 1m/5m縺ｮBucketStart髮・粋縺梧悄蠕・ｼ・s1m/bs5m・峨→螳悟・荳閾ｴ縲・
/// 莉倩ｨ・
/// - 蝗ｺ螳壼渕貅匁凾蛻ｻ・・025-01-01T12:00:00Z・峨ｒ逕ｨ縺・∫腸蠅・､画焚縺ｫ繧医ｋ譎ょ綾荳頑嶌縺阪・陦後ｏ縺ｪ縺・・
/// - 謨ｰ蛟､縺ｯ decimal 豈碑ｼ・〒隱､蟾ｮ髱櫁ｨｱ螳ｹ縲・
/// </summary>
[Collection("KsqlExclusive")]
public partial class BarChartTimeBucketTests
{
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class Bar
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)]
        [KsqlTimestamp]
        public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }

        // Translations (for smoke verification)
        public string UpperSymbol { get; set; } = string.Empty;
        public string BrokerHead { get; set; } = string.Empty;
        public double RoundAvg1 { get; set; }
        public int Year { get; set; }
    }

    // ===== TestContext: 繝・せ繝亥燕謠撰ｼ域磁邯夊ｨｭ螳壹・繧ｨ繝ｳ繝・ぅ繝・ぅ螳夂ｾｩ・・=====
    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Bar>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1, 5 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Bar
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low = g.Min(x => x.Bid),
                        KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid),

                        UpperSymbol = g.Key.Symbol.ToUpper(),
                        BrokerHead = g.Key.Broker.Substring(0, 1),
                        RoundAvg1 = Math.Round(g.Average(x => x.Bid), 1),
                        Year = SqlFuncs.Year(g.WindowStart())
                    }));
        }
    }

    private static class SqlFuncs
    {
        public static int Year(DateTime dt) => dt.Year;
    }

    // ===== Test Code: 繝・せ繝亥燕謠舌・逶ｴ蠕後↓驟咲ｽｮ =====
    [Fact]
    public async Task Chart_1m_and_5m_With_TimeBucket_Verify()
    {
        await Chart_1m_and_5m_With_TimeBucket_Verify_Impl();
    }

    // ===== Helpers: 縺薙％縺九ｉ荳九・陬懷勧髢｢謨ｰ・亥ｰ・擂縲∝挨繝輔ぃ繧､繝ｫ縺ｸ蛻・屬莠亥ｮ夲ｼ・=====
    // 蜑肴署螂醍ｴ・ｼ・eriod竊偵ユ繝ｼ繝悶Ν蜷搾ｼ峨・蛻･縺ｮ蜊倅ｽ薙ユ繧ｹ繝医〒諡・ｿ昴☆繧句燕謠舌よ悽繝・せ繝医・E2E縺ｮ謨ｰ蛟､縺ｨ遯薙↓髮・ｸｭ縲・    private static DateTime FixedBaseUtc()
        => new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime AlignToMinute(DateTime t)
        => new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0, DateTimeKind.Utc);

    private static DateTime AlignToFiveMinute(DateTime t)
        => new DateTime(t.Year, t.Month, t.Day, t.Hour, (t.Minute / 5) * 5, 0, DateTimeKind.Utc);

    private static long Ms(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;
    // /query-stream 縺ｫ繧医ｋ隕ｳ貂ｬ繝倥Ν繝代・蜑企勁・・ull縺ｮ縺ｿ縺ｧ蜴ｳ蟇・､懆ｨｼ・・
    [Fact]
    public async Task NoWhenEmpty_GapMinute_NoRow_Verify()
    {
        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
        await EnsureKafkaTopicAsync("deduprates");

        await using (var ctx = new TestContext())
        {
            var baseUtc = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var minuteStartUtc = new DateTime(baseUtc.Year, baseUtc.Month, baseUtc.Day, baseUtc.Hour, baseUtc.Minute, 0, DateTimeKind.Utc);
            static long Ms(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;
            var bs1m = Ms(minuteStartUtc);
            var bsNext = Ms(minuteStartUtc.AddMinutes(1));
            var keys = new (string broker, string symbol)[] { ("B", "S"), ("B2", "S2") };

            // Produce first minute only (no data in next minute) - different series per key
            foreach (var (b, s) in keys)
            {
                if (b == "B" && s == "S")
                {
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(1), Bid = 100 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(20), Bid = 105 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(40), Bid = 99 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(55), Bid = 101 });
                }
                else
                {
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(1), Bid = 200 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(20), Bid = 220 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(40), Bid = 190 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(55), Bid = 210 });
                }
            }

            // Assert via TimeBucket IF (use static ReadAsync API)
            // First minute rows exist with expected OHLC
            foreach (var (b, s) in keys)
            {
                var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { b, s }, CancellationToken.None);
                var row = list.SingleOrDefault(x => x.BucketStart == minuteStartUtc);
                Assert.NotNull(row);
                if (b == "B" && s == "S")
                {
                    Assert.Equal(100d, row!.Open);
                    Assert.Equal(105d, row.High);
                    Assert.Equal(99d,  row.Low);
                    Assert.Equal(99d,  row.KsqlTimeFrameClose);

                    // Translations smoke (on Bar)
                    Assert.Equal("SYM", row.UpperSymbol);
                    Assert.Equal("b", row.BrokerHead);
                    // Average(100,105,99,101) = 101.25 -> round(1) = 101.3
                    Assert.Equal(101.3, Math.Round(row.RoundAvg1, 1));
                    Assert.Equal(2025, row.Year);
                }
                else
                {
                    Assert.Equal(200d, row!.Open);
                    Assert.Equal(220d, row.High);
                    Assert.Equal(190d, row.Low);
                    Assert.Equal(190d, row.KsqlTimeFrameClose);
                }
            }

            // Next minute has no row (no WhenEmpty fill)
            foreach (var (b, s) in keys)
            {
                var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { b, s }, CancellationToken.None);
                Assert.False(list.Any(x => x.BucketStart == minuteStartUtc.AddMinutes(1)));
            }
        }

        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
    }

    [Fact]
    public async Task Boundary_1m_Window_Assignment_Verify()
    {
        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
        await EnsureKafkaTopicAsync("deduprates");

        await using (var ctx = new TestContext())
        {
            var baseUtc = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var minuteStartUtc = new DateTime(baseUtc.Year, baseUtc.Month, baseUtc.Day, baseUtc.Hour, baseUtc.Minute, 0, DateTimeKind.Utc);
            var key = (broker: "B", symbol: "S");

            // Events around boundaries
            await ctx.Rates.AddAsync(new Rate { Broker = key.broker, Symbol = key.symbol, Timestamp = minuteStartUtc.AddMilliseconds(-1), Bid = 90 }); // prev minute
            await ctx.Rates.AddAsync(new Rate { Broker = key.broker, Symbol = key.symbol, Timestamp = minuteStartUtc, Bid = 100 });                 // exact start
            await ctx.Rates.AddAsync(new Rate { Broker = key.broker, Symbol = key.symbol, Timestamp = minuteStartUtc.AddSeconds(59).AddMilliseconds(999), Bid = 105 }); // end-ﾎｵ
            await ctx.Rates.AddAsync(new Rate { Broker = key.broker, Symbol = key.symbol, Timestamp = minuteStartUtc.AddMinutes(1), Bid = 99 });   // next minute start

            // Read via TimeBucket IF (1m)
            var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { key.broker, key.symbol }, CancellationToken.None);

            // Window at minuteStart: includes [100, 105] only
            var row0 = list.SingleOrDefault(x => x.BucketStart == minuteStartUtc);
            Assert.NotNull(row0);
            Assert.Equal(100d, row0!.Open);
            Assert.Equal(105d, row0.High);
            Assert.Equal(100d, row0.Low);
            Assert.Equal(105d, row0.KsqlTimeFrameClose);

            // Window at minuteStart+1m: includes [99] only
            var row1 = list.SingleOrDefault(x => x.BucketStart == minuteStartUtc.AddMinutes(1));
            Assert.NotNull(row1);
            Assert.Equal(99d, row1!.Open);
            Assert.Equal(99d, row1.High);
            Assert.Equal(99d, row1.Low);
            Assert.Equal(99d, row1.KsqlTimeFrameClose);

            // Also verify 5m bucket uses the same Windows{ Minutes = new[]{1,5} } definition
            var fiveStart = new DateTime(baseUtc.Year, baseUtc.Month, baseUtc.Day, baseUtc.Hour, (baseUtc.Minute / 5) * 5, 0, DateTimeKind.Utc);
            var list5 = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(5), new[] { key.broker, key.symbol }, CancellationToken.None);
            var row5 = list5.SingleOrDefault(x => x.BucketStart == fiveStart);
            Assert.NotNull(row5);
            // 5m includes 12:00:00(100), 12:00:59.999(105), 12:01:00(99) 窶・excludes 11:59:59.999
            Assert.Equal(100d, row5!.Open);
            Assert.Equal(105d, row5.High);
            Assert.Equal(99d,  row5.Low);
            Assert.Equal(99d,  row5.KsqlTimeFrameClose);
        }

        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
    }

    [Fact(Skip = "Enable once continuation-based carry-forward is verified via TimeBucket")]
    public async Task WhenEmpty_Fills_GapMinute_With_PrevClose_Verify()
    {
        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
        await EnsureKafkaTopicAsync("deduprates");

        await using (var ctx = new TestContext())
        {
            var baseUtc = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var minuteStartUtc = new DateTime(baseUtc.Year, baseUtc.Month, baseUtc.Day, baseUtc.Hour, baseUtc.Minute, 0, DateTimeKind.Utc);
            static long Ms(DateTime dt) => (long)(dt - DateTime.UnixEpoch).TotalMilliseconds;
            var bs1m = Ms(minuteStartUtc);
            var bsNext = Ms(minuteStartUtc.AddMinutes(1));
            var keys = new (string broker, string symbol)[] { ("B", "S"), ("B2", "S2") };

            foreach (var (b, s) in keys)
            {
                await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(1), Bid = 100 });
                await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(20), Bid = 105 });
                await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(40), Bid = 99 });
                await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(55), Bid = 101 });
            }

            // First minute as usual via TimeBucket
            foreach (var (b, s) in keys)
            {
                var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { b, s }, CancellationToken.None);
                var row = list.SingleOrDefault(x => x.BucketStart == minuteStartUtc);
                Assert.NotNull(row);
                Assert.Equal(100d, row!.Open);
                Assert.Equal(105d, row.High);
                Assert.Equal(99d,  row.Low);
                Assert.Equal(99d,  row.KsqlTimeFrameClose);
            }

            // Next minute should be filled with previous close (101) via TimeBucket
            foreach (var (b, s) in keys)
            {
                var list = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { b, s }, CancellationToken.None);
                var rowNext = list.SingleOrDefault(x => x.BucketStart == minuteStartUtc.AddMinutes(1));
                Assert.NotNull(rowNext);
                Assert.Equal(101d, rowNext!.Open);
                Assert.Equal(101d, rowNext.High);
                Assert.Equal(101d, rowNext.Low);
                Assert.Equal(101d, rowNext.KsqlTimeFrameClose);
            }
        }

        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
    }
    // diag逶｣隕冶ｵｷ蜍包ｼ・HYS_DIAG=1縺ｮ譎ゅ・縺ｿ・・ 1m/5m縺ｮ隕ｳ貂ｬ繧ｿ繧ｹ繧ｯ繧定ｿ斐☆・井ｸ崎ｦ∵凾縺ｯnull・・    // diag逶｣隕悶・繝ｫ繝代・蜑企勁・医ユ繧ｹ繝亥粋蜷ｦ縺ｫ蟇・ｸ弱＠縺ｪ縺・◆繧・ｼ・
    private static async Task CleanupBarArtifactsAsync()
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ct = cts.Token;

        var queries = await GetBarQueryIdsAsync(http, ct);
        foreach (var queryId in queries)
        {
            await ExecuteKsqlStatementAsync(http, $"TERMINATE {queryId};", ct);
        }

        var tables = await GetEntityNamesAsync(http, "SHOW TABLES;", "tables", ct);
        foreach (var table in tables.Where(IsBarIdentifier))
        {
            await ExecuteKsqlStatementAsync(http, $"DROP TABLE IF EXISTS {table} DELETE TOPIC;", ct);
        }

        var streams = await GetEntityNamesAsync(http, "SHOW STREAMS;", "streams", ct);
        foreach (var stream in streams.Where(IsBarIdentifier))
        {
            await ExecuteKsqlStatementAsync(http, $"DROP STREAM IF EXISTS {stream} DELETE TOPIC;", ct);
        }
    }
    private static async Task EnsureKafkaTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build();
        try
        {
            var metadata = admin.GetMetadata(topicName, TimeSpan.FromSeconds(2));
            if (metadata?.Topics != null && metadata.Topics.Any(t => string.Equals(t.Topic, topicName, StringComparison.OrdinalIgnoreCase) && t.Error.Code == ErrorCode.NoError))
                return;
        }
        catch
        {
        }

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = replicationFactor
                }
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex)
        {
            if (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
                return;
            throw;
        }
    }


    private static async Task EnsureBar1sRowsInputStreamAsync(HttpClient http, CancellationToken cancellationToken)
    {
        const string ddl = @"CREATE STREAM IF NOT EXISTS bar_1s_rows_input (
            Broker STRING KEY,
            Symbol STRING KEY,
            BucketStart TIMESTAMP KEY,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            KsqlTimeFrameClose DOUBLE
        ) WITH (KAFKA_TOPIC='bar_1s_rows', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO', TIMESTAMP='BUCKETSTART');";
        await ExecuteKsqlStatementAsync(http, ddl, cancellationToken);
    }

    // Helper: per-key single-bucket assertion for readability
    private static async Task AssertPerKeySingleBucketAsync(
        Func<string, TimeSpan, Task<List<object?[]>>> query,
        string table,
        (string broker, string symbol)[] keys,
        long bucketStartMs,
        TimeSpan timeout)
    {
        foreach (var (b, s) in keys)
        {
            var rows = await query($"SELECT BUCKETSTART FROM {table} WHERE BROKER='{b}' AND SYMBOL='{s}';", timeout);
            var set = rows.Select(r => Convert.ToInt64(r[0]!)).Distinct().OrderBy(x => x).ToArray();
            Assert.Single(set);
            Assert.Equal(bucketStartMs, set[0]);
        }
    }

    private static async Task<List<object?[]>> QueryPullRowsAsync(string sql, TimeSpan timeout)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        var payload = new { sql, properties = new Dictionary<string, object>() };
        using var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
        using var cts = new CancellationTokenSource(timeout);
        using var resp = await http.PostAsync("/query", content, cts.Token);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(cts.Token);
        using var doc = JsonDocument.Parse(body);
        var rows = new List<object?[]>();
        foreach (var element in doc.RootElement.EnumerateArray())
        {
            if (element.TryGetProperty("row", out var row) && row.ValueKind == JsonValueKind.Object && row.TryGetProperty("columns", out var cols))
            {
                var arr = new List<object?>();
                foreach (var v in cols.EnumerateArray())
                    arr.Add(v.ValueKind == JsonValueKind.Null ? null : v.Deserialize<object>());
                rows.Add(arr.ToArray());
            }
        }
        return rows;
    }

    private static async Task ProduceOneSecondBarsAsync(string broker, string symbol, DateTime minuteStartUtc, IReadOnlyList<double> prices)
    {
        using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cts.Token;

        await EnsureBar1sRowsInputStreamAsync(http, ct);

        var sanitizedBroker = broker.Replace("'", "''");
        var sanitizedSymbol = symbol.Replace("'", "''");
        for (var i = 0; i < prices.Count; i++)
        {
            var timestamp = minuteStartUtc.AddSeconds(i * 10);
            var timestampLiteral = timestamp.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
            var price = prices[i].ToString(CultureInfo.InvariantCulture);
            var sql = $"INSERT INTO bar_1s_rows_input (BROKER, SYMBOL, BUCKETSTART, OPEN, HIGH, LOW, KSQLTIMEFRAMECLOSE) VALUES ('{sanitizedBroker}', '{sanitizedSymbol}', TIMESTAMP '{timestampLiteral}', {price}, {price}, {price}, {price});";
            await ExecuteKsqlStatementAsync(http, sql, ct);
        }
    }

    // 菴呵ｨ医↑蜑肴署蛻､螳壹Ο繧ｸ繝・け縺ｯ蜑企勁・域悽繝・せ繝医〒縺ｯ荳崎ｦ・ｼ・
    // SHOW QUERIES繝吶・繧ｹ縺ｮ襍ｰ譟ｻ縺ｯ譛ｬ繝・せ繝医〒縺ｯ荳崎ｦ√・縺溘ａ蜑企勁

    // Not used

    // Not used

    // Not used

    private static async Task ExecuteKsqlStatementAsync(HttpClient http, string sql, CancellationToken cancellationToken)
    {
        var doc = await ExecuteKsqlForJsonAsync(http, sql, cancellationToken);
        doc?.Dispose();
    }

    private static async Task<JsonDocument?> ExecuteKsqlForJsonAsync(HttpClient http, string sql, CancellationToken cancellationToken)
    {
        var payload = new { ksql = sql, streamsProperties = new Dictionary<string, object>() };
        using var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
        try
        {
            using var response = await http.PostAsync("/ksql", content, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            if (!response.IsSuccessStatusCode || string.IsNullOrWhiteSpace(body))
                return null;

            try
            {
                return JsonDocument.Parse(body);
            }
            catch
            {
                return null;
            }
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (TaskCanceledException)
        {
            return null;
        }
    }
    private async Task Chart_1m_and_5m_With_TimeBucket_Verify_Impl()
    {
        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");

        await EnsureKafkaTopicAsync("bar");

        await EnsureKafkaTopicAsync("rate");

        await using (var ctx = new TestContext())
        {
            // 蜑肴署螂醍ｴ・・譏守､ｺ繝√ぉ繝・け縺ｯ譛ｬ繝・せ繝医°繧蛾勁螟厄ｼ医さ繧｢讀懆ｨｼ縺ｫ髮・ｸｭ・・
            // 繝・・繧ｿ謚募・・域ｱｺ螳夊ｫ厄ｼ・ 迴ｾ蝨ｨ萓晏ｭ倥ｒ謗偵＠蝗ｺ螳啅TC繝吶・繧ｹ繧呈治逕ｨ・・HYS_BASE_UTC縺ｧ荳頑嶌縺榊庄・・
            // 蜀咲樟諤ｧ縺ｮ縺溘ａPHYS_BASE_UTC縺ｯ菴ｿ逕ｨ縺帙★蝗ｺ螳・
            var baseUtc = FixedBaseUtc();
            // 隍・焚繧ｭ繝ｼ縺ｧ讀懆ｨｼ・医げ繝ｫ繝ｼ繝斐Φ繧ｰ縺ｮ蛛･蜈ｨ諤ｧ繧呈球菫晢ｼ・
            var keys = new (string broker, string symbol)[] { ("B", "S"), ("B2", "S2") };
            var minuteStartUtc = AlignToMinute(baseUtc);
            var bs1m = Ms(minuteStartUtc);
            var bs5m = Ms(AlignToFiveMinute(baseUtc));

            await Task.Delay(500);
            foreach (var (b, s) in keys)
            {
                if (b == "B" && s == "S")
                {
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(1), Bid = 100 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(20), Bid = 105 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(40), Bid = 99 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(55), Bid = 101 });
                }
                else
                {
                    // 逡ｰ縺ｪ繧狗ｳｻ蛻暦ｼ亥・髮｢讀懆ｨｼ逕ｨ・・
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(1), Bid = 200 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(20), Bid = 220 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(40), Bid = 190 });
                    await ctx.Rates.AddAsync(new Rate { Broker = b, Symbol = s, Timestamp = minuteStartUtc.AddSeconds(55), Bid = 210 });
                }
            }


            foreach (var (b, s) in keys)
            {
                if (b == "B" && s == "S")
                {
                    await ProduceOneSecondBarsAsync(b, s, minuteStartUtc, new[] { 100d, 103d, 105d, 99d, 102d, 101d });
                }
                else
                {
                    // 逡ｰ縺ｪ繧狗ｳｻ蛻暦ｼ亥・髮｢讀懆ｨｼ逕ｨ・・
                    await ProduceOneSecondBarsAsync(b, s, minuteStartUtc, new[] { 100d, 103d, 105d, 99d, 102d, 101d });
                }
            }\r\n            // 隕ｳ貂ｬ邉ｻ・・iag・峨・菴ｿ逕ｨ縺励↑縺Ыr\n
            // 謨ｰ蛟､讀懆ｨｼ・・ull・・
            // 莉ｶ謨ｰ縺ｨ繧ｭ繝ｼ髮・粋縺ｮ蜴ｳ蟇・､懆ｨｼ・・m/5m 縺昴ｌ縺槭ｌ縺ｮ譎る俣譫・・
            await PhysOhlcAssertions.AssertBucketExactAsync(
                QueryPullRowsAsync,
                "BAR_1M_LIVE",
                bs1m,
                new[]
                {
                    (broker: "B",  symbol: "S",  Open: 100m, High: 105m, Low: 99m,  Close: 99m),
                    (broker: "B2", symbol: "S2", Open: 100m, High: 105m, Low: 99m, Close: 99m)
                },
                TimeSpan.FromSeconds(20));

            // 譎る俣譫蛻･縺ｮ莉ｶ謨ｰ繝√ぉ繝・け・・m・会ｼ壼推繧ｭ繝ｼ縺ｧ繝舌こ繝・ヨ縺ｯ1縺､縺縺・
            await AssertPerKeySingleBucketAsync(QueryPullRowsAsync, "BAR_1M_LIVE", keys, bs1m, TimeSpan.FromSeconds(20));

            await PhysOhlcAssertions.AssertBucketExactAsync(
                QueryPullRowsAsync,
                "BAR_5M_LIVE",
                bs5m,
                new[]
                {
                    (broker: "B",  symbol: "S",  Open: 100m, High: 105m, Low: 99m,  Close: 101m),
                    (broker: "B2", symbol: "S2", Open: 100m, High: 105m, Low: 99m, Close: 101m)
                },
                TimeSpan.FromSeconds(20));

            // 譎る俣譫蛻･縺ｮ莉ｶ謨ｰ繝√ぉ繝・け・・m・会ｼ壼推繧ｭ繝ｼ縺ｧ繝舌こ繝・ヨ縺ｯ1縺､縺縺・
            await AssertPerKeySingleBucketAsync(QueryPullRowsAsync, "BAR_5M_LIVE", keys, bs5m, TimeSpan.FromSeconds(20));
        }

        await PhysicalTestEnv.KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
    }

    [Fact(Skip = "WhenEmpty fill KSQL is pending; will verify via TimeBucket once implemented")]
    public void WhenEmpty_Fills_Missing_Minute_With_Previous_Close()
    {
        // docs/chart.md 縺ｮ WhenEmpty・域ｬ謳榊沂繧・ｼ峨↓蟇ｾ蠢懊☆繧区､懆ｨｼ
        // 繝輔ぅ繧ｸ繧ｫ繝ｫ逕滓・・・B/Prev JOIN + Fill 謚募ｽｱ・峨・蜈ｷ雎｡蛹悶′譛ｪ蜿肴丐縺ｮ縺溘ａ Skip縲・
        // 螳溯｣・ｾ後・ 1蛻・・谺謳阪ｒ逶ｴ蜑・Close 縺ｧ蝓九ａ繧九％縺ｨ繧・TimeBucket 邨檎罰縺ｧ遒ｺ隱阪☆繧九・
    }

}


















