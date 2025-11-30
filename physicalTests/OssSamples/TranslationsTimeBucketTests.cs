using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Sdk;
using PhysicalTestEnv.Logging;

namespace Ksql.Linq.Tests.Integration;

/// <summary>
/// Translations（LINQ → KSQL）最小カノニカルの物理テスト。
/// 目的: 代表関数が正しく翻訳・集約され、TimeBucket 経由で決定論的に取得できることを確認。
/// ポリシー: 固定基準時刻、1分窓、1枠=1行、decimal/double 即値比較。
/// 依存: 観測（/query-stream）に依存せず、TimeBucket.ReadAsync で検証。
/// </summary>
[Collection("KsqlExclusive")]
public class TranslationsTimeBucketTests
{
    [KsqlTopic("deduprates")]
    private sealed class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        // For json/url/coords tests
        public string Json { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
        public double Lat1 { get; set; }
        public double Lon1 { get; set; }
        public double Lat2 { get; set; }
        public double Lon2 { get; set; }
    }

    private sealed class Xform
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)]
        public DateTime BucketStart { get; set; }

        public long? WindowStartRaw { get; set; }
        public string UpperSymbol { get; set; } = string.Empty;    // ToUpper
        public string BrokerHead { get; set; } = string.Empty;      // Substring(0,1)
        public double? AvgRounded { get; set; }                      // Round(Average(...), 1)
        public int? Year { get; set; }                               // YEAR(BucketStart)

        // String
        public string LowerSymbol { get; set; } = string.Empty;
        public int? NameLen { get; set; }
        // duplicate removed

        // Math
        public double? AbsBid { get; set; }
        public double? FloorBid { get; set; }
        public double? CeilBid { get; set; }
        public double? RoundAvg1 { get; set; }

        // Date parts
        public int? Month { get; set; }
        public int? Day { get; set; }
        public int? Hour { get; set; }
        public int? Minute { get; set; }
        public int? Second { get; set; }

        // Aggregates
        public double? SumBid { get; set; }
        public double? MaxBid { get; set; }
        public double? MinBid { get; set; }
        public long? Cnt { get; set; }
        public double? FirstBid { get; set; }
        public double? LastBid { get; set; }

        // reserved for future: array/json/url (syntax tests)

        // Cast / Conditional
        public string BidStr { get; set; } = string.Empty;
        public int? BidInt { get; set; }
        public int? ConvInt { get; set; }
        public int? CaseFlag { get; set; }
    }

    private static class SqlFuncs
    {
        // Expression用のダミー。実際はKsqlFunctionRegistryの Year → YEAR へ翻訳される。
        public static int Year(DateTime dt) => dt.Year;
        public static int Month(DateTime dt) => dt.Month;
        public static int Day(DateTime dt) => dt.Day;
        public static int Hour(DateTime dt) => dt.Hour;
        public static int Minute(DateTime dt) => dt.Minute;
        public static int Second(DateTime dt) => dt.Second;

        // (reserved for future mappings not required in this canonical test)
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory =
            PhysicalTestLog.CreateFactory(
                nameof(TranslationsTimeBucketTests),
                LogLevel.Debug,
                builder => builder.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug));

        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }

        // 物理テストではスキーマ登録を有効化
        protected override bool SkipSchemaRegistration => false;

        public EventSet<Rate> Rates { get; set; } = null!;

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Xform>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new Xform
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        WindowStartRaw = new DateTimeOffset(g.WindowStart()).ToUnixTimeMilliseconds(),

                        UpperSymbol = g.Key.Symbol.ToUpper(),
                        BrokerHead = g.Key.Broker.Substring(0, 1),
                        AvgRounded = Math.Round(g.Average(x => x.Bid), 1),
                        Year = SqlFuncs.Year(g.WindowStart()),

                        // String
                        LowerSymbol = g.Key.Symbol.ToLower(),
                        NameLen = g.Key.Symbol.Length,

                        // Math
                        AbsBid = Math.Abs(g.Max(x => x.Bid)),
                        FloorBid = Math.Floor(g.Max(x => x.Bid)),
                        CeilBid = Math.Ceiling(g.Min(x => x.Bid)),
                        RoundAvg1 = Math.Round(g.Average(x => x.Bid), 1),

                        // Date parts
                        Month = SqlFuncs.Month(g.WindowStart()),
                        Day = SqlFuncs.Day(g.WindowStart()),
                        Hour = SqlFuncs.Hour(g.WindowStart()),
                        Minute = SqlFuncs.Minute(g.WindowStart()),
                        Second = SqlFuncs.Second(g.WindowStart()),

                        // Aggregates
                        SumBid = g.Sum(x => x.Bid),
                        MaxBid = g.Max(x => x.Bid),
                        MinBid = g.Min(x => x.Bid),
                        Cnt = g.Count(),
                        FirstBid = g.EarliestByOffset(x => x.Bid),
                        LastBid = g.LatestByOffset(x => x.Bid),

                        // reserved for future: array/json/url

                        // Cast / Conditional
                        BidStr = (g.Max(x => x.Bid)).ToString(),
                        BidInt = (int)g.Min(x => x.Bid),
                        ConvInt = Convert.ToInt32(g.Max(x => x.Bid)),
                        CaseFlag = g.Sum(x => x.Bid) > 0 ? 1 : 0
                    }));
        }
    }

    [Fact]
    public async Task Translations_Minimal_Canonical()
    {
        // 前後でksqlDBをクリーン（依存アーティファクトを極力残さない）
        await PhysicalTestEnv.KsqlHelpers.TerminateAllAsync("http://127.0.0.1:18088");

        await using var ctx = new TestContext();

        // Ensure input topic exists (auto-creation disabled in env)
        await EnsureKafkaTopicAsync("deduprates");

        var baseUtc = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var t0 = new DateTime(baseUtc.Year, baseUtc.Month, baseUtc.Day, baseUtc.Hour, baseUtc.Minute, 0, DateTimeKind.Utc);

        var broker = "brk";
        var symbol = "sym"; // 小文字 → ToUpper で SYM へ

        // 同一1分枠内のイベント（Average=1.24 → Round(1) = 1.2）
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(5), Bid = 1.20, Json = "{\"a\":\"X\",\"list\":[1,2]}", Url = "https://example.com/p?q=1", Lat1 = 0, Lon1 = 0, Lat2 = 0, Lon2 = 0 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(25), Bid = 1.28, Json = "{\"a\":\"X\",\"list\":[1,2]}", Url = "https://example.com/p?q=1", Lat1 = 0, Lon1 = 0, Lat2 = 0, Lon2 = 0 });

        // TimeBucket で1枠=1行を取得
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        var allRows = await Ksql.Linq.Runtime.TimeBucket
            .Get<Xform>(ctx, Ksql.Linq.Runtime.Period.Minutes(1))
            .ToListAsync(null, cts.Token);
        Assert.NotEmpty(allRows);

        var rows = await Ksql.Linq.TimeBucket.ReadAsync<Xform>(ctx, Ksql.Linq.Runtime.Period.Minutes(1), new[] { broker, symbol }, t0, tolerance: TimeSpan.FromSeconds(1), ct: cts.Token);
        var targetMs = new DateTimeOffset(t0).ToUnixTimeMilliseconds();
        var row = rows.SingleOrDefault(x => Math.Abs((x.BucketStart - t0).TotalSeconds) <= 1);
        if (row == null)
        {
            var dump = string.Join(" | ", rows.Select(r => $"{r.WindowStartRaw}:{r.BucketStart:o}"));
            throw new XunitException($"TimeBucket rows did not contain target bucket. count={rows.Count} targetMs={targetMs} rows=[{dump}]");
        }

        var actualMs = row.WindowStartRaw ?? new DateTimeOffset(row.BucketStart).ToUnixTimeMilliseconds();
        Assert.Equal(targetMs, actualMs);
        Assert.Equal(t0, row.BucketStart);
        // Live は集計と時間のみ検証（計算列は Rows 側で検証）

        // Aggregates
        Assert.Equal(2.48, row.SumBid.GetValueOrDefault());
        Assert.Equal(1.28, row.MaxBid.GetValueOrDefault());
        Assert.Equal(1.20, row.MinBid.GetValueOrDefault());
        Assert.Equal(2, row.Cnt.GetValueOrDefault());
        Assert.Equal(1.20, row.FirstBid.GetValueOrDefault());
        Assert.Equal(1.28, row.LastBid.GetValueOrDefault());

        // reserved for future: array/json/url（計算列は Live で検証しない）

        await PhysicalTestEnv.KsqlHelpers.TerminateAllAsync("http://127.0.0.1:18088");
    }

    private static async Task EnsureKafkaTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build();
        try
        {
            var metadata = admin.GetMetadata(topicName, TimeSpan.FromSeconds(2));
            if (metadata?.Topics != null && metadata.Topics.Any(t => string.Equals(t.Topic, topicName, StringComparison.OrdinalIgnoreCase) && t.Error.Code == Confluent.Kafka.ErrorCode.NoError))
                return;
        }
        catch { }
        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new Confluent.Kafka.Admin.TopicSpecification { Name = topicName, NumPartitions = partitions, ReplicationFactor = replicationFactor }
            }).ConfigureAwait(false);
        }
        catch (Confluent.Kafka.Admin.CreateTopicsException ex)
        {
            if (ex.Results.All(r => r.Error.Code == Confluent.Kafka.ErrorCode.TopicAlreadyExists)) return;
            throw;
        }
    }
}


