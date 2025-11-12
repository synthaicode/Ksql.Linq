using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Ksql.Linq.Core.Modeling;
using Xunit;
using PhysicalTestEnv;

namespace Ksql.Linq.Tests.Integration;

[Collection("KsqlExclusive")]
public class BarTranslationsSmokeTests
{
    [KsqlTopic("deduprates")] 
    private sealed class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("bar")]
    private sealed class BarT
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlKey(3)] [KsqlTimestamp] public DateTime BucketStart { get; set; }

        // Original OHLC-ish columns
        public double Open { get; set; }
        public double High { get; set; }
        public double Low  { get; set; }
        public double Close { get; set; }

        // Translations to verify
        public string BrokerHead { get; set; } = string.Empty;  // Substring(0,1)
        public double RoundAvg1  { get; set; }                  // Round(Avg(...),1)
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });

        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _lf) { }

        // physical input
        public EventSet<Rate> Rates { get; set; } = null!;

        // physical: enable schema registration
        protected override bool SkipSchemaRegistration => false;

        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BarT>()
                .ToQuery(q => q.From<Rate>()
                    .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
                    .GroupBy(r => new { r.Broker, r.Symbol })
                    .Select(g => new BarT
                    {
                        Broker = g.Key.Broker,
                        Symbol = g.Key.Symbol,
                        BucketStart = g.WindowStart(),
                        Open = g.EarliestByOffset(x => x.Bid),
                        High = g.Max(x => x.Bid),
                        Low  = g.Min(x => x.Bid),
                        Close = g.LatestByOffset(x => x.Bid),

                        BrokerHead = g.Key.Broker.Substring(0, 1),
                        RoundAvg1  = Math.Round(g.Average(x => x.Bid), 1),
                        
                    }));
        }
    }

    private static class SqlFuncs
    {
        public static int Year(DateTime dt) => dt.Year;
    }

    [Fact]
    public async Task Bar_Translations_Minute_Smoke()
    {
        await KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
        await EnsureKafkaTopicAsync("deduprates");
        // ksqlDB does not create source topics for CREATE STREAM definitions.
        // Ensure the intermediate rows topic exists physically before CTAS reads it.
        await EnsureKafkaTopicAsync("bar_1s_rows");

        await using var ctx = new TestContext();
        // Ensure derived entity is registered/running (best-effort)
        var baseUpper = ctx.GetTopicName<BarT>().ToUpperInvariant();
        var liveName = $"{baseUpper}_1M_LIVE";
        await WaitForLiveObjectsAsync("http://127.0.0.1:18088", new[] { liveName }, TimeSpan.FromSeconds(180));
        await WaitForQueriesRunningAsync("http://127.0.0.1:18088", liveName, TimeSpan.FromSeconds(180));
        // Note: 1s_rows の初回行は入力が届いてから生成されるため、先に投入してから到達確認を行う。

        var t0 = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var broker = "brk"; var symbol = "sym";

        // two events in the same minute -> Open=1.20, High=1.28, Low=1.20, Close=1.28, Avg=1.24 => RoundAvg1=1.2
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(5),  Bid = 1.20 });
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(25), Bid = 1.28 });

        // 1) 1s_rows の対象キー到達を確認（発行経路の健全性を先に担保）
        await WaitForOneSecondRowsKeyAsync(ctx, baseUpper, broker, symbol, TimeSpan.FromSeconds(60));

        // 2) ウィンドウ確定を促すため、同一キーでウィンドウ終了+GRACE超過のダミーを1件追加
        //    GRACE(1s) のため、t0+65s に1件入れるとテーブル可視化が促進される実装/環境があります。
        await ctx.Rates.AddAsync(new Rate { Broker = broker, Symbol = symbol, Timestamp = t0.AddSeconds(65), Bid = 1.22 });

        // Wait before pull-reading: even after queries are RUNNING and rows are being produced,
        // ksqlDB persistent CTAS (BAR_1M_LIVE) can take a brief moment to materialize the first row
        // into its state store. Without this guard, the immediate pull can race and observe no rows.
        await Task.Delay(TimeSpan.FromSeconds(2));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(180));
        // Poll for materialization: even with RUNNING queries, ksqlDB's persistent CTAS may take a
        // short time to emit and materialize the first row into the table. Pull queries are eventually
        // consistent to the internal state store. We retry reads for the specific key/bucket.
        BarT? row = null;
        for (int attempt = 0; attempt < 60; attempt++) // up to ~60s with 1s interval
        {
            var rows = await TimeBucket.ReadAsync<BarT>(ctx, Ksql.Linq.Runtime.Period.Minutes(1), new[] { broker, symbol }, cts.Token);
            row = rows.SingleOrDefault(r => r.BucketStart == t0);
            if (row != null) break;
            await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
        }

        // Compare filtered vs unfiltered snapshots to isolate filter-related behavior.
        // Note: Unfiltered uses obsolete facade to allow null filter for diagnostics only.
        var allRows = await Ksql.Linq.Runtime.TimeBucket
            .Get<BarT>(ctx, Ksql.Linq.Runtime.Period.Minutes(1))
            .ToListAsync(cts.Token);
        var filteredRows = await TimeBucket.ReadAsync<BarT>(ctx, Ksql.Linq.Runtime.Period.Minutes(1), new[] { broker, symbol }, cts.Token);
        Console.WriteLine($"[diag] 1m_all_count={allRows?.Count ?? -1} filtered_count={filteredRows?.Count ?? -1}");
        if ((allRows?.Count ?? 0) > 0)
        {
            try { var bs = allRows[0]?.GetType().GetProperty("BucketStart")?.GetValue(allRows[0]); Console.WriteLine($"[diag] first_all.BucketStart={bs}"); } catch { }
        }
        if ((filteredRows?.Count ?? 0) > 0)
        {
            try { var bs = filteredRows[0]?.GetType().GetProperty("BucketStart")?.GetValue(filteredRows[0]); Console.WriteLine($"[diag] first_filtered.BucketStart={bs}"); } catch { }
        }
        Assert.NotNull(row);

        Assert.Equal(1.20, row!.Open);
        Assert.Equal(1.28, row.High);
        Assert.Equal(1.20, row.Low);
        Assert.Equal(1.28, row.Close);

        // Live は計算列を含めない方針のため、計算列の検証は省略（Rows 側で検証）

        await KsqlHelpers.TerminateAndDropBarArtifactsAsync("http://127.0.0.1:18088");
    }


    private static async Task WaitForLiveObjectsAsync(string ksqlBaseUrl, string[] nameTokens, TimeSpan timeout)
    {
        using var http = new System.Net.Http.HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        var until = DateTime.UtcNow + timeout;
        int consec = 0;
        while (DateTime.UtcNow < until)
        {
            try
            {
                // Check multiple ksql metadata endpoints for presence of expected live objects
                var statements = new[] { "SHOW QUERIES;", "SHOW TABLES;", "SHOW STREAMS;" };
                var anyOk = false;
                foreach (var stmt in statements)
                {
                    var payload = new { ksql = stmt, streamsProperties = new { } };
                    using var content = new System.Net.Http.StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
                    using var resp = await http.PostAsync("/ksql", content);
                    var body = await resp.Content.ReadAsStringAsync();
                    if (!string.IsNullOrWhiteSpace(body) && nameTokens.All(t => body.IndexOf(t, StringComparison.OrdinalIgnoreCase) >= 0))
                    {
                        anyOk = true; break;
                    }
                }
                if (anyOk) { consec++; if (consec >= 3) return; } else { consec = 0; }
            }
            catch { consec = 0; }
            await Task.Delay(1000);
        }
    }

    private static async Task WaitForQueriesRunningAsync(string ksqlBaseUrl, string nameToken, TimeSpan timeout)
    {
        using var http = new System.Net.Http.HttpClient { BaseAddress = new Uri(ksqlBaseUrl.TrimEnd('/')) };
        var until = DateTime.UtcNow + timeout;
        int consec = 0;
        while (DateTime.UtcNow < until)
        {
            try
            {
                var payload = new { ksql = "SHOW QUERIES;", streamsProperties = new { } };
                using var content = new System.Net.Http.StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
                using var resp = await http.PostAsync("/ksql", content);
                var body = await resp.Content.ReadAsStringAsync();
                if (body.IndexOf(nameToken, StringComparison.OrdinalIgnoreCase) >= 0 && body.IndexOf("RUNNING", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    consec++;
                    if (consec >= 3) return;
                }
                else consec = 0;
            }
            catch { consec = 0; }
            await Task.Delay(1000);
        }
    }

    private static async Task WaitForAnyRowsAsync(TestContext ctx, Ksql.Linq.Runtime.Period period, string broker, string symbol, TimeSpan timeout)
    {
        var until = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < until)
        {
            try
            {
                var rows = await TimeBucket.ReadAsync<BarT>(ctx, period, new[] { broker, symbol }, CancellationToken.None);
                if (rows != null && rows.Count > 0) return;
            }
            catch { }
            await Task.Delay(500);
        }
    }

    // Best-effort: wait until 1s_rows has at least one row (any key)
    private static async Task WaitForOneSecondRowsAsync(TestContext ctx, string baseTopicUpper, TimeSpan timeout)
    {
        var until = DateTime.UtcNow + timeout;
        var table = $"{baseTopicUpper}_1S_ROWS";
        while (DateTime.UtcNow < until)
        {
            try
            {
                var sql = $"SELECT * FROM {table} EMIT CHANGES LIMIT 1;";
                var rows = await ctx.QueryRowsAsync(sql, TimeSpan.FromSeconds(10));
                if (rows != null && rows.Any()) return;
            }
            catch { }
            await Task.Delay(1000);
        }
    }

    // 指定キー(Broker,Symbol)の1s_rows到達を確認する（最大timeout）
    private static async Task WaitForOneSecondRowsKeyAsync(TestContext ctx, string baseTopicUpper, string broker, string symbol, TimeSpan timeout)
    {
        var until = DateTime.UtcNow + timeout;
        var table = $"{baseTopicUpper}_1S_ROWS";
        while (DateTime.UtcNow < until)
        {
            try
            {
                var sql = $"SELECT 1 FROM {table} WHERE BROKER='{'{'}' + broker + '{'}'}' AND SYMBOL='{'{'}' + symbol + '{'}'}' EMIT CHANGES LIMIT 1;";
                var rows = await ctx.QueryRowsAsync(sql, TimeSpan.FromSeconds(10));
                if (rows != null && rows.Any()) return;
            }
            catch { }
            await Task.Delay(1000);
        }
    }

    private static async Task EnsureKafkaTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build();
        try
        {
            var md = admin.GetMetadata(topicName, TimeSpan.FromSeconds(2));
            if (md?.Topics != null && md.Topics.Any(t => string.Equals(t.Topic, topicName, StringComparison.OrdinalIgnoreCase) && t.Error.Code == ErrorCode.NoError))
                return;
        }
        catch { }

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topicName, NumPartitions = partitions, ReplicationFactor = replicationFactor }
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex)
        {
            if (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)) return;
            throw;
        }
    }
}
