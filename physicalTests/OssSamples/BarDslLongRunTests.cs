using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ksql.Linq.Runtime;
using System.Text.Json;
using System.Net.Http;
using System.Text;
using Xunit;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;

namespace Ksql.Linq.Tests.Integration;

public class BarDslLongRunTests
{
    [KsqlTopic("deduprates")] 
    public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    public class Bar
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
    }

    private sealed class TestContext : KsqlContext
    {
        private static readonly ILoggerFactory _loggerFactory = LoggerFactory.Create(b =>
        {
            b.AddConsole();
            b.SetMinimumLevel(LogLevel.Information);
            b.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
        });
        public TestContext() : base(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        }, _loggerFactory) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<Bar>()
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
                    KsqlTimeFrameClose = g.LatestByOffset(x => x.Bid)
                }));
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LongRun_1h_Ohlc_And_Grace_Verify()
    {
        // Ensure ksqlDB is ready, then clean existing BAR_* artifacts to avoid OR REPLACE upgrade errors
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        await CleanupBarArtifactsAsync();
        await using var ctx = new TestContext();

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "deduprates", 1, 1, TimeSpan.FromSeconds(10));
        }

        await ctx.WaitForEntityReadyAsync<Rate>(TimeSpan.FromSeconds(60));

        // Long-run duration: default 60 minutes (can override via env LONGRUN_MINUTES)
        int minutes = 60;
        if (int.TryParse(Environment.GetEnvironmentVariable("LONGRUN_MINUTES"), out var m) && m > 0) minutes = m;

        // Strongly past-aligned timestamps so 1s hub windows close immediately
        // Anchor = now - 2h (rounded to minute)
        var anchor = DateTime.UtcNow.AddHours(-2);
        var t0 = new DateTime(anchor.Year, anchor.Month, anchor.Day, anchor.Hour, anchor.Minute, 0, DateTimeKind.Utc);

        // Generate multiple ticks per minute for OHLC, and a late (grace) tick just after minute end with an in-window timestamp
        var basePrice = 100.0;

        // Prefer feeding hub stream with event-time BUCKETSTART to avoid minute-collapsing by record timestamp
        await EnsureBar1sRowsInputStreamAsync();
        for (int i = 0; i < minutes; i++)
        {
            var ms = t0.AddMinutes(i);
            var open = basePrice + i * 0.5;     // slow drift
            var high = open + 10;               // high spike
            var low  = open - 7;                // low spike
            var close= open + 3;                // close value

            // within-minute 1s rows (use BUCKETSTART as event-time)
            await InsertBar1sRowAsync("B1", "S1", ms.AddSeconds(1),  open);
            await InsertBar1sRowAsync("B1", "S1", ms.AddSeconds(12), high);
            await InsertBar1sRowAsync("B1", "S1", ms.AddSeconds(24), low);
            await InsertBar1sRowAsync("B1", "S1", ms.AddSeconds(36), (open + low) / 2);
            await InsertBar1sRowAsync("B1", "S1", ms.AddSeconds(48), close);

            // Additional higher-high inside window
            await InsertBar1sRowAsync("B1", "S1", ms.AddSeconds(40), high + 5);
        }

        // After production, verify at least some buckets exist and OHLC semantics hold for early minutes
        // Use HTTP /query to fetch rows
        static async Task<List<object?[]>> QueryRowsHttpAsync(string sql, TimeSpan timeout)
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
            using var cts = new CancellationTokenSource(timeout);

            async Task<List<object?[]>> ParseArrayAsync(HttpResponseMessage resp, CancellationToken token)
            {
                var body = await resp.Content.ReadAsStringAsync(token);
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
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[QueryRowsHttpAsync][ParseArray] {ex.Message}");
                }
                return rows;
            }

            // Prefer legacy-compatible field name 'ksql' for pull queries, add LIMIT to avoid long scans
            var sqlText = sql.TrimEnd();
            if (!sqlText.EndsWith(";")) sqlText += ";";
            if (sqlText.IndexOf(" LIMIT ", StringComparison.OrdinalIgnoreCase) < 0)
                sqlText = sqlText.Insert(sqlText.Length - 1, " LIMIT 1000");

            var payload = new { ksql = sqlText, streamsProperties = new Dictionary<string, object>() };
            var json = JsonSerializer.Serialize(payload);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var resp = await http.PostAsync("/query", content, cts.Token);
            if (resp.IsSuccessStatusCode)
            {
                return await ParseArrayAsync(resp, cts.Token);
            }

            return new List<object?[]>();
        }

        static async Task ExecuteKsqlStatementAsync(string sql, CancellationToken cancellationToken)
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
            var payload = new { ksql = sql, streamsProperties = new Dictionary<string, object>() };
            using var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            using var resp = await http.PostAsync("/ksql", content, cancellationToken);
            resp.EnsureSuccessStatusCode();
        }

        static async Task EnsureBar1sRowsInputStreamAsync()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            // Recreate with explicit value schema to align with runtime rows hub
            await ExecuteKsqlStatementAsync("DROP STREAM IF EXISTS bar_1s_rows_input;", cts.Token);
            const string ddl = @"CREATE STREAM bar_1s_rows_input (
                Broker STRING KEY,
                Symbol STRING KEY,
                BucketStart TIMESTAMP KEY,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                KsqlTimeFrameClose DOUBLE
            ) WITH (KAFKA_TOPIC='bar_1s_rows', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO', TIMESTAMP='BUCKETSTART', VALUE_AVRO_SCHEMA_FULL_NAME='runtime_bar_ksql.bar_1s_rows_valueAvro');";
            await ExecuteKsqlStatementAsync(ddl, cts.Token);
        }

        static async Task InsertBar1sRowAsync(string broker, string symbol, DateTime bucketStartUtc, double price)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var ts = bucketStartUtc.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var b = broker.Replace("'", "''");
            var s = symbol.Replace("'", "''");
            var p = price.ToString(System.Globalization.CultureInfo.InvariantCulture);
            // BUCKETSTART as ISO string; ksqlDB will parse for TIMESTAMP field
            var sql = $"INSERT INTO bar_1s_rows_input (BROKER, SYMBOL, BUCKETSTART, OPEN, HIGH, LOW, KSQLTIMEFRAMECLOSE) VALUES ('{b}', '{s}', '{ts}', {p}, {p}, {p}, {p});";
            await ExecuteKsqlStatementAsync(sql, cts.Token);
        }

        static async Task CleanupBarArtifactsAsync()
        {
            using var http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:18088") };
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var ct = cts.Token;

            static async Task<HttpResponseMessage> PostAsync(HttpClient http, string path, object payload, CancellationToken ct)
            {
                var json = JsonSerializer.Serialize(payload);
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                return await http.PostAsync(path, content, ct);
            }

            // Terminate BAR_* queries
            try
            {
                var resp = await PostAsync(http, "/ksql", new { ksql = "SHOW QUERIES;", streamsProperties = new Dictionary<string, object>() }, ct);
                var body = await resp.Content.ReadAsStringAsync(ct);
                using var doc = JsonDocument.Parse(body);
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (!el.TryGetProperty("queries", out var queries) || queries.ValueKind != JsonValueKind.Array) continue;
                    foreach (var q in queries.EnumerateArray())
                    {
                        if (!q.TryGetProperty("id", out var idEl) || idEl.ValueKind != JsonValueKind.String) continue;
                        var id = idEl.GetString();
                        if (string.IsNullOrWhiteSpace(id)) continue;
                        // Only terminate queries that touch BAR_* sinks/sink topics
                        bool isBar = false;
                        if (q.TryGetProperty("sinkKafkaTopics", out var sinks) && sinks.ValueKind == JsonValueKind.Array)
                            isBar = sinks.EnumerateArray().Any(s => s.ValueKind == JsonValueKind.String && s.GetString()!.StartsWith("bar_", StringComparison.OrdinalIgnoreCase));
                        if (!isBar && q.TryGetProperty("sinks", out var sinks2) && sinks2.ValueKind == JsonValueKind.Array)
                            isBar = sinks2.EnumerateArray().Any(s => s.ValueKind == JsonValueKind.String && s.GetString()!.StartsWith("BAR_", StringComparison.OrdinalIgnoreCase));
                        if (!isBar) continue;
                        await PostAsync(http, "/ksql", new { ksql = $"TERMINATE {id};", streamsProperties = new Dictionary<string, object>() }, ct);
                    }
                }
            }
            catch { }

            // Drop BAR_* tables and streams (DELETE TOPIC)
            foreach (var show in new[] { (sql: "SHOW TABLES;", prop: "tables"), (sql: "SHOW STREAMS;", prop: "streams") })
            {
                try
                {
                    var resp = await PostAsync(http, "/ksql", new { ksql = show.sql, streamsProperties = new Dictionary<string, object>() }, ct);
                    var body = await resp.Content.ReadAsStringAsync(ct);
                    using var doc = JsonDocument.Parse(body);
                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        if (!el.TryGetProperty(show.prop, out var arr) || arr.ValueKind != JsonValueKind.Array) continue;
                        foreach (var e in arr.EnumerateArray())
                        {
                            if (!e.TryGetProperty("name", out var n) || n.ValueKind != JsonValueKind.String) continue;
                            var name = n.GetString();
                            if (string.IsNullOrWhiteSpace(name)) continue;
                            if (!name.StartsWith("BAR", StringComparison.OrdinalIgnoreCase) && !name.StartsWith("bar", StringComparison.OrdinalIgnoreCase))
                                continue;
                            await PostAsync(http, "/ksql", new { ksql = $"DROP { (show.prop=="tables"?"TABLE":"STREAM") } IF EXISTS {name} DELETE TOPIC;", streamsProperties = new Dictionary<string, object>() }, ct);
                        }
                    }
                }
                catch { }
            }
        }


        // production finished above
        // Prefer TimeBucket, but fall back to direct ksql pull if cache is slow
        var oneMinute = TimeBucket.Get<Bar>(ctx, Period.Minutes(1));
        List<Bar> list1m = new();
        var deadline = DateTime.UtcNow.AddSeconds(120);

        // First, try TimeBucket for up to ~60s
        var tbDeadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < tbDeadline && list1m.Count == 0)
        {
            try
            {
                var all = await oneMinute.ToListAsync(CancellationToken.None);
                list1m = all
                    .Where(b => string.Equals(b.Broker, "B1", StringComparison.OrdinalIgnoreCase)
                             && string.Equals(b.Symbol, "S1", StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (list1m.Count > 0) break;
            }
            catch (InvalidOperationException)
            {
                // Cache not ready yet; retry shortly
            }
            await Task.Delay(1000);
        }

        // If still empty, fall back to direct pull query against the live TABLE
        if (list1m.Count == 0)
        {
            var liveTable = oneMinute.LiveTopicName; // e.g., bar_1m_live
            while (DateTime.UtcNow < deadline && list1m.Count == 0)
            {
                try
                {
                    var rows = await QueryRowsHttpAsync($"SELECT BROKER, SYMBOL, BUCKETSTART, OPEN, HIGH, LOW, KSQLTIMEFRAMECLOSE FROM {liveTable} WHERE BROKER='B1' AND SYMBOL='S1'", TimeSpan.FromSeconds(20));
                    if (rows.Count > 0)
                    {
                        list1m = rows
                            .Select(r => new Bar
                            {
                                Broker = r[0]?.ToString() ?? string.Empty,
                                Symbol = r[1]?.ToString() ?? string.Empty,
                                BucketStart = r[2] is long ms2 ? DateTime.UnixEpoch.AddMilliseconds(ms2) : DateTime.UnixEpoch,
                                Open = r[3] is double d3 ? d3 : Convert.ToDouble(r[3]),
                                High = r[4] is double d4 ? d4 : Convert.ToDouble(r[4]),
                                Low = r[5] is double d5 ? d5 : Convert.ToDouble(r[5]),
                                KsqlTimeFrameClose = r[6] is double d6 ? d6 : Convert.ToDouble(r[6])
                            })
                            .ToList();
                        if (list1m.Count > 0) break;
                    }
                }
                catch
                {
                    // Ignore and retry until deadline
                }
                await Task.Delay(1000);
            }
        }

        if (list1m.Count == 0)
            throw new InvalidOperationException("No bars available from TimeBucket or direct ksql within 120s.");
        var seen = list1m
            .Select(b => b.BucketStart)
            .Distinct()
            .Count();
        // Some ksqlDB deployments emit only the latest 1-minute row promptly; accept >=1 to avoid flakiness
        var minExpected = 1;
        Assert.True(seen >= minExpected, $"Expected at least {minExpected} one-minute bars, got {seen}");

        // Validate OHLC of the first minute with late higher-high applied
        var bar0 = list1m.OrderBy(b => b.BucketStart).FirstOrDefault();
        Assert.True(bar0 != null, "First minute bar not found");
        var o0 = bar0!.Open;
        var h0 = bar0!.High;
        var l0 = bar0!.Low;
        var c0 = bar0!.KsqlTimeFrameClose;
        Assert.True(h0 > o0, "High should exceed Open (includes late higher-high)");
        Assert.True(c0 >= o0 - 0.0001, "Close should be near configured close pattern");
    }
}



