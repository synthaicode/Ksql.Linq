using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

/// <summary>
/// POC: Tumbling+CTAS 縺ｧ菴懈・縺輔ｌ縺・1m 繝ｩ繧､繝傍ABLE縺後〔sql縺ｮPull縺ｧ縺ｯ隕九∴繧九・縺ｫ縲ヾtreamiz繧ｭ繝｣繝・す繝･縺九ｉ縺ｯ遨ｺ縺九←縺・°繧呈､懆ｨｼ縲・/// - 譌｢蟄倥・Bar DSL縺ｮ繝代う繝励Λ繧､繝ｳ繧偵◎縺ｮ縺ｾ縺ｾ蛻ｩ逕ｨ・・ar_1s_rows竊鍛ar_1m_live・峨・/// - ksql Pull縺ｧ bar_1m_live 縺ｫ陦後′蜃ｺ繧九％縺ｨ繧堤｢ｺ隱阪・/// - 縺昴・蠕後ヾtreamiz縺ｮTableCache縺ｫ逶ｴ謗･繝ｪ繝輔Ξ繧ｯ繧ｷ繝ｧ繝ｳ縺ｧ繧｢繧ｯ繧ｻ繧ｹ縺励∝酔荳縺ｮ陦後′蜿門ｾ励〒縺阪ｋ縺九ｒ豈碑ｼ・・/// </summary>
public class TumblingCtasCachePocTests
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
                .Tumbling(r => r.Timestamp, new Ksql.Linq.Query.Dsl.Windows { Minutes = new[] { 1 } })
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
    public async Task POC_TumblingCtas_LiveTable_KsqlPull_VS_StreamizCache()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);

        // Cleanup BAR_* artifacts to avoid upgrade errors
        await CleanupBarArtifactsAsync();

        await using var ctx = new TestContext();

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
            await PhysicalTestEnv.TopicHelpers.WaitForTopicReady(admin, "deduprates", 1, 1, TimeSpan.FromSeconds(10));
        }
        await ctx.WaitForEntityReadyAsync<Rate>(TimeSpan.FromSeconds(60));

        // Produce 2 minutes of past-aligned ticks quickly
        var anchor = DateTime.UtcNow.AddHours(-2);
        var t0 = new DateTime(anchor.Year, anchor.Month, anchor.Day, anchor.Hour, anchor.Minute, 0, DateTimeKind.Utc);
        for (int i = 0; i < 2; i++)
        {
            var ms = t0.AddMinutes(i);
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(1), Bid = 100 + i });
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(20), Bid = 105 + i });
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = ms.AddSeconds(40), Bid = 102 + i });
        }

        // Locate derived 1m live model and topic
        var derived1mModel = ctx.GetEntityModels()
            .Values
            .FirstOrDefault(m => m.AdditionalSettings.TryGetValue("timeframe", out var tf) && tf?.ToString()?.Equals("1m", StringComparison.OrdinalIgnoreCase) == true
                              && m.AdditionalSettings.TryGetValue("role", out var role) && role?.ToString()?.Equals("Live", StringComparison.OrdinalIgnoreCase) == true);
        Assert.True(derived1mModel != null, "Derived 1m live model not found (mapping not registered?)");
        var derived1mType = derived1mModel!.EntityType;
        var liveTopic = derived1mModel!.TopicName;

        // 1) Verify changelog topic (bar_1m_live) emits data (kafka high watermark > 0)
        Assert.True(await WaitForChangelogDataAsync(liveTopic, TimeSpan.FromSeconds(20)), $"Changelog topic {liveTopic} did not advance high watermark");

        var getCache = typeof(Ksql.Linq.Cache.Extensions.KsqlContextCacheExtensions)
            .GetMethod("GetTableCache", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public)!;
        var getCacheGeneric = getCache.MakeGenericMethod(derived1mType!);
        var cacheObj = getCacheGeneric.Invoke(null, new object?[] { ctx });
        Assert.True(cacheObj != null, "TableCache was not registered for 1m live entity");

        var toList = cacheObj!.GetType().GetMethod("ToListAsync", new[] { typeof(List<string>), typeof(TimeSpan?) });
        List<object> cacheRows = new();
        try
        {
            // Pass null timeout to use TableCache default (RUNNING蠕・■繧丹SS蛛ｴ縺ｫ蟋斐・繧・ 譌｢螳・0s)
            var taskObj = (Task)toList!.Invoke(cacheObj, new object?[] { null!, (TimeSpan?)null })!;
            await taskObj; // xUnit: avoid ConfigureAwait(false) in tests
            var resultProp = taskObj.GetType().GetProperty("Result")!;
            var list = (System.Collections.IEnumerable)resultProp.GetValue(taskObj)!;
            cacheRows = list.Cast<object>().ToList();
        }
        catch (TargetInvocationException ex) when (ex.InnerException is TimeoutException ||
                                                  (ex.InnerException?.GetType().FullName?.Contains("IllegalStateException") ?? false))
        {
            throw new TimeoutException("KafkaStreams did not reach RUNNING state within default wait (TableCache).", ex.InnerException);
        }

        // 2) POC: changelog縺ｫ繝・・繧ｿ縺悟・縺ｦ縺・ｋ縺ｮ縺ｫ cacheRows=0 縺ｮ縺ｾ縺ｾ縺ｪ繧峨ゝumbling+CTAS竊担treamiz 騾｣謳ｺ縺ｫ繧ｮ繝｣繝・・縺ｮ逍代＞
        Assert.True(cacheRows.Count > 0, "Streamiz TableCache returned no rows for 1m live (POC)");
    }

    private static async Task CleanupBarArtifactsAsync()
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

    private static async Task<bool> WaitForChangelogDataAsync(string topic, TimeSpan timeout)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build();
        var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(3));
        if (md.Topics == null || md.Topics.Count == 0 || md.Topics[0].Error.Code != ErrorCode.NoError)
            return false;

        var partitions = md.Topics[0].Partitions?.Select(p => p.PartitionId).ToArray() ?? Array.Empty<int>();
        if (partitions.Length == 0) partitions = new[] { 0 };

        var conf = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:39092",
            GroupId = "poc-changelog-check-" + Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false
        };
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(conf).Build();

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            foreach (var pid in partitions)
            {
                var tp = new TopicPartition(topic, new Partition(pid));
                try
                {
                    var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(2));
                    if (wm.High.Value > 0)
                        return true;
                }
                catch { }
            }
            await Task.Delay(500);
        }
        return false;
    }
}
