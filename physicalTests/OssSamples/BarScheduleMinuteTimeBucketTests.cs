using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Runtime;
using Microsoft.Extensions.Logging;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration;

public class BarScheduleMinuteTimeBucketTests
{
    [KsqlTopic("deduprates")]
    public class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        [KsqlDecimal(18,2)] public decimal Bid { get; set; }
    }

    [KsqlTopic("marketschedule")]
    public class MarketSchedule
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
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
        private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
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
        }, _lf) { }
        protected override bool SkipSchemaRegistration => false;
        public EventSet<Rate> Rates { get; set; } = null!;
        public EventSet<MarketSchedule> Schedules { get; set; } = null!;
        protected override void OnModelCreating(IModelBuilder mb)
        {
            mb.Entity<Bar>()
              .ToQuery(q => q.From<Rate>()
                .TimeFrame<MarketSchedule>((r, s) => r.Broker == s.Broker && r.Symbol == s.Symbol && s.Open <= r.Timestamp && r.Timestamp < s.Close,
                                           dayKey: s => s.MarketDate)
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

    private static DateTime Utc(int y, int M, int d, int h, int m, int s = 0)
        => DateTime.SpecifyKind(new DateTime(y, M, d, h, m, s), DateTimeKind.Utc);

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultipleSchedules_PerKey_And_OutOfSession_Filtered_MinuteBars()
    {
        await PhysicalTestEnv.KsqlHelpers.WaitForKsqlReadyAsync("http://127.0.0.1:18088", TimeSpan.FromSeconds(180), graceMs: 2000);
        var ctx = new TestContext();

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build())
        {
            try { await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = "deduprates", NumPartitions = 1, ReplicationFactor = 1 }, new TopicSpecification { Name = "marketschedule", NumPartitions = 1, ReplicationFactor = 1 } }); } catch { }
        }

        // 蝗ｺ螳壼渕貅匁凾蛻ｻ・域ｱｺ螳夊ｫ厄ｼ・ 2025-01-01 12:00:00Z
        var baseTs = Utc(2025, 1, 1, 12, 0, 0);

        // 繧ｹ繧ｱ繧ｸ繝･繝ｼ繝ｫ: 蜷ПK縺斐→縺ｫ2蝗槫・・磯幕蟋句性繧/邨ゆｺ・性縺ｾ縺ｪ縺・ｼ・        // B1/S1: [12:00,12:02) 縺ｨ [12:03,12:05)
        await ctx.Schedules.AddAsync(new MarketSchedule { Broker = "B1", Symbol = "S1", MarketDate = baseTs.Date, Open = baseTs, Close = baseTs.AddMinutes(2) });
        await ctx.Schedules.AddAsync(new MarketSchedule { Broker = "B1", Symbol = "S1", MarketDate = baseTs.Date, Open = baseTs.AddMinutes(3), Close = baseTs.AddMinutes(5) });
        // B2/S2: [12:01,12:03) 縺ｨ [12:04,12:05)
        await ctx.Schedules.AddAsync(new MarketSchedule { Broker = "B2", Symbol = "S2", MarketDate = baseTs.Date, Open = baseTs.AddMinutes(1), Close = baseTs.AddMinutes(3) });
        await ctx.Schedules.AddAsync(new MarketSchedule { Broker = "B2", Symbol = "S2", MarketDate = baseTs.Date, Open = baseTs.AddMinutes(4), Close = baseTs.AddMinutes(5) });

        // 繝・・繧ｿ謚募・: 蜷ПK縺ｫ繝舌Μ繧ｨ繝ｼ繧ｷ繝ｧ繝ｳ・九せ繧ｱ繧ｸ繝･繝ｼ繝ｫ螟悶ョ繝ｼ繧ｿ繧呈ｷｷ蝨ｨ縺輔○繧・        // B1/S1 in-session minutes: 12:00,12:01,12:03,12:04・・2:02 縺ｯ螟悶・2:05 繧ょ､厄ｼ・        var b1s1 = new (DateTime ts, decimal bid)[]
        {
            (baseTs.AddSeconds(10), 100m), (baseTs.AddSeconds(20), 105m), (baseTs.AddSeconds(40), 101m), // 12:00
            (baseTs.AddMinutes(1).AddSeconds(15), 108m), (baseTs.AddMinutes(1).AddSeconds(25), 97m), // 12:01
            // out-of-session 12:02
            (baseTs.AddMinutes(2).AddSeconds(10), 123m),
            // in-session 12:03
            (baseTs.AddMinutes(3).AddSeconds(05), 106m), (baseTs.AddMinutes(3).AddSeconds(35), 100m),
            // in-session 12:04
            (baseTs.AddMinutes(4).AddSeconds(25), 112m), (baseTs.AddMinutes(4).AddSeconds(55), 109m),
            // out-of-session exact end 12:05
            (baseTs.AddMinutes(5), 777m)
        };
        foreach (var e in b1s1)
            await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = e.ts, Bid = e.bid });

        // B1/S1 - 遯灘｢・阜4轤ｹ・磯幕蟋・1ms/髢句ｧ・邨ゆｺ・ﾎｵ/邨ゆｺ・ｼ峨ｒ霑ｽ蜉謚募・
        // 12:00 遯・ start-1ms=11:59:59.999(髯､螟・, start=12:00:00(蜷ｫ繧), end-ﾎｵ=12:00:59.999(蜷ｫ繧), end=12:01:00(谺｡遯・
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMilliseconds(-1), Bid = 100m }); // 髯､螟・        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs, Bid = 100m });                    // 蜷ｫ繧(open=100)
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(0).AddSeconds(59).AddMilliseconds(999), Bid = 101m }); // 蜷ｫ繧(close=101)
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(1), Bid = 150m });       // 谺｡遯難ｼ・2:01・・        // 繧ｻ繝・す繝ｧ繝ｳ蠅・阜 12:02 end / 12:03 start
        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(2), Bid = 200m }); // 12:02:00・磯勁螟厄ｼ・        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(3).AddMilliseconds(-1), Bid = 100m }); // 12:02:59.999・磯勁螟厄ｼ・        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(3), Bid = 106m }); // 12:03:00・亥性繧・・        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(4).AddSeconds(59).AddMilliseconds(999), Bid = 109m }); // 12:04:59.999・亥性繧・・        await ctx.Rates.AddAsync(new Rate { Broker = "B1", Symbol = "S1", Timestamp = baseTs.AddMinutes(5), Bid = 300m }); // 12:05:00・磯勁螟厄ｼ・
        // B2/S2 in-session minutes: 12:01,12:02,12:04・・2:00/12:03/12:05 縺ｯ螟厄ｼ・        var b2s2 = new (DateTime ts, decimal bid)[]
        {
            // out-of-session 12:00
            (baseTs.AddSeconds(15), 999m),
            // in-session 12:01
            (baseTs.AddMinutes(1).AddSeconds(5), 202m), (baseTs.AddMinutes(1).AddSeconds(45), 203m),
            // in-session 12:02
            (baseTs.AddMinutes(2).AddSeconds(10), 204m), (baseTs.AddMinutes(2).AddSeconds(50), 205m),
            // out-of-session 12:03
            (baseTs.AddMinutes(3).AddSeconds(10), 999m),
            // in-session 12:04
            (baseTs.AddMinutes(4).AddSeconds(20), 210m)
        };
        foreach (var e in b2s2)
            await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = e.ts, Bid = e.bid });

        // B2/S2 - 遯灘｢・阜4轤ｹ繧定ｿｽ蜉謚募・・・2:01/12:02 遯薙・2:03 邨らｫｯ縲・2:04 髢句ｧ九・m end-ﾎｵ・・        await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = baseTs.AddMinutes(1).AddMilliseconds(-1), Bid = 202m }); // 12:00:59.999・磯勁螟厄ｼ・        await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = baseTs.AddMinutes(1), Bid = 202m });                      // 12:01:00・亥性繧・・        await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = baseTs.AddMinutes(2).AddSeconds(59).AddMilliseconds(999), Bid = 205m }); // 12:02:59.999・亥性繧・・        await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = baseTs.AddMinutes(3), Bid = 999m }); // 12:03:00・磯勁螟厄ｼ・        await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = baseTs.AddMinutes(4), Bid = 210m }); // 12:04:00・亥性繧・・        await ctx.Rates.AddAsync(new Rate { Broker = "B2", Symbol = "S2", Timestamp = baseTs.AddMinutes(4).AddSeconds(59).AddMilliseconds(999), Bid = 210m }); // 12:04:59.999・亥性繧・・
        // ksql 蛛ｴ繧ｪ繝悶ず繧ｧ繧ｯ繝医・襍ｷ蜍・CTAS 縺ｮ繝ｪ繧ｹ繝亥喧蠕・ｩ滂ｼ郁ｻｽ縺上・繝ｭ繝ｼ繝厄ｼ・        try { await ctx.QueryStreamCountAsync("SELECT * FROM bar_1m_live EMIT CHANGES LIMIT 1;", TimeSpan.FromSeconds(30)); } catch { }
        try { await ctx.QueryStreamCountAsync("SELECT * FROM bar_5m_live EMIT CHANGES LIMIT 1;", TimeSpan.FromSeconds(30)); } catch { }

        // TimeBucket 縺ｧ 1m/5m 繧貞叙蠕暦ｼ医く繝ｼ縺斐→・・        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(300));
        var list1m_b1 = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { "B1", "S1" }, cts.Token);
        var list1m_b2 = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(1), new[] { "B2", "S2" }, cts.Token);
        var list5m_b1 = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(5), new[] { "B1", "S1" }, cts.Token);
        var list5m_b2 = await Ksql.Linq.TimeBucket.ReadAsync<Bar>(ctx, Period.Minutes(5), new[] { "B2", "S2" }, cts.Token);

        // 譛溷ｾ・ｪ馴寔蜷茨ｼ磯幕蟋九・蜷ｫ繧/邨ゆｺ・・蜷ｫ縺ｾ縺ｪ縺・ｼ・        static DateTime FloorMinute(DateTime t, int size) => new DateTime((t.Ticks / TimeSpan.FromMinutes(size).Ticks) * TimeSpan.FromMinutes(size).Ticks, DateTimeKind.Utc);
        var expected1m_b1 = new HashSet<DateTime> { baseTs, baseTs.AddMinutes(1), baseTs.AddMinutes(3), baseTs.AddMinutes(4) };
        var expected1m_b2 = new HashSet<DateTime> { baseTs.AddMinutes(1), baseTs.AddMinutes(2), baseTs.AddMinutes(4) };

        // 螳滓ｸｬ髮・粋
        var actual1m_b1 = list1m_b1.Select(b => new DateTime(b.BucketStart.Year, b.BucketStart.Month, b.BucketStart.Day, b.BucketStart.Hour, b.BucketStart.Minute, 0, DateTimeKind.Utc)).ToHashSet();
        var actual1m_b2 = list1m_b2.Select(b => new DateTime(b.BucketStart.Year, b.BucketStart.Month, b.BucketStart.Day, b.BucketStart.Hour, b.BucketStart.Minute, 0, DateTimeKind.Utc)).ToHashSet();

        Assert.True(expected1m_b1.SetEquals(actual1m_b1), $"B1/S1 1m set mismatch. expected={expected1m_b1.Count} actual={actual1m_b1.Count}");
        Assert.True(expected1m_b2.SetEquals(actual1m_b2), $"B2/S2 1m set mismatch. expected={expected1m_b2.Count} actual={actual1m_b2.Count}");

        // 莉ｶ謨ｰ繝√ぉ繝・け・亥崋螳壼渕貅厄ｼ・        Assert.Equal(expected1m_b1.Count, list1m_b1.Count);
        Assert.Equal(expected1m_b2.Count, list1m_b2.Count);

        // 驥崎ｦ∫ｪ難ｼ・2:00・峨ｒ蜊ｳ蛟､縺ｧ譁ｭ螳夲ｼ・HLC・・        var b1200 = Assert.Single(list1m_b1.Where(b => b.BucketStart == baseTs));
        Assert.Equal(100m, b1200.Open);
        Assert.Equal(105m, b1200.High);
        Assert.Equal(99m,  b1200.Low);
        Assert.Equal(101m, b1200.KsqlTimeFrameClose);

        // B2/S2 縺ｮ 12:02 繧貞叉蛟､縺ｧ譁ｭ螳夲ｼ医せ繧ｱ繧ｸ繝･繝ｼ繝ｫ蜀・ｼ・        var b1202_b2 = Assert.Single(list1m_b2.Where(b => b.BucketStart == baseTs.AddMinutes(2)));
        Assert.Equal(204m, b1202_b2.Open);
        Assert.Equal(205m, b1202_b2.High);
        Assert.Equal(204m, b1202_b2.Low);
        Assert.Equal(205m, b1202_b2.KsqlTimeFrameClose);

        // 5m 縺ｮ蠅・阜4轤ｹ・磯幕蟋・1ms/髢句ｧ・邨ゆｺ・ﾎｵ/邨ゆｺ・ｼ画､懆ｨｼ・磯寔蜷・1縲√°縺､OHLC蜊ｳ蛟､・・        var start5 = FloorMinute(baseTs, 5);
        var b5m_b1 = Assert.Single(list5m_b1);
        Assert.Equal(start5, b5m_b1.BucketStart);
        Assert.Equal(100m, b5m_b1.Open);  // 12:00:00
        Assert.Equal(112m, b5m_b1.High);  // 譛螟ｧ蛟､
        Assert.Equal(97m,  b5m_b1.Low);   // 譛蟆丞､
        Assert.Equal(109m, b5m_b1.KsqlTimeFrameClose); // 12:04:59.999

        var b5m_b2 = Assert.Single(list5m_b2);
        Assert.Equal(start5, b5m_b2.BucketStart);
        Assert.Equal(202m, b5m_b2.Open);  // 12:01:00・域怙蛻昴・in-session・・        Assert.Equal(210m, b5m_b2.High);  // 12:04:00/59.999
        Assert.Equal(202m, b5m_b2.Low);
        Assert.Equal(210m, b5m_b2.KsqlTimeFrameClose); // 12:04:59.999

        // 蠕檎援莉倥￠
        try { await ctx.DisposeAsync(); } catch { }
    }
}
