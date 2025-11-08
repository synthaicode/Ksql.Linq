using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Ksql.Linq.Tests.Runtime;

public class WeekendScheduleWhenEmptyTests
{
    private readonly ITestOutputHelper _out;
    public WeekendScheduleWhenEmptyTests(ITestOutputHelper output) => _out = output;

    public sealed class Tick
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime TimestampUtc { get; set; }
        public decimal Bid { get; set; }
    }

    public sealed class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime OpenTimeUtc { get; set; }
        public DateTime CloseTimeUtc { get; set; }
        public DateTime MarketDate => OpenTimeUtc.Date;
    }

    public sealed class Rate
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
    }

    private static DateTime FloorMin(DateTime dt) => new(((dt.Ticks / TimeSpan.TicksPerMinute) * TimeSpan.TicksPerMinute), DateTimeKind.Utc);
    private static DateTime Floor5m(DateTime dt) => new(((dt.Ticks / TimeSpan.FromMinutes(5).Ticks) * TimeSpan.FromMinutes(5).Ticks), DateTimeKind.Utc);

    private static List<Rate> Build1mBarsWithWhenEmpty(string broker, string symbol, MarketSchedule sch, IEnumerable<Tick> ticks)
    {
        var src = ticks.Where(t => t.Broker == broker && t.Symbol == symbol)
                       .Where(t => t.TimestampUtc >= sch.OpenTimeUtc && t.TimestampUtc < sch.CloseTimeUtc)
                       .OrderBy(t => t.TimestampUtc)
                       .ToList();
        var start = FloorMin(sch.OpenTimeUtc);
        var end = FloorMin(sch.CloseTimeUtc);
        var minutes = new List<DateTime>();
        for (var t = start; t < end; t = t.AddMinutes(1)) minutes.Add(t);
        var bars = new List<Rate>();
        Rate? prev = null; int idx = 0;
        foreach (var m in minutes)
        {
            var next = m.AddMinutes(1);
            var group = new List<Tick>();
            while (idx < src.Count && src[idx].TimestampUtc >= m && src[idx].TimestampUtc < next)
            { group.Add(src[idx]); idx++; }
            if (group.Count > 0)
            {
                var o = group.First().Bid; var h = group.Max(x => x.Bid); var l = group.Min(x => x.Bid); var c = group.Last().Bid;
                var bar = new Rate { Broker = broker, Symbol = symbol, BucketStart = m, Open = o, High = h, Low = l, Close = c };
                bars.Add(bar); prev = bar;
            }
            else if (prev != null)
            {
                var c = prev.Close;
                var bar = new Rate { Broker = broker, Symbol = symbol, BucketStart = m, Open = c, High = c, Low = c, Close = c };
                bars.Add(bar); prev = bar;
            }
        }
        return bars;
    }

    [Fact]
    public void Schedule_Excludes_Weekend_WhenEmpty_Fills_Weekday_Only()
    {
        var broker = "B1"; var symbol = "S1";
        // Assume Fri=2025-09-05, Sat=2025-09-06, Sun=2025-09-07, Mon=2025-09-08
        var fri = new DateTime(2025, 9, 5, 0, 0, 0, DateTimeKind.Utc);
        var sat = fri.AddDays(1);
        var sun = fri.AddDays(2);
        var mon = fri.AddDays(3);

        // Sessions: Fri 00:00-00:10, Mon 00:00-00:10 (Sat/Sun 莨大ｴ)
        var friSch = new MarketSchedule { Broker = broker, Symbol = symbol, OpenTimeUtc = fri, CloseTimeUtc = fri.AddMinutes(10) };
        var monSch = new MarketSchedule { Broker = broker, Symbol = symbol, OpenTimeUtc = mon, CloseTimeUtc = mon.AddMinutes(10) };

        // Ticks: Fri縺ｨMon縺ｫ1遘・0.01縲４at/Sun縺ｯ逕滓・縺励※繧５imeFrame(=sch遽・峇)縺ｧ髯､螟悶＆繧後ｋ諠ｳ螳・
        var ticks = new List<Tick>();
        void AddTicks(DateTime start)
        {
            var cur = start; decimal px = 100m;
            for (int s = 0; s < 600; s++)
            {
                // 2蛻・岼繧呈ｬ謳坂・WhenEmpty 遒ｺ隱・
                if (!(s >= 60 && s < 120))
                    ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = cur, Bid = Math.Round(px, 4, MidpointRounding.AwayFromZero) });
                cur = cur.AddSeconds(1); px += 0.01m;
            }
        }
        AddTicks(fri);
        AddTicks(mon);
        // 縺､縺・〒縺ｫ蝨滓律縺ｮ繝・・繧ｿ繧よ兜蜈･・医＠縺九＠繧ｹ繧ｱ繧ｸ繝･繝ｼ繝ｫ螟悶↑縺ｮ縺ｧ辟｡隕悶＆繧後ｋ・・
        ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = sat.AddMinutes(1), Bid = 999m });
        ticks.Add(new Tick { Broker = broker, Symbol = symbol, TimestampUtc = sun.AddMinutes(1), Bid = 999m });

        _out.WriteLine("[ticks] samples around Fri and Mon (weekend ignored by TimeFrame)");
        foreach (var t in ticks.Where(t => t.TimestampUtc < fri.AddMinutes(1)).Take(5))
            _out.WriteLine($"F {t.TimestampUtc:MM-dd HH:mm:ss} {t.Bid:F4}");
        foreach (var t in ticks.Where(t => t.TimestampUtc >= mon && t.TimestampUtc < mon.AddMinutes(1)).Take(5))
            _out.WriteLine($"M {t.TimestampUtc:MM-dd HH:mm:ss} {t.Bid:F4}");

        var friBars = Build1mBarsWithWhenEmpty(broker, symbol, friSch, ticks);
        var monBars = Build1mBarsWithWhenEmpty(broker, symbol, monSch, ticks);
        Assert.Equal(10, friBars.Count);
        Assert.Equal(10, monBars.Count);

        // 蝨滓律繧ｻ繝・す繝ｧ繝ｳ縺ｯ辟｡縺・・邨仙粋蠕後・蜈ｨ菴薙ヰ繝ｼ縺ｫ繧ょ悄譌･縺ｮ蛻・・蟄伜惠縺励↑縺・ｼ・imeFrame 蜉ｹ縺・※縺・ｋ蜑肴署・・
        var allBars = new List<Rate>();
        allBars.AddRange(friBars);
        allBars.AddRange(monBars);
        Assert.DoesNotContain(allBars, b => b.BucketStart.Date == sat.Date || b.BucketStart.Date == sun.Date);

        // Fri縺ｮ2蛻・岼縺・WhenEmpty 陬懷ｮ後＆繧後※縺・ｋ縺薙→
        Assert.Equal(friBars[0].Close, friBars[1].Open);
        Assert.Equal(friBars[1].Open, friBars[1].High);
        Assert.Equal(friBars[1].Open, friBars[1].Low);
        Assert.Equal(friBars[1].Open, friBars[1].Close);

        // 5蛻・Ο繝ｼ繝ｫ繧｢繝・・繧ょｹｳ譌･縺ｮ縺ｿ逕滓・縺輔ｌ繧・
        var fri5 = friBars.GroupBy(b => Floor5m(b.BucketStart)).Select(g => g.Key).ToList();
        var mon5 = monBars.GroupBy(b => Floor5m(b.BucketStart)).Select(g => g.Key).ToList();
        Assert.All(fri5, d => Assert.Equal(DayOfWeek.Friday, d.DayOfWeek));
        Assert.All(mon5, d => Assert.Equal(DayOfWeek.Monday, d.DayOfWeek));
    }
}
