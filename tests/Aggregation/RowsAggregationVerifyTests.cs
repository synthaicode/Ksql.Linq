using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Window;
using Xunit;

namespace Ksql.Linq.Tests.Aggregation;

public class RowsAggregationVerifyTests
{
    private sealed class Row
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private sealed class Agg
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    private static DateTime FloorTo(DateTime tsUtc, TimeSpan window)
    {
        var ticks = (tsUtc.Ticks / window.Ticks) * window.Ticks;
        return new DateTime(ticks, DateTimeKind.Utc);
    }

    private static async Task<List<Agg>> AggregateAsync(IEnumerable<Row> rows, TimeSpan window)
    {
        var output = new List<Agg>();
        var agg = new WindowAggregator<Row, (string Broker, string Symbol), Agg>(
            window,
            gracePeriod: TimeSpan.Zero,
            sweepInterval: TimeSpan.FromMilliseconds(50),
            idleThreshold: TimeSpan.FromSeconds(1),
            keySelector: r => (r.Broker, r.Symbol),
            timestampSelector: r => r.BucketStart,
            resultSelector: g =>
            {
                var open = g.EarliestByOffset(e => e.Open);
                var close = g.LatestByOffset(e => e.KsqlTimeFrameClose);
                var high = g.Max(e => e.High);
                var low = g.Min(e => e.Low);
                return new Agg
                {
                    Broker = g.Key.Broker,
                    Symbol = g.Key.Symbol,
                    BucketStart = g.WindowStart,
                    Open = open,
                    High = high,
                    Low = low,
                    Close = close
                };
            },
            emitCallback: (a, ct) =>
            {
                output.Add(a);
                return ValueTask.CompletedTask;
            });

        agg.Start();
        foreach (var r in rows)
            agg.ProcessMessage(r);
        await agg.FlushAsync(CancellationToken.None);
        await agg.DisposeAsync();
        return output.OrderBy(a => a.BucketStart).ToList();
    }

    [Fact]
    public async Task Verify_1m_from_rows_matches_expected()
    {
        var baseUtc = new DateTime(2025, 10, 13, 22, 31, 0, DateTimeKind.Utc);
        var rows = new List<Row>
        {
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddSeconds(14), Open=100, High=100, Low=100, KsqlTimeFrameClose=100 },
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddSeconds(33), Open=105, High=105, Low=105, KsqlTimeFrameClose=105 },
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddSeconds(53), Open=99,  High=99,  Low=99,  KsqlTimeFrameClose=99  },
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddMinutes(1).AddSeconds(8), Open=101, High=101, Low=101, KsqlTimeFrameClose=101 },
        };

        var actual = await AggregateAsync(rows, TimeSpan.FromMinutes(1));

        // expected by simple LINQ over floor-to-minute
        var expected = rows
            .GroupBy(r => (r.Broker, r.Symbol, Bucket: FloorTo(r.BucketStart, TimeSpan.FromMinutes(1))))
            .OrderBy(g => g.Key.Bucket)
            .Select(g => new Agg
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.Key.Bucket,
                Open = g.First().Open,
                High = g.Max(e => e.High),
                Low = g.Min(e => e.Low),
                Close = g.Last().KsqlTimeFrameClose
            })
            .ToList();

        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].BucketStart, actual[i].BucketStart);
            Assert.Equal(expected[i].Open, actual[i].Open, 6);
            Assert.Equal(expected[i].High, actual[i].High, 6);
            Assert.Equal(expected[i].Low, actual[i].Low, 6);
            Assert.Equal(expected[i].Close, actual[i].Close, 6);
        }
    }

    [Fact]
    public async Task Verify_5m_from_rows_matches_expected()
    {
        var baseUtc = new DateTime(2025, 10, 13, 22, 30, 0, DateTimeKind.Utc);
        var rows = new List<Row>
        {
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddMinutes(1).AddSeconds(14), Open=100, High=100, Low=100, KsqlTimeFrameClose=100 },
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddMinutes(1).AddSeconds(33), Open=105, High=105, Low=105, KsqlTimeFrameClose=105 },
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddMinutes(1).AddSeconds(53), Open=99,  High=99,  Low=99,  KsqlTimeFrameClose=99  },
            new() { Broker="B", Symbol="S", BucketStart = baseUtc.AddMinutes(2).AddSeconds(8),  Open=101, High=101, Low=101, KsqlTimeFrameClose=101 },
        };

        var actual = await AggregateAsync(rows, TimeSpan.FromMinutes(5));

        var expected = rows
            .GroupBy(r => (r.Broker, r.Symbol, Bucket: FloorTo(r.BucketStart, TimeSpan.FromMinutes(5))))
            .OrderBy(g => g.Key.Bucket)
            .Select(g => new Agg
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.Key.Bucket,
                Open = g.First().Open,
                High = g.Max(e => e.High),
                Low = g.Min(e => e.Low),
                Close = g.Last().KsqlTimeFrameClose
            })
            .ToList();

        Assert.Single(actual);
        Assert.Single(expected);
        Assert.Equal(expected[0].BucketStart, actual[0].BucketStart);
        Assert.Equal(expected[0].Open, actual[0].Open, 6);
        Assert.Equal(expected[0].High, actual[0].High, 6);
        Assert.Equal(expected[0].Low, actual[0].Low, 6);
        Assert.Equal(expected[0].Close, actual[0].Close, 6);
    }
}
