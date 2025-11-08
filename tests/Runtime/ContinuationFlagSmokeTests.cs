using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Dsl;
using System;
using Xunit;

namespace Ksql.Linq.Tests.Runtime;

[Trait("Level", "L5")]
public class ContinuationFlagSmokeTests
{
    [KsqlTopic("rate")]
    private class Rate
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [Fact]
    public void RowMonitor_Enables_Continuation_When_Model_Requests_It()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } }, continuation: true)
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { Broker = g.Key.Broker, Symbol = g.Key.Symbol, BucketStart = g.WindowStart(), Close = g.LatestByOffset(x => x.Bid) })
            .Build();
        Assert.True(model.Continuation);
        Assert.True(model.Extras.TryGetValue("continuation", out var extra) && extra is bool b && b);
    }
}