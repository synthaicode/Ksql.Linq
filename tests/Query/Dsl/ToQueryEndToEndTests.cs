using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using Xunit;
using System.Threading.Tasks;
using System.Linq;

namespace Ksql.Linq.Tests.Query.Dsl;

public class ToQueryEndToEndTests
{
    [KsqlTopic("deduprates")]
    private class DeDupRate
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class MarketSchedule
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime MarketDate { get; set; }
    }

    [KsqlTable]
    private class Rate
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public string Symbol { get; set; } = string.Empty;
        [KsqlKey] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class RateSet : EventSet<Rate>
    {
        public RateSet(EntityModel model) : base(new DummyContext(), model) { }
        protected override Task SendEntityAsync(Rate entity, Dictionary<string, string>? headers, System.Threading.CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<Rate> GetAsyncEnumerator(System.Threading.CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [KsqlTable]
    private class SingleKeyRate
    {
        [KsqlKey] public string Broker { get; set; } = string.Empty;
        [KsqlKey] public DateTime BucketStart { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
    }

    private class SingleKeyRateSet : EventSet<SingleKeyRate>
    {
        public SingleKeyRateSet(EntityModel model) : base(new DummyContext(), model) { }
        protected override Task SendEntityAsync(SingleKeyRate entity, Dictionary<string, string>? headers, System.Threading.CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<SingleKeyRate> GetAsyncEnumerator(System.Threading.CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [Fact]
    public void Generates_expected_live_ddls()
    {
        var mb = new ModelBuilder();
        mb.Entity<Rate>();
        var model = mb.GetEntityModel<Rate>()!;
        var set = new RateSet(model);

        set.ToQuery(q => q
            .From<DeDupRate>()
            .TimeFrame<MarketSchedule>((r, s) =>
                r.Broker == s.Broker &&
                r.Symbol == s.Symbol &&
                s.Open <= r.Timestamp &&
                r.Timestamp < s.Close,
                dayKey: s => s.MarketDate)
            .Tumbling(r => r.Timestamp, new Windows
            {
                Minutes = new[] { 1, 5 }
            }, 1, TimeSpan.FromSeconds(1))
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new Rate
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid)
            })
        );

        var live1m = KsqlCreateWindowedStatementBuilder.Build(
            "rate_1m_live",
            model.QueryModel!,
            "1m",
            "EMIT CHANGES",
            "rate_1s_rows");

        var live5m = KsqlCreateWindowedStatementBuilder.Build(
            "rate_5m_live",
            model.QueryModel!,
            "5m",
            "EMIT CHANGES",
            "rate_1s_rows");

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(live1m, "CREATE TABLE IF NOT EXISTS rate_1m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(live1m, "FROM rate_1s_rows");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(live1m, "WINDOW TUMBLING (SIZE 1 MINUTES)");

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(live5m, "CREATE TABLE IF NOT EXISTS rate_5m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(live5m, "FROM rate_1s_rows");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(live5m, "WINDOW TUMBLING (SIZE 5 MINUTES)");
    }

    [Fact]
    public void Generates_single_key_live_ddls()
    {
        var mb = new ModelBuilder();
        mb.Entity<SingleKeyRate>();
        var model = mb.GetEntityModel<SingleKeyRate>()!;
        var set = new SingleKeyRateSet(model);

        set.ToQuery(q => q
            .From<DeDupRate>()
            .TimeFrame<MarketSchedule>((r, s) =>
                r.Broker == s.Broker &&
                r.Symbol == s.Symbol &&
                s.Open <= r.Timestamp &&
                r.Timestamp < s.Close,
                dayKey: s => s.MarketDate)
            .Tumbling(r => r.Timestamp, new Windows
            {
                Minutes = new[] { 5, 15 },
                Hours = new[] { 1, 4 }
            }, 1, TimeSpan.FromSeconds(1))
            .GroupBy(r => r.Broker)
            .Select(g => new SingleKeyRate
            {
                Broker = g.Key,
                BucketStart = g.WindowStart(),
                Open = g.EarliestByOffset(x => x.Bid),
                High = g.Max(x => x.Bid),
                Low = g.Min(x => x.Bid),
                Close = g.LatestByOffset(x => x.Bid)
            })
        );

        var live5m = KsqlCreateWindowedStatementBuilder.Build(
            "rate_broker_5m_live",
            model.QueryModel!,
            "5m",
            "EMIT CHANGES",
            "rate_broker_1s_rows");

        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(live5m, "CREATE TABLE IF NOT EXISTS rate_broker_5m_live");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(live5m, "FROM rate_broker_1s_rows");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(live5m, "WINDOW TUMBLING (SIZE 5 MINUTES)");
    }
}


