using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using System;
using System.Linq;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class DerivationPlannerTests
{
    [KsqlTopic("bar")]
    private class Source
    {
        public int Id { get; set; }
    }

    [KsqlTopic("bar")]
    private class SourceOhlc
    {
        public int Id { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double KsqlTimeFrameClose { get; set; }
    }

    private static TumblingQao Create(params Timeframe[] tfs) => new()
    {
        TimeKey = "Timestamp",
        Windows = tfs,
        Keys = new[] { "Id", "BucketStart" },
        Projection = new[] { "Id", "BucketStart", "KsqlTimeFrameClose" },
        PocoShape = new[]
        {
            new ColumnShape("Id", typeof(int), false),
            new ColumnShape("BucketStart", typeof(long), false),
            new ColumnShape("KsqlTimeFrameClose", typeof(double), false)
        },
        BasedOn = new BasedOnSpec(new[] { "Id", "BucketStart" }, string.Empty, "KsqlTimeFrameClose", string.Empty),
        WeekAnchor = DayOfWeek.Monday
    };

    [Fact]
    public void Plan_1m_Includes_Hub_And_Live_Only()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "m")), model);

        var hub = Assert.Single(entities, e => e.Id == "bar_1s_rows" && e.Role == Role.Final1sStream);
        Assert.Null(hub.InputHint);

        var live1m = Assert.Single(entities, e => e.Id == "bar_1m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live1m.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final");
        // Legacy roles removed; only hub(1s rows) and live should exist
    }

    [Fact]
    public void Plan_5m_Uses_1s_Hub_Live_Only()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(5, "m")), model);

        var hub = Assert.Single(entities, e => e.Id == "bar_1s_rows" && e.Role == Role.Final1sStream);
        Assert.Null(hub.InputHint);

        var live5m = Assert.Single(entities, e => e.Id == "bar_5m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live5m.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final");
        // Legacy roles removed
    }

    [Fact]
    public void Plan_Windows_Final_Use_Hub_Input()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "m"), new Timeframe(5, "m")), model);

        var live1 = Assert.Single(entities, e => e.Id == "bar_1m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live1.InputHint);
        var live5 = Assert.Single(entities, e => e.Id == "bar_5m_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live5.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final" && e.Id.EndsWith("_final", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Plan_1wk_Final_Uses_1s_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "wk")), model);
        var live = Assert.Single(entities, e => e.Id == "bar_1wk_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final" && e.Id.EndsWith("_final", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Plan_Hour_Windows_Use_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(3, "h"), new Timeframe(1, "h")), model);
        var live1 = Assert.Single(entities, e => e.Id == "bar_1h_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live1.InputHint);
        var live3 = Assert.Single(entities, e => e.Id == "bar_3h_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live3.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final" && e.Id.EndsWith("_final", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Plan_Day_Windows_Use_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "d"), new Timeframe(1, "h")), model);
        var liveH = Assert.Single(entities, e => e.Id == "bar_1h_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", liveH.InputHint);
        var liveD = Assert.Single(entities, e => e.Id == "bar_1d_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", liveD.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final" && e.Id.EndsWith("_final", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Plan_Month_Windows_Use_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "mo")), model);
        var live = Assert.Single(entities, e => e.Id == "bar_1mo_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final" && e.Id.EndsWith("_final", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Plan_Months_Array_Uses_Hub()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "mo"), new Timeframe(12, "mo")), model);
        var live1 = Assert.Single(entities, e => e.Id == "bar_1mo_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live1.InputHint);
        var live12 = Assert.Single(entities, e => e.Id == "bar_12mo_live" && e.Role == Role.Live);
        Assert.Equal("bar_1s_rows", live12.InputHint);
        Assert.DoesNotContain(entities, e => e.Role.ToString() == "Final" && e.Id.EndsWith("_final", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Plan_WhenEmpty_Does_Not_Add_Fill_Entity()
    {
        var model = new EntityModel { EntityType = typeof(Source) };
        var entities = DerivationPlanner.Plan(Create(new Timeframe(1, "m")), model);
        // Legacy roles removed
    }
}