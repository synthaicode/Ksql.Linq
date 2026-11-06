using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using Ksql.Linq.Runtime.Heartbeat;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Mapping;

[Trait("Level", "L3")]
public class MarketScheduleNulKeyRegressionTests
{
    public class Schedule
    {
        public string First { get; set; } = string.Empty;
        public string Second { get; set; } = string.Empty;
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
    }

    [Theory]
    [InlineData("a\0b", "c")]
    [InlineData("a", "b\0c")]
    public async Task BuildIndex_RejectsAmbiguousComponents(string first, string second)
    {
        var provider = CreateProvider();
        await Assert.ThrowsAsync<ArgumentException>(() => provider.InitializeAsync(typeof(Schedule),
            new[] { Row(first, second) }, CancellationToken.None));
    }

    [Theory]
    [InlineData("a\0b", "c")]
    [InlineData("a", "b\0c")]
    public async Task Lookup_RejectsAmbiguousComponents(string first, string second)
    {
        var provider = CreateProvider();
        var row = Row("a", "b");
        await provider.InitializeAsync(typeof(Schedule), new[] { row }, CancellationToken.None);
        Assert.Throws<ArgumentException>(() => provider.IsInSession(new[] { first, second }, row.Open));
    }

    [Theory]
    [InlineData("東京", "regular")]
    [InlineData("", "regular")]
    public async Task ValidComponents_PreserveLookupAndSessionBounds(string first, string second)
    {
        var provider = CreateProvider();
        var row = Row(first, second);
        await provider.InitializeAsync(typeof(Schedule), new[] { row }, CancellationToken.None);
        Assert.True(provider.IsInSession(new[] { first, second }, row.Open));
        Assert.False(provider.IsInSession(new[] { first, second }, row.Close));
        Assert.False(provider.IsInSession(new[] { first, second + "other" }, row.Open));
    }

    private static MarketScheduleProvider CreateProvider()
    {
        var registry = new MappingRegistry();
        var properties = typeof(Schedule).GetProperties();
        registry.Register(typeof(Schedule), properties.Take(2).Select(p => PropertyMeta.FromProperty(p)).ToArray(),
            properties.Select(p => PropertyMeta.FromProperty(p)).ToArray());
        return new MarketScheduleProvider(registry);
    }

    private static Schedule Row(string first, string second) => new()
    {
        First = first, Second = second,
        Open = new DateTime(2026, 11, 6, 0, 0, 0, DateTimeKind.Utc),
        Close = new DateTime(2026, 11, 6, 1, 0, 0, DateTimeKind.Utc)
    };
}
