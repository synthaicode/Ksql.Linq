using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit;
using Ksql.Linq.Tests.Utils;

namespace Ksql.Linq.Tests.Wait;

[Trait("Level", TestLevel.L2)]
public class ShowQueriesMatchTests
{
    private static MethodInfo GetPrivateStatic(string name, params Type[] types)
    {
        var t = typeof(Ksql.Linq.KsqlContext);
        var m = t.GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static, binder: null, types: types, modifiers: null);
        Assert.NotNull(m);
        return m!;
    }

    [Fact]
    public void TryGetQueryStateFromJson_Running_ByStateField()
    {
        var json = @"[ { ""@type"":""queries"",""queries"":[{ ""id"":""CTAS_BAR_1M_LIVE_42"",""sinks"" : [""BAR_1M_LIVE""], ""state"":""RUNNING"" }] } ]";

        var method = GetPrivateStatic("TryGetQueryStateFromJson", typeof(string), typeof(string), typeof(string), typeof(string).MakeByRefType());
        var args = new object?[] { json, "bar_1m_live", null, null };
        var ok = (bool)method.Invoke(null, args)!;
        Assert.True(ok);
        Assert.Equal("RUNNING", (string?)args[3]);
    }

    [Fact]
    public void TryGetQueryStateFromJson_Running_ByStatusCount()
    {
        var json = @"[ { ""@type"":""queries"",""queries"":[{ ""id"":""CTAS_BAR_1M_LIVE_7"",""sinks"" : [""BAR_1M_LIVE""], ""statusCount"":{""RUNNING"":1} }] } ]";

        var method = GetPrivateStatic("TryGetQueryStateFromJson", typeof(string), typeof(string), typeof(string), typeof(string).MakeByRefType());
        var args = new object?[] { json, "bar_1m_live", null, null };
        var ok = (bool)method.Invoke(null, args)!;
        Assert.True(ok);
        Assert.Equal("RUNNING", (string?)args[3]);
    }

    [Fact]
    public void CheckQueryRunningInText_Matches_ByQueryIdOrTarget()
    {
        var method = GetPrivateStatic("CheckQueryRunningInText", typeof(IEnumerable<string>), typeof(string), typeof(string));
        var lines = new[]
        {
            "Header | QueryID | Sinks | State",
            "--- | CTAS_BAR_1M_LIVE_99 | BAR_1M_LIVE | RUNNING"
        };
        var okById = (bool)method.Invoke(null, new object?[] { lines, "BAR_1M_LIVE", "CTAS_BAR_1M_LIVE_99" })!;
        Assert.True(okById);

        var lines2 = new[]
        {
            "--- | Q | BAR_1M_LIVE | RUNNING:1"
        };
        var okByTarget = (bool)method.Invoke(null, new object?[] { lines2, "BAR_1M_LIVE", null })!;
        Assert.True(okByTarget);
    }
}