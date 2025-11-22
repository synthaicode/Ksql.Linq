using System;
using System.Reflection;
using Ksql.Linq.Cache.Extensions;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Query.Abstractions;
using Xunit;

namespace Ksql.Linq.Tests.Cache;

public class UseTableCacheRegistrationTests
{
    private static MethodInfo GetShouldRegisterMethod()
    {
        var method = typeof(KsqlContextCacheExtensions)
            .GetMethod("ShouldRegisterForStreamizCache", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return method!;
    }

    private static bool InvokeShouldRegister(EntityModel model, bool isExplicit)
    {
        var method = GetShouldRegisterMethod();
        return (bool)method.Invoke(null, new object[] { model, isExplicit })!;
    }

    private static EntityModel CreateModel(StreamTableType type = StreamTableType.Table)
    {
        var model = new EntityModel
        {
            EntityType = typeof(UseTableCacheRegistrationTests),
            TopicName = "dummy"
        };
        model.SetStreamTableType(type);
        return model;
    }

    [Fact]
    public void TimeframeLiveRole_IsEligible()
    {
        var model = CreateModel();
        model.AdditionalSettings["timeframe"] = "1m";
        model.AdditionalSettings["role"] = "Live";

        Assert.True(InvokeShouldRegister(model, isExplicit: false));
    }

    [Fact]
    public void TimeframeFinalRole_IsNotEligible()
    {
        var model = CreateModel();
        model.AdditionalSettings["timeframe"] = "1m";
        model.AdditionalSettings["role"] = "Final";

        Assert.False(InvokeShouldRegister(model, isExplicit: false));
    }

    [Fact]
    public void NonTimeframeExplicitTrue_IsNotEligible()
    {
        var model = CreateModel();

        Assert.False(InvokeShouldRegister(model, isExplicit: true));
    }

    [Fact]
    public void NonTimeframeExplicitFalse_IsNotEligible()
    {
        var model = CreateModel();

        Assert.False(InvokeShouldRegister(model, isExplicit: false));
    }

    private static MethodInfo GetResolveWindowSizeMsMethod()
    {
        var method = typeof(KsqlContextCacheExtensions)
            .GetMethod("ResolveWindowSizeMs", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return method!;
    }

    private static long? InvokeResolveWindowSizeMs(EntityModel model)
    {
        var method = GetResolveWindowSizeMsMethod();
        return (long?)method.Invoke(null, new object[] { model });
    }

    [Fact]
    public void ResolveWindowSizeMs_ReturnsMilliseconds_ForHoppingMetadata()
    {
        var model = CreateModel();
        model.AdditionalSettings["timeframe"] = "5m";
        model.AdditionalSettings["role"] = "Live";

        var ms = InvokeResolveWindowSizeMs(model);

        Assert.Equal(300_000, ms);
    }
}
