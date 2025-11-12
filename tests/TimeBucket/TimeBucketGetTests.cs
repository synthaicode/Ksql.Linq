using System;

using System.Collections.Concurrent;

using System.Collections.Generic;

using System.Linq;

using System.Runtime.Serialization;

using System.Threading;

using System.Threading.Tasks;

using Ksql.Linq;

using Ksql.Linq.Cache.Core;

using Ksql.Linq.Cache.Extensions;

using Ksql.Linq.Configuration;

using Ksql.Linq.Core.Abstractions;

using Ksql.Linq.Core.Attributes;

using Ksql.Linq.Core.Modeling;

using Ksql.Linq.Mapping;

using Ksql.Linq.Query.Abstractions;

using Ksql.Linq.Runtime;

using Microsoft.Extensions.Logging;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;



namespace Ksql.Linq.Tests.TimeBucket;



public class TimeBucketGetTests

{

    private static readonly Period OneMinute = Period.Minutes(1);



    [KsqlTopic("tb_unit" )]

    private class TestBar

    {

        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;

        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;

        public decimal Close { get; set; }

    }



    private class TestBarRead : TestBar

    {

        public DateTime BucketStart { get; set; }

    }



    private sealed class RecordingTableCache<T> : ITableCache<T> where T : class

    {

        private readonly Queue<List<T>> _responses;



        public RecordingTableCache(params List<T>[] responses)

        {

            _responses = new Queue<List<T>>(responses.Length > 0 ? responses : new[] { new List<T>() });

        }



        public List<IReadOnlyList<string>?> ObservedFilters { get; } = new();



        public Task<List<T>> ToListAsync(List<string>? filter, TimeSpan? timeout)

        {

            ObservedFilters.Add(filter is null ? null : new List<string>(filter));

            var next = _responses.Count > 0 ? _responses.Dequeue() : new List<T>();

            return Task.FromResult(next);

        }



        public void Dispose()

        {

        }

    }



    private sealed class FakeContext : KsqlContext

    {

        private FakeContext() : base(new KsqlDslOptions()) { }

    }



    private static FakeContext CreateContext(TableCacheRegistry registry)

    {

        #pragma warning disable SYSLIB0050
        var ctx = (FakeContext)FormatterServices.GetUninitializedObject(typeof(FakeContext));
        #pragma warning restore SYSLIB0050



        static void SetField(object target, string name, object value)

        {

            var field = typeof(KsqlContext).GetField(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

            Assert.NotNull(field);

            field!.SetValue(target, value);

        }



        var entityModel = new EntityModel

        {

            EntityType = typeof(TestBar),

            TopicName = "tb_unit",

            KeyProperties = new[]

            {

                typeof(TestBar).GetProperty(nameof(TestBar.Broker))!,

                typeof(TestBar).GetProperty(nameof(TestBar.Symbol))!

            },

            AllProperties = typeof(TestBar).GetProperties(),

            AccessMode = EntityAccessMode.ReadOnly

        };

        entityModel.SetStreamTableType(StreamTableType.Table);

        entityModel.AdditionalSettings["timeframe"] = "1m";

        var entityModels = new ConcurrentDictionary<Type, EntityModel>();

        entityModels[typeof(TestBar)] = entityModel;



        SetField(ctx, "_entityModels", entityModels);

        SetField(ctx, "_entitySets", new Dictionary<Type, object>());

        SetField(ctx, "_resolvedConfigs", new Dictionary<Type, Ksql.Linq.Configuration.ResolvedEntityConfig>());

        SetField(ctx, "_mappingRegistry", new MappingRegistry());

        SetField(ctx, "_dslOptions", new KsqlDslOptions());

        SetField(ctx, "_loggerFactory", NullLoggerFactory.Instance);

        SetField(ctx, "_logger", NullLoggerFactory.Instance.CreateLogger<KsqlContext>());

        SetField(ctx, "_cacheRegistry", registry);



        ctx.AttachTableCacheRegistry(registry);

        return ctx;

    }



    [Fact]

    public async Task ToListAsync_UsesRegisteredTableCache()

    {

        var row = new TestBarRead { Broker = "B", Symbol = "S", Close = 101m, BucketStart = DateTime.UnixEpoch };

        var cache = new RecordingTableCache<TestBarRead>(new List<TestBarRead> { row });

        var registry = new TableCacheRegistry();

        registry.Register(typeof(TestBarRead), cache);



        var ctx = CreateContext(registry);

        TimeBucketTypes.RegisterRead(typeof(TestBar), OneMinute, typeof(TestBarRead));



        var bucket = Ksql.Linq.Runtime.TimeBucket.Get<TestBar>(ctx, OneMinute);

        var result = await bucket.ToListAsync(new[] { "B", "S" }, CancellationToken.None);



        Assert.Single(result);

        Assert.Equal("B", result[0].Broker);

        Assert.Equal("S", result[0].Symbol);

        Assert.Equal(101m, result[0].Close);



        var observed = Assert.Single(cache.ObservedFilters);

        Assert.Equal(new[] { "B", "S" }, observed);

    }



    [Fact]

    public async Task ToListAsync_RetriesUntilCachePopulated()

    {

        var delayed = new RecordingTableCache<TestBarRead>(

            new List<TestBarRead>(),

            new List<TestBarRead> { new TestBarRead { Broker = "B", Symbol = "S", Close = 77m, BucketStart = DateTime.UnixEpoch } });

        var registry = new TableCacheRegistry();

        registry.Register(typeof(TestBarRead), delayed);



        var ctx = CreateContext(registry);

        TimeBucketTypes.RegisterRead(typeof(TestBar), OneMinute, typeof(TestBarRead));



        var bucket = Ksql.Linq.Runtime.TimeBucket.Get<TestBar>(ctx, OneMinute);

        var result = await bucket.ToListAsync(new[] { "B", "S" }, CancellationToken.None);



        Assert.Single(result);

        Assert.Equal(2, delayed.ObservedFilters.Count);

        foreach (var filter in delayed.ObservedFilters)

        {

            Assert.Equal(new[] { "B", "S" }, filter);

        }

    }

    [Fact]
    public void LiveTopicName_Follows_LiveConvention()
    {
        var registry = new TableCacheRegistry();
        var ctx = CreateContext(registry);
        var bucket = Ksql.Linq.Runtime.TimeBucket.Get<TestBar>(ctx, OneMinute);
        Assert.Equal("tb_unit_1m_live", bucket.LiveTopicName);
    }
    [Fact]

    public async Task ToListAsync_AllowsNullFilter()

    {

        var row = new TestBarRead { Broker = "B", Symbol = "S", Close = 42m, BucketStart = DateTime.UnixEpoch };

        var cache = new RecordingTableCache<TestBarRead>(new List<TestBarRead> { row });

        var registry = new TableCacheRegistry();

        registry.Register(typeof(TestBarRead), cache);



        var ctx = CreateContext(registry);

        TimeBucketTypes.RegisterRead(typeof(TestBar), OneMinute, typeof(TestBarRead));



        var bucket = Ksql.Linq.Runtime.TimeBucket.Get<TestBar>(ctx, OneMinute);

        var result = await bucket.ToListAsync(CancellationToken.None);



        Assert.Single(result);

        Assert.Equal("B", result[0].Broker);

        Assert.Equal("S", result[0].Symbol);



        var observed = Assert.Single(cache.ObservedFilters);

        Assert.Null(observed);

    }

}

