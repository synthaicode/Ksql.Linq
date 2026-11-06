using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Configuration;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using Ksql.Linq.Messaging.Producers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Messaging;

[Trait("Level", "L3")]
public class ProducerCacheOwnershipRegressionTests
{
    public class Sample { public string Name { get; set; } = ""; }
    public class OtherSample { public string Name { get; set; } = ""; }

    [Theory]
    [InlineData(null, null)]
    [InlineData("OwNeRsHiP", "ownership")]
    public Task ConcurrentFirstAcquisition_ReturnsOneOwnedHolder(string? firstTopic, string? secondTopic)
        => AssertConcurrentOwnership(firstTopic, secondTopic, upgrade: false);

    [Theory]
    [InlineData(null, null)]
    [InlineData("OwNeRsHiP", "ownership")]
    public Task ConcurrentKeyedUpgrade_DisposesOldHolderOnceAndPublishesOneReplacement(string? firstTopic, string? secondTopic)
        => AssertConcurrentOwnership(firstTopic, secondTopic, upgrade: true);

    private static async Task AssertConcurrentOwnership(string? firstTopic, string? secondTopic, bool upgrade)
    {
        using var firstCreating = new ManualResetEventSlim();
        using var secondCreating = new ManualResetEventSlim();
        using var secondStarted = new ManualResetEventSlim();
        int creations = 0;
        using var logger = new CallbackLogger(() =>
        {
            if (Interlocked.Increment(ref creations) == 1)
            {
                firstCreating.Set();
                Assert.True(secondStarted.Wait(TimeSpan.FromSeconds(10)), "Second acquisition did not start");
                // Baseline admits both factories. Serialized ownership admits only the first;
                // a bounded wait allows that implementation to complete without a deadlock.
                secondCreating.Wait(TimeSpan.FromSeconds(2));
            }
            else secondCreating.Set();
        });
        using var manager = Build(logger, keyed: upgrade);
        int oldDisposals = 0;
        if (upgrade)
        {
            var old = new KafkaProducerManager.ProducerHolder(firstTopic?.ToLowerInvariant() ?? "sample",
                (k, v, c, ct) => Task.CompletedTask, _ => { },
                () => Interlocked.Increment(ref oldDisposals), isValueOnly: true);
            if (firstTopic == null)
                ((ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
                    .GetField("_producers", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!)[typeof(Sample)] = old;
            else
                ((ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
                    .GetField("_topicProducers", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!)[(typeof(Sample), firstTopic.ToLowerInvariant())] = old;
        }
        Task<KafkaProducerManager.ProducerHolder>? first = null, second = null;
        try
        {
            first = AcquireOnThread<Sample>(manager, firstTopic);
            Assert.True(firstCreating.Wait(TimeSpan.FromSeconds(10)), "First factory did not reach config logging");
            second = AcquireOnThread<Sample>(manager, secondTopic, secondStarted.Set);
            var holders = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Same(holders[0], holders[1]);
            Assert.Equal(1, Volatile.Read(ref creations));
            Assert.Single(Cached(manager, firstTopic));
            Assert.Same(holders[0], Cached(manager, firstTopic).Single());
            if (upgrade)
            {
                Assert.False(holders[0].IsValueOnly);
                Assert.Equal(1, Volatile.Read(ref oldDisposals));
            }
        }
        finally
        {
            // Dispose losers too: the baseline manager can enumerate only the winning holder.
            if (first != null && second != null)
            {
                var holders = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(20));
                var owned = Cached(manager, firstTopic);
                foreach (var holder in holders.Distinct().Where(h => !owned.Contains(h))) holder.Dispose();
            }
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("OwNeRsHiP")]
    public async Task SequentialAcquisition_ReusesSameKeyAndSeparatesDifferentKeys(string? topic)
    {
        using var manager = Build();
        var first = await Acquire<Sample>(manager, topic);
        var same = await Acquire<Sample>(manager, topic?.ToLowerInvariant());
        var other = await Acquire<OtherSample>(manager, topic);
        Assert.Same(first, same);
        Assert.NotSame(first, other);
        Assert.Equal(2, Cached(manager, topic).Length);
        if (topic != null)
        {
            var otherTopic = await Acquire<Sample>(manager, "different-topic");
            Assert.NotSame(first, otherTopic);
            Assert.Equal(3, Cached(manager, topic).Length);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("ownership")]
    public async Task AcquisitionAfterDispose_RejectsWithoutPublishing(string? topic)
    {
        using var manager = Build();
        manager.Dispose();
        KafkaProducerManager.ProducerHolder? unexpected = null;
        try
        {
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => unexpected = await Acquire<Sample>(manager, topic));
            Assert.Empty(Cached(manager, topic));
        }
        finally { unexpected?.Dispose(); }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("ownership")]
    public async Task FailedCreation_CanBeRetried(string? topic)
    {
        int attempts = 0;
        using var logger = new CallbackLogger(() =>
        {
            if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("controlled creation failure");
        });
        using var manager = Build(logger);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Acquire<Sample>(manager, topic));
        Assert.Empty(Cached(manager, topic));
        var holder = await Acquire<Sample>(manager, topic);
        Assert.Same(holder, Assert.Single(Cached(manager, topic)));
        Assert.Equal(2, attempts);
    }

    private static KafkaProducerManager Build(ILoggerFactory? logger = null, bool keyed = false)
    {
        var mapping = new MappingRegistry();
        foreach (var type in new[] { typeof(Sample), typeof(OtherSample) })
            mapping.Register(type,
                keyed ? new[] { PropertyMeta.FromProperty(type.GetProperty(nameof(Sample.Name))!) } : Array.Empty<PropertyMeta>(),
                type.GetProperties().Select(p => PropertyMeta.FromProperty(p)).ToArray());
        return new KafkaProducerManager(mapping, Options.Create(new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:1", ClientId = "ownership-regression" },
            SchemaRegistry = new SchemaRegistrySection { Url = "" }
        }), logger);
    }

    private static Task<KafkaProducerManager.ProducerHolder> AcquireOnThread<T>(KafkaProducerManager manager, string? topic, Action? started = null) where T : class
        => Task.Factory.StartNew(() => { started?.Invoke(); return Acquire<T>(manager, topic); },
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private static Task<KafkaProducerManager.ProducerHolder> Acquire<T>(KafkaProducerManager manager, string? topic) where T : class
    {
        try
        {
            return (Task<KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
                .GetMethod("GetProducerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(typeof(T)).Invoke(manager, new object?[] { topic })!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            // Reflection can wrap both GetProducerAsync and CreateProducer.
            Exception cause = ex.InnerException;
            while (cause is TargetInvocationException nested && nested.InnerException != null) cause = nested.InnerException;
            return Task.FromException<KafkaProducerManager.ProducerHolder>(cause);
        }
    }

    private static KafkaProducerManager.ProducerHolder[] Cached(KafkaProducerManager manager, string? topic)
        => topic == null
            ? ((ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager).GetField("_producers", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!).Values.ToArray()
            : ((ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager).GetField("_topicProducers", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!).Values.ToArray();

    private sealed class CallbackLogger : ILoggerFactory, ILogger
    {
        private readonly Action _onProducerConfig;
        public CallbackLogger(Action onProducerConfig) => _onProducerConfig = onProducerConfig;
        public ILogger CreateLogger(string categoryName) => this;
        public void AddProvider(ILoggerProvider provider) { }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;
        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (formatter(state, exception).StartsWith("producer:", StringComparison.Ordinal)) _onProducerConfig();
        }
        public void Dispose() { }
    }
}
