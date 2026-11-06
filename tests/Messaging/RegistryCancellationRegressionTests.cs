using Confluent.SchemaRegistry;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Configuration;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Mapping;
using Ksql.Linq.Messaging.Producers;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Messaging;

[Trait("Level", "L3")]
public class RegistryCancellationRegressionTests
{
    [Theory]
    [InlineData("normal", false)]
    [InlineData("recovery", false)]
    [InlineData("delete", false)]
    [InlineData("normal", true)]
    [InlineData("recovery", true)]
    [InlineData("delete", true)]
    public async Task PendingRegistry_CancelsCallerWithoutLaterLookupOrSend(string path, bool delayValue)
    {
        using var fixture = new Fixture(path, delayValue: delayValue);
        using var cancellation = new CancellationTokenSource();
        // Run on a worker so the original synchronous Registry wait cannot hang the test thread.
        var operation = Task.Run(() => fixture.Invoke(cancellation.Token));
        try
        {
            await fixture.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var before = fixture.Mapping.AvroValueRecordSchema;
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation.WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.False(fixture.Release.Task.IsCompleted); // underlying third-party request is still alive
            Assert.Equal(path == "recovery" ? 1 : 0, fixture.Sends);
            Assert.Equal((path == "recovery" ? 1 : 0) + (delayValue ? 1 : 0), fixture.ValueLookups);
            fixture.Release.TrySetResult(new RegisteredSchema("cancel-value", 2, 2,
                "{\"type\":\"record\",\"name\":\"ChangedAfterCancellation\",\"fields\":[{\"name\":\"Id\",\"type\":\"int\"}]}", SchemaType.Avro, new List<SchemaReference>()));
            // Drain continuations after the underlying request finishes; cancelled callers must not mutate mapping.
            await Task.Delay(50);
            Assert.Same(before, fixture.Mapping.AvroValueRecordSchema);
        }
        finally
        {
            fixture.Release.TrySetResult(null!);
            try { await operation.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
        }
        Assert.Equal(path == "recovery" ? 1 : 0, fixture.Sends);
        Assert.Equal((path == "recovery" ? 1 : 0) + (delayValue ? 1 : 0), fixture.ValueLookups);
    }

    [Theory]
    [InlineData("normal")]
    [InlineData("delete")]
    public async Task PreCancelled_DoesNotAcquireProducerOrCallRegistry(string path)
    {
        using var fixture = new Fixture(path, delay: false, installProducer: false);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Invoke(cancellation.Token).WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(0, fixture.KeyLookups);
        Assert.Equal(0, fixture.ProducerCount);
    }

    [Theory]
    [InlineData("normal", 1)]
    [InlineData("recovery", 2)]
    [InlineData("delete", 1)]
    public async Task NoDelay_PreservesNormalAndRecoveryBehavior(string path, int sends)
    {
        using var fixture = new Fixture(path, delay: false);
        await fixture.Invoke(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(sends, fixture.Sends);
        Assert.Equal(sends, fixture.ValueLookups);
    }

    private sealed class Fixture : IDisposable
    {
        public readonly TaskCompletionSource<bool> Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource<RegisteredSchema> Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Sends, ValueLookups, KeyLookups;
        public readonly KeyValueTypeMapping Mapping;
        private readonly string _path;
        private readonly KafkaProducerManager _manager;
        private readonly ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder> _default;
        private readonly ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder> _topics;
        public int ProducerCount => _default.Count + _topics.Count;

        public Fixture(string path, bool delay = true, bool installProducer = true, bool delayValue = false)
        {
            _path = path;
            const string topic = "cancel";
            var registry = new MappingRegistry();
            var mapping = registry.Register(typeof(Sample), Array.Empty<PropertyMeta>(),
                new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) }, topicName: topic, genericValue: true);
            Mapping = mapping;
            _manager = new KafkaProducerManager(registry, Options.Create(new KsqlDslOptions { SchemaRegistry = new SchemaRegistrySection { Url = "http://unused.invalid" } }));
            var client = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);
            client.Setup(c => c.Dispose());
            client.Setup(c => c.GetLatestSchemaAsync(topic + "-key")).Returns(() =>
            {
                var count = Interlocked.Increment(ref KeyLookups);
                if (delay && !delayValue && count == (path == "recovery" ? 2 : 1))
                {
                    Entered.TrySetResult(true);
                    return Release.Task;
                }
                return Task.FromException<RegisteredSchema>(new SchemaRegistryException("Subject not found", HttpStatusCode.NotFound, 40401));
            });
            client.Setup(c => c.GetLatestSchemaAsync(topic + "-value")).Returns(() =>
            {
                var count = Interlocked.Increment(ref ValueLookups);
                if (delay && delayValue && count == (path == "recovery" ? 2 : 1))
                {
                    Entered.TrySetResult(true);
                    return Release.Task;
                }
                return Task.FromResult(new RegisteredSchema(topic + "-value", 1, 1, mapping.AvroValueSchema!, SchemaType.Avro, new List<SchemaReference>()));
            });
            var field = typeof(KafkaProducerManager).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
            ((ISchemaRegistryClient)field.GetValue(_manager)!).Dispose();
            field.SetValue(_manager, client.Object);
            _default = (ConcurrentDictionary<Type, KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager).GetField("_producers", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(_manager)!;
            _topics = (ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager).GetField("_topicProducers", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(_manager)!;
            if (installProducer)
            {
                var holder = new KafkaProducerManager.ProducerHolder(topic, (key, value, context, token) =>
                    Interlocked.Increment(ref Sends) == 1 && path == "recovery"
                        ? Task.FromException(new SchemaRegistryException("NAME_MISMATCH", HttpStatusCode.Conflict, 409)) : Task.CompletedTask,
                    _ => { }, () => { }, isValueOnly: true);
                if (path == "delete") _default[typeof(Sample)] = holder;
                else _topics[(typeof(Sample), topic)] = holder;
            }
        }

        public Task Invoke(CancellationToken token) => _path == "delete"
            ? _manager.DeleteAsync(new Sample { Id = 7 }, token)
            : _manager.SendAsync("cancel", new Sample { Id = 7 }, cancellationToken: token);
        public void Dispose() => _manager.Dispose();
    }

    public class Sample { public int Id { get; set; } }
}
