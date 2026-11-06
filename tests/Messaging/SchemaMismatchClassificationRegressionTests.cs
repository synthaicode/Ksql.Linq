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
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Messaging;

[Trait("Level", "L3")]
public class SchemaMismatchClassificationRegressionTests
{
    public static IEnumerable<object[]> Markers()
    {
        foreach (var marker in new[] { "NAME_MISMATCH", "incompatible", "Schema being registered is incompatible", "unknown magic byte", "subject not found", "error code: 409" })
            yield return new object[] { marker };
    }

    private static SchemaRegistryException SchemaError(string message, int errorCode = 403) =>
        new SchemaRegistryException(message, HttpStatusCode.Conflict, errorCode);

    private static bool Classify(Exception error)
    {
        using var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(new KsqlDslOptions()));
        return (bool)typeof(KafkaProducerManager).GetMethod("IsSchemaMismatch", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(manager, new object[] { error })!;
    }

    [Theory]
    [MemberData(nameof(Markers))]
    public void NonSchemaException_WithSameMarker_DoesNotMatch(string marker)
    {
        Assert.False(Classify(new InvalidOperationException(marker)));
    }

    [Theory]
    [MemberData(nameof(Markers))]
    public void TypedMarker_MatchesDirectlyAndWhenWrapped(string marker)
    {
        Assert.True(Classify(SchemaError(marker.ToUpperInvariant())));
        Assert.True(Classify(new Exception("serialization failed", SchemaError(marker))));
    }

    [Theory]
    [MemberData(nameof(Markers))]
    public void WrapperMarker_DoesNotTaintUnrelatedTypedError(string marker)
    {
        Assert.False(Classify(new Exception(marker, SchemaError("permission denied"))));
    }

    [Fact]
    public void UnknownTypedError_WithoutExistingMismatchEvidence_DoesNotMatch()
    {
        Assert.False(Classify(SchemaError("permission denied")));
        Assert.False(Classify(SchemaError("registry unavailable", 500)));
    }

    [Fact]
    public void TypedConflictCode_PreservesExistingMismatchEvidence()
    {
        var error = SchemaError("opaque registry conflict", 409);
        Assert.Contains("error code: 409", error.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.True(Classify(error));
        Assert.True(Classify(new Exception("serialization failed", error)));
    }

    [Fact]
    public void Aggregate_SearchesEveryChildWithoutMixingMessages()
    {
        Assert.True(Classify(new AggregateException(new Exception("unrelated"), new Exception("wrapper", SchemaError("NAME_MISMATCH")))));
        Assert.False(Classify(new AggregateException("incompatible", new Exception("subject not found"), SchemaError("permission denied"))));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SendAsync_OnlyTypedMismatchLooksUpRecoverySchemasAndResends(bool typed)
    {
        const string topic = "classification";
        var registry = new MappingRegistry();
        var mapping = registry.Register(typeof(Sample), Array.Empty<PropertyMeta>(),
            new[] { PropertyMeta.FromProperty(typeof(Sample).GetProperty(nameof(Sample.Id))!) },
            topicName: topic, genericValue: true);
        var options = new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection { Url = "http://unused.invalid" }
        };
        using var manager = new KafkaProducerManager(registry, Options.Create(options));
        var schemaClient = new Mock<ISchemaRegistryClient>(MockBehavior.Strict);
        schemaClient.Setup(c => c.Dispose());
        schemaClient.Setup(c => c.GetLatestSchemaAsync(topic + "-key"))
            .ThrowsAsync(new SchemaRegistryException("Subject not found", HttpStatusCode.NotFound, 40401));
        schemaClient.Setup(c => c.GetLatestSchemaAsync(topic + "-value"))
            .ReturnsAsync(new RegisteredSchema(topic + "-value", 1, 1, mapping.AvroValueSchema!, SchemaType.Avro, new List<SchemaReference>()));
        var clientField = typeof(KafkaProducerManager).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
        ((ISchemaRegistryClient)clientField.GetValue(manager)!).Dispose();
        clientField.SetValue(manager, schemaClient.Object);
        Exception original = typed ? SchemaError("NAME_MISMATCH") : new InvalidOperationException("NAME_MISMATCH");
        var sends = 0;
        var holder = new KafkaProducerManager.ProducerHolder(topic,
            (key, value, context, cancellation) => ++sends == 1 ? Task.FromException(original) : Task.CompletedTask,
            _ => { }, () => { }, isValueOnly: true);
        var holders = (ConcurrentDictionary<(Type, string), KafkaProducerManager.ProducerHolder>)typeof(KafkaProducerManager)
            .GetField("_topicProducers", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(manager)!;
        holders[(typeof(Sample), topic)] = holder;

        if (typed)
            await manager.SendAsync(topic, new Sample { Id = 7 });
        else
            Assert.Same(original, await Assert.ThrowsAsync<InvalidOperationException>(() => manager.SendAsync(topic, new Sample { Id = 7 })));

        Assert.Equal(typed ? 2 : 1, sends);
        // Both paths perform two normal pre-send lookups; only the typed mismatch adds two recovery lookups.
        schemaClient.Verify(c => c.GetLatestSchemaAsync(It.IsAny<string>()), Times.Exactly(typed ? 4 : 2));
    }

    public class Sample
    {
        public int Id { get; set; }
    }
}
