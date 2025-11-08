using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Ksql.Linq.Tests.Integration;

public class EventSetMapTests
{
    private class DummyContext : KsqlContext
    {
        public DummyContext() : base(new Ksql.Linq.Configuration.KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    [KsqlTopic("sample-topic")]
    [KsqlTable]
    private class Sample
    {
        [KsqlKey(Order = 0)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class SampleDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class SampleSet : EventSet<Sample>
    {
        private readonly List<Sample> _items;

        public SampleSet(List<Sample> items, EntityModel model) : base(new DummyContext(), model)
        {
            _items = items;
        }

        protected override Task SendEntityAsync(Sample entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<Sample> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            // Not used by current pipeline, keep as empty to satisfy abstract contract
            await Task.CompletedTask;
            yield break;
        }
        public override async Task ForEachAsync(Func<Sample, Dictionary<string, string>, Ksql.Linq.Messaging.MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout != default && timeout != TimeSpan.Zero) cts.CancelAfter(timeout);
            long offset = 0;
            foreach (var item in _items)
            {
                if (cts.IsCancellationRequested) break;
                var headers = new Dictionary<string, string>();
                var meta = new Ksql.Linq.Messaging.MessageMeta("sample-topic", 0, offset++, DateTimeOffset.UtcNow, null, null, false, headers);
                await action(item, headers, meta);
            }
        }
        protected override async IAsyncEnumerable<(Sample Entity, Dictionary<string, string> Headers, Ksql.Linq.Messaging.MessageMeta Meta)> ConsumeAsync(
            KsqlContext context, bool autoCommit, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            long offset = 0;
            foreach (var item in _items)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;
                var headers = new Dictionary<string, string>();
                var meta = new Ksql.Linq.Messaging.MessageMeta(
                    "sample-topic",
                    0,
                    offset++,
                    DateTimeOffset.UtcNow,
                    null,
                    null,
                    false,
                    headers);
                yield return (item, headers, meta);
                await Task.Yield();
            }
        }
    }

    private static EntityModel CreateModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<Sample>();
        return builder.GetEntityModel<Sample>()!;
    }

    [Fact(Skip = "Excluded from physicalTests: mapping behavior validated in UT")]
    public async Task Map_ForEachAsync_ReturnsMappedValues()
    {
        var items = new List<Sample> { new Sample { Id = 1, Name = "A" } };
        var set = new SampleSet(items, CreateModel());

        var mapped = set.Map(x => new SampleDto { Id = x.Id, Name = x.Name });

        var results = new List<SampleDto>();
        await mapped.ForEachAsync((dto, _, _) =>
        {
            results.Add(dto);
            return Task.CompletedTask;
        });

        var resultDto = Assert.Single(results);
        Assert.Equal(1, resultDto.Id);
        Assert.Equal("A", resultDto.Name);
    }
}

