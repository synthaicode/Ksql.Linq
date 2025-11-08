using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Messaging;
using Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Ksql.Linq.Tests.Integration;

public class DlqStreamRestrictionsTests
{
    private class DummyContext : KsqlContext
    {
        public DummyContext() : base(new Ksql.Linq.Configuration.KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private class DlqSet : EventSet<DlqEnvelope>
    {
        public DlqSet(EntityModel model) : base(new DummyContext(), model) { }

        protected override Task SendEntityAsync(DlqEnvelope entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<DlqEnvelope> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
        public override async Task ForEachAsync(Func<DlqEnvelope, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        {
            // No-op stream validates that method is callable without Kafka context
            await Task.CompletedTask;
        }
        protected override async IAsyncEnumerable<(DlqEnvelope Entity, Dictionary<string, string> Headers, Ksql.Linq.Messaging.MessageMeta Meta)> ConsumeAsync(
            KsqlContext context, bool autoCommit, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static EntityModel CreateModel()
    {
        var model = new EntityModel
        {
            EntityType = typeof(DlqEnvelope),
            TopicName = "dlq",
            AllProperties = typeof(DlqEnvelope).GetProperties(),
            KeyProperties = Array.Empty<PropertyInfo>()
        };
        model.SetStreamTableType(StreamTableType.Stream);
        return model;
    }

    [Fact(Skip = "Excluded from physicalTests: DLQ/ForEach restrictions covered by UT/integration DLQ tests")]
    public async Task DlqStream_ForEachAsync_Allows()
    {
        var set = new DlqSet(CreateModel());
        await set.ForEachAsync(_ => Task.CompletedTask);
    }

    [Fact(Skip = "Excluded from physicalTests: DLQ ToList behavior covered by UT")]
    public async Task DlqStream_ToListAsync_Throws()
    {
        var set = new DlqSet(CreateModel());
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => set.ToListAsync());
        Assert.Contains("DLQ", ex.Message);
    }

}

