using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Ksql.Linq.Tests.Integration;

public class ForEachAsyncErrorHandlingTests
{
    private class DummyContext : KsqlContext
    {
        public DummyContext() : base(new Ksql.Linq.Configuration.KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    private class TestEntity { public int Id { get; set; } }

    private class FaultySet : EventSet<TestEntity>
    {
        private readonly List<TestEntity> _items;
        public FaultySet(List<TestEntity> items, EntityModel model) : base(new DummyContext(), model)
        {
            _items = items;
        }

        protected override Task SendEntityAsync(TestEntity entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
        public override async Task ForEachAsync(Func<TestEntity, Dictionary<string, string>, Ksql.Linq.Messaging.MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout != default && timeout != TimeSpan.Zero) cts.CancelAfter(timeout);
            int index = -1;
            bool thrown = false;
            long offset = 0;
            while (true)
            {
                index++;
                if (!thrown && index == 1)
                {
                    thrown = true;
                    // simulate enumerator exception on second fetch and skip
                    try { throw new InvalidOperationException("fail"); } catch { continue; }
                }
                if (index >= _items.Count)
                    break;
                var entity = _items[index];
                var headers = new Dictionary<string, string>();
                var meta = new Ksql.Linq.Messaging.MessageMeta("faulty", 0, offset++, DateTimeOffset.UtcNow, null, null, false, headers);
                await action(entity, headers, meta);
                await Task.Yield();
            }
        }
    }

    private static EntityModel CreateModel()
    {
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        return builder.GetEntityModel<TestEntity>()!;
    }

    [Fact(Skip = "Excluded from physicalTests: covered by UT (ForEachAsync error handling)")]
    public async Task EnumeratorException_IsSkipped()
    {
        var items = new List<TestEntity>
        {
            new TestEntity{ Id = 1 },
            new TestEntity{ Id = 2 },
            new TestEntity{ Id = 3 }
        };
        var set = new FaultySet(items, CreateModel());
        var results = new List<int>();

        await set.ForEachAsync((e, _, _) =>
        {
            results.Add(e.Id);
            return Task.CompletedTask;
        });

        Assert.Equal(new[] { 1, 3 }, results);
    }
}





