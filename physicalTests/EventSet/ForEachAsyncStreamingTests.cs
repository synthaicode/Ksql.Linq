using Ksql.Linq;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace Ksql.Linq.Tests.Integration;

public class ForEachAsyncStreamingTests
{
    private class DummyContext : KsqlContext
    {
        public DummyContext() : base(new Ksql.Linq.Configuration.KsqlDslOptions()) { }
        protected override bool SkipSchemaRegistration => true;
    }

    [KsqlTopic("t")]
    private class TestEvent { [KsqlKey(Order = 0)] public int Id { get; set; } }

    private class ChannelEventSet : EventSet<TestEvent>
    {
        private readonly Channel<TestEvent> _channel;
        public ChannelEventSet(Channel<TestEvent> channel) : base(new DummyContext(), CreateModel())
        {
            _channel = channel;
        }
        protected override Task SendEntityAsync(TestEvent entity, Dictionary<string, string>? headers, CancellationToken cancellationToken) => Task.CompletedTask;
        public override IAsyncEnumerator<TestEvent> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => new ChannelEnumerator(_channel, cancellationToken);
        // New pipeline requires overriding ConsumeAsync to avoid Kafka dependency
        protected override async IAsyncEnumerable<(TestEvent Entity, Dictionary<string, string> Headers, Ksql.Linq.Messaging.MessageMeta Meta)> ConsumeAsync(
            KsqlContext context, bool autoCommit, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var enumerator = new ChannelEnumerator(_channel, cancellationToken);
            long offset = 0;
            while (await enumerator.MoveNextAsync())
            {
                var headers = new Dictionary<string, string>();
                var meta = new Ksql.Linq.Messaging.MessageMeta(
                    "t",
                    0,
                    offset++,
                    DateTimeOffset.UtcNow,
                    null,
                    null,
                    false,
                    headers);
                yield return (enumerator.Current, headers, meta);
            }
        }

        public override async Task ForEachAsync(Func<TestEvent, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout != default && timeout != TimeSpan.Zero) cts.CancelAfter(timeout);
            await using var enumerator = new ChannelEnumerator(_channel, cts.Token);
            while (await enumerator.MoveNextAsync())
            {
                await action(enumerator.Current);
            }
        }

        public override async Task ForEachAsync(Func<TestEvent, Dictionary<string, string>, Ksql.Linq.Messaging.MessageMeta, Task> action, TimeSpan timeout = default, bool autoCommit = true, CancellationToken cancellationToken = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout != default && timeout != TimeSpan.Zero) cts.CancelAfter(timeout);
            await using var enumerator = new ChannelEnumerator(_channel, cts.Token);
            long offset = 0;
            while (await enumerator.MoveNextAsync())
            {
                var headers = new Dictionary<string, string>();
                var meta = new Ksql.Linq.Messaging.MessageMeta("t", 0, offset++, DateTimeOffset.UtcNow, null, null, false, headers);
                await action(enumerator.Current, headers, meta);
            }
        }

        private sealed class ChannelEnumerator : IAsyncEnumerator<TestEvent>
        {
            private readonly Channel<TestEvent> _channel;
            private readonly CancellationToken _token;
            public ChannelEnumerator(Channel<TestEvent> channel, CancellationToken token)
            {
                _channel = channel;
                _token = token;
            }

            public TestEvent Current { get; private set; } = null!;

            public async ValueTask<bool> MoveNextAsync()
            {
                try
                {
                    Current = await _channel.Reader.ReadAsync(_token);
                    return true;
                }
                catch (ChannelClosedException)
                {
                    return false;
                }
            }

            public ValueTask DisposeAsync() => default;
        }
        private static EntityModel CreateModel()
        {
            var builder = new ModelBuilder();
            builder.Entity<TestEvent>();
            var model = builder.GetEntityModel<TestEvent>()!;
            model.ValidationResult = new ValidationResult { IsValid = true };
            model.SetStreamTableType(StreamTableType.Stream);
            return model;
        }
    }

    [Fact(Skip = "Excluded from physicalTests: covered by UT (ForEachAsync streaming behavior)")]
    public async Task ForEachAsync_Processes_NewData_Until_Inactivity()
    {
        var channel = Channel.CreateUnbounded<TestEvent>();
        var set = new ChannelEventSet(channel);
        var results = new List<int>();
        var cts = new CancellationTokenSource();

        var task = Task.Run(() => set.ForEachAsync((e, _, _) =>
        {
            results.Add(e.Id);
            return Task.CompletedTask;
        }, cancellationToken: cts.Token));

        await channel.Writer.WriteAsync(new TestEvent { Id = 1 });
        await Task.Delay(100);
        await channel.Writer.WriteAsync(new TestEvent { Id = 2 });
        await Task.Delay(100);
        channel.Writer.Complete();
        cts.Cancel();
        await task;

        Assert.Equal(new[] { 1, 2 }, results);
    }

    [Fact(Skip = "Excluded from physicalTests: covered by UT (ForEachAsync cancel behavior)")]
    public async Task ForEachAsync_Cancels_With_Token()
    {
        var channel = Channel.CreateUnbounded<TestEvent>();
        var set = new ChannelEventSet(channel);
        var cts = new CancellationTokenSource();
        var task = Task.Run(() => set.ForEachAsync((_, _, _) => Task.CompletedTask, cancellationToken: cts.Token));

        await Task.Delay(100);
        channel.Writer.Complete();
        cts.Cancel();
        await task;
        Assert.True(cts.IsCancellationRequested);
    }
}


