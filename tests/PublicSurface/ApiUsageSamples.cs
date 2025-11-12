using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Application;
using Ksql.Linq.Events;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Infrastructure.KsqlDb;
using Ksql.Linq.Messaging;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.PublicSurface;

// This file is a compile-time probe that references the public API
// used in examples and wiki. If a symbol becomes non-public or changes
// signature, this file will fail to compile, flagging the drift.
public class ApiUsageSamples
{
    [Fact]
    public void Compile_time_probe_for_examples_and_wiki()
    {
        // KsqlContextBuilder + Options
        var builder = KsqlContextBuilder.Create()
            .ConfigureValidation(autoRegister: false, failOnErrors: false, enablePreWarming: false)
            .WithTimeouts(TimeSpan.FromSeconds(10));

        // KsqlDbResponse record properties
        var resp = new KsqlDbResponse(true, "ok");
        bool s = resp.IsSuccess; string m = resp.Message; int? code = resp.ErrorCode; string? detail = resp.ErrorDetail;
        Assert.True(s && m != null);

        // MessageMeta usage
        var meta = new MessageMeta { Topic = "t", Partition = 0, Offset = 1, TimestampUtc = DateTime.UtcNow };
        _ = meta.Topic; _ = meta.Partition; _ = meta.Offset; _ = meta.TimestampUtc;

        // QueryExtensions.ToQuery presence (signature only, no execution)
        var toQuery = typeof(QueryExtensions).GetMethods()
            .FirstOrDefault(m => m.Name == "ToQuery");
        Assert.NotNull(toQuery);

        // EventSet.ForEachAsync presence (signature only)
        var forEachAsync = typeof(EventSet<>).MakeGenericType(typeof(Dummy)).GetMethods()
            .FirstOrDefault(m => m.Name == "ForEachAsync");
        Assert.NotNull(forEachAsync);

        // WindowExtensions.WindowStart presence (signature only)
        var windowStart = typeof(WindowExtensions).GetMethods()
            .FirstOrDefault(m => m.Name == "WindowStart");
        Assert.NotNull(windowStart);

        // TimeBucket facade ReadAsync
        CancellationToken ct = default;
        var readAsync = typeof(global::Ksql.Linq.TimeBucket).GetMethods()
            .FirstOrDefault(m => m.Name == "ReadAsync");
        Assert.NotNull(readAsync);

        // IKsqlExecutor + IKsqlDbClient interfaces
        IKsqlDbClient client = new StubClient();
        IKsqlExecutor exec = new Ksql.Linq.Infrastructure.Ksql.KsqlExecutor(client);
        _ = exec.ExecuteStatementAsync("SHOW STREAMS;", ct);

        // Runtime events (sink interface)
        IRuntimeEventSink? sink = null;
        RuntimeEventBus.SetSink(sink);
    }

    private sealed class MinimalContext : KsqlContext
    {
        public MinimalContext(Microsoft.Extensions.Configuration.IConfiguration cfg) : base(cfg, loggerFactory: null!) { }
        protected override void OnModelCreating(Ksql.Linq.Core.Abstractions.IModelBuilder modelBuilder) { }
    }

    private sealed class Dummy { }

    private sealed class StubClient : IKsqlDbClient, IDisposable
    {
        public Task<KsqlDbResponse> ExecuteStatementAsync(string statement) => Task.FromResult(new KsqlDbResponse(true, "ok"));
        public Task<KsqlDbResponse> ExecuteExplainAsync(string ksql) => ExecuteStatementAsync("EXPLAIN");
        public Task<HashSet<string>> GetTableTopicsAsync() => Task.FromResult(new HashSet<string>());
        public Task<HashSet<string>> GetStreamTopicsAsync() => Task.FromResult(new HashSet<string>());
        public Task<int> ExecuteQueryStreamCountAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(0);
        public Task<int> ExecutePullQueryCountAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(0);
        public Task<List<object?[]>> ExecutePullQueryRowsAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(new List<object?[]>());
        public Task<List<object?[]>> ExecuteQueryStreamRowsAsync(string sql, TimeSpan? timeout = null) => Task.FromResult(new List<object?[]>());
        public void Dispose() { }
    }
}
