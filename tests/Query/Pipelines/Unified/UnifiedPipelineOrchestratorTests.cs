using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Mapping;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Metadata;
using Ksql.Linq.Query.Pipelines.Unified;
using Ksql.Linq.Query.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ksql.Linq.Tests.Query.Pipelines.Unified;

public class UnifiedPipelineOrchestratorTests
{
    [Fact]
    public async Task ExecuteAsync_RunsStagesInOrder()
    {
        var (request, baseModel, statements) = CreateRequest();

        var order = new List<string>();
        UnifiedPipelineContext? capturedContext = null;

        var stage1 = new DelegateStage("stage1", (ctx, _) =>
        {
            order.Add("stage1");
            capturedContext = ctx;
            return Task.CompletedTask;
        });
        var stage2 = new DelegateStage("stage2", (ctx, _) =>
        {
            order.Add("stage2");
            Assert.Same(capturedContext, ctx);
            return Task.CompletedTask;
        });

        var orchestrator = new UnifiedPipelineOrchestrator(new IUnifiedPipelineStage[] { stage1, stage2 });
        var context = await orchestrator.ExecuteAsync(request);

        Assert.Equal(new[] { "stage1", "stage2" }, order);
        Assert.NotNull(context);
        Assert.True(context.Executions.Count > 0);
        Assert.True(statements.Count > 0);
        Assert.Same(baseModel, context.BaseModel);
    }

    [Fact]
    public async Task ExecuteAsync_WrapsStageExceptions()
    {
        var (request, _, _) = CreateRequest();
        var orchestrator = new UnifiedPipelineOrchestrator(new IUnifiedPipelineStage[]
        {
            new ThrowStage()
        });

        var ex = await Assert.ThrowsAsync<UnifiedPipelineException>(() => orchestrator.ExecuteAsync(request));
        Assert.NotNull(ex.Context);
        Assert.True(ex.Context.Executions.Count > 0);
        Assert.IsType<InvalidOperationException>(ex.InnerException);
    }

    private static (UnifiedPipelineRequest Request, EntityModel BaseModel, List<string> Statements) CreateRequest()
    {
        var sourceModel = new EntityModel
        {
            EntityType = typeof(Quote),
            TopicName = "quotes_src",
            AllProperties = typeof(Quote).GetProperties(),
            KeyProperties = new[]
            {
                typeof(Quote).GetProperty(nameof(Quote.Broker))!,
                typeof(Quote).GetProperty(nameof(Quote.Symbol))!
            },
            QueryMetadata = new QueryMetadata()
        };

        var baseModel = new EntityModel
        {
            EntityType = typeof(Bar1m),
            TopicName = "bar_1m_live",
            AllProperties = typeof(Bar1m).GetProperties(),
            KeyProperties = new[]
            {
                typeof(Bar1m).GetProperty(nameof(Bar1m.Broker))!,
                typeof(Bar1m).GetProperty(nameof(Bar1m.Symbol))!,
                typeof(Bar1m).GetProperty(nameof(Bar1m.BucketStart))!
            }
        };
        baseModel.SetStreamTableType(Ksql.Linq.Query.Abstractions.StreamTableType.Table);
        baseModel.QueryModel = new KsqlQueryRoot()
            .From<Quote>()
            .Tumbling(q => q.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(q => new { q.Broker, q.Symbol })
            .Select(g => new Bar1m
            {
                Broker = g.Key.Broker,
                Symbol = g.Key.Symbol,
                BucketStart = g.WindowStart(),
                Close = g.LatestByOffset(x => x.Bid)
            })
            .Build();

        var statements = new List<string>();
        var entityModels = new ConcurrentDictionary<Type, EntityModel>
        {
            [typeof(Quote)] = sourceModel,
            [typeof(Bar1m)] = baseModel
        };

        var request = new UnifiedPipelineRequest(
            baseModel.EntityType,
            baseModel,
            baseModel.QueryModel!,
            new MappingRegistry(),
            entityModels,
            NullLogger.Instance,
            (model, sql) =>
            {
                statements.Add(sql);
                return Task.FromResult(new KsqlDbResponse(true, string.Empty));
            },
            null);

        return (request, baseModel, statements);
    }

    private sealed class DelegateStage : IUnifiedPipelineStage
    {
        private readonly string _name;
        private readonly Func<UnifiedPipelineContext, UnifiedPipelineRequest, Task> _action;

        public DelegateStage(string name, Func<UnifiedPipelineContext, UnifiedPipelineRequest, Task> action)
        {
            _name = name;
            _action = action;
        }

        public Task ExecuteAsync(UnifiedPipelineContext context, UnifiedPipelineRequest request)
            => _action(context, request);

        public override string ToString() => _name;
    }

    private sealed class ThrowStage : IUnifiedPipelineStage
    {
        public Task ExecuteAsync(UnifiedPipelineContext context, UnifiedPipelineRequest request)
            => Task.FromException(new InvalidOperationException("boom"));
    }

    [KsqlTopic("quotes_src")]
    private class Quote
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    [KsqlTopic("bar_1m_live")]
    private class Bar1m
    {
        [KsqlKey(1)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(2)] public string Symbol { get; set; } = string.Empty;
        [KsqlTimestamp] public DateTime BucketStart { get; set; }
        public double Close { get; set; }
    }
}