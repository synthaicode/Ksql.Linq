using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace Ksql.Linq.Query.Pipelines.Unified;

/// <summary>
/// Coordinates derived pipeline planning, DDL generation and execution.
/// Consolidates the previous DerivedTumblingPipeline orchestration into a reusable component.
/// </summary>
internal sealed class UnifiedPipelineOrchestrator
{
    private readonly IReadOnlyList<IUnifiedPipelineStage> _stages;

    public UnifiedPipelineOrchestrator(IEnumerable<IUnifiedPipelineStage>? stages = null)
    {
        _stages = stages?.ToArray() ?? Array.Empty<IUnifiedPipelineStage>();
    }

    public async Task<UnifiedPipelineContext> ExecuteAsync(UnifiedPipelineRequest request)
    {
        if (request is null) throw new ArgumentNullException(nameof(request));

        var qao = BuildQao(request.BaseModel);
        var logger = request.PipelineLogger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        var executions = await DerivedTumblingPipeline.RunAsync(
            qao,
            request.BaseModel,
            request.QueryModel,
            request.ExecuteDerivedAsync,
            name => DerivedTypeFactory.GetDerivedType(name),
            request.MappingRegistry,
            request.EntityModels,
            logger,
            request.AfterExecution).ConfigureAwait(false);

        var executionArray = executions?.ToArray() ?? Array.Empty<DerivedTumblingPipeline.ExecutionResult>();
        var context = new UnifiedPipelineContext(request.EntityType, request.BaseModel, executionArray);

        try
        {
            foreach (var stage in _stages)
            {
                await stage.ExecuteAsync(context, request).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new UnifiedPipelineException($"Unified pipeline stage failed for {request.EntityType.Name}.", context, ex);
        }

        return context;
    }

    private static TumblingQao BuildQao(EntityModel model)
    {
        if (model.QueryModel == null)
            throw new InvalidOperationException("QueryModel is required for derived pipeline.");

        var ctx = new NullabilityInfoContext();
        var shape = model.AllProperties
            .Select(p => new ColumnShape(p.Name, p.PropertyType, ctx.Create(p).WriteState == NullabilityState.Nullable))
            .ToArray();

        var queryModel = model.QueryModel;
        var frames = queryModel.Windows
            .Select(tf =>
            {
                var (value, unit) = TimeframeUtils.Decompose(tf);
                return new Timeframe(value, unit);
            })
            .ToList();

        var keys = model.KeyProperties.Length > 0
            ? model.KeyProperties.Select(p => p.Name).ToArray()
            : ExtractGroupKeyNames(queryModel.GroupByExpression);

        var basedOnKeys = queryModel.BasedOnJoinKeys.Count > 0 ? queryModel.BasedOnJoinKeys.ToArray() : keys;
        var dayKey = PropertyName(queryModel.BasedOnDayKey);
        var timeKey = queryModel.TimeKey
            ?? model.AllProperties.FirstOrDefault(p => p.GetCustomAttribute<KsqlTimestampAttribute>() != null)?.Name
            ?? string.Empty;
        var projection = model.AllProperties.Select(p => p.Name).Where(n => !keys.Contains(n)).ToArray();
        var basedOnOpen = queryModel.BasedOnOpen ?? string.Empty;
        var basedOnClose = queryModel.BasedOnClose ?? string.Empty;

        return new TumblingQao
        {
            TimeKey = timeKey,
            Windows = frames,
            Keys = keys,
            Projection = projection,
            PocoShape = shape,
            BasedOn = new BasedOnSpec(
                basedOnKeys,
                basedOnOpen,
                basedOnClose,
                dayKey,
                queryModel.BasedOnOpenInclusive,
                queryModel.BasedOnCloseInclusive),
            WeekAnchor = queryModel.WeekAnchor,
            BaseUnitSeconds = queryModel.BaseUnitSeconds,
            GraceSeconds = queryModel.GraceSeconds
        };
    }

    private static string[] ExtractGroupKeyNames(LambdaExpression? groupBy)
    {
        if (groupBy == null)
            return Array.Empty<string>();

        try
        {
            var body = groupBy.Body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert ? unary.Operand : groupBy.Body;
            if (body is NewExpression ne && ne.Members != null)
                return ne.Members.OfType<MemberInfo>().Select(m => m.Name).ToArray();
            if (body is MemberExpression me)
                return new[] { me.Member.Name };
        }
        catch
        {
            // ignore expression parsing failures
        }

        return Array.Empty<string>();
    }

    private static string PropertyName(LambdaExpression? expression)
        => expression?.Body is MemberExpression me ? me.Member.Name : string.Empty;
}
