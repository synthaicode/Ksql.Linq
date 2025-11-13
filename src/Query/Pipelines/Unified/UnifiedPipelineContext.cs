using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Query.Analysis;
using System;
using System.Collections.Generic;

namespace Ksql.Linq.Query.Pipelines.Unified;

/// <summary>
/// Captures execution results and state that unified pipeline stages can inspect or mutate.
/// </summary>
internal sealed class UnifiedPipelineContext
{
    private readonly List<PersistentQueryExecution> _persistentExecutions = new();

    public UnifiedPipelineContext(Type entityType, EntityModel baseModel, IReadOnlyList<DerivedTumblingPipeline.ExecutionResult> executions)
    {
        EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
        BaseModel = baseModel ?? throw new ArgumentNullException(nameof(baseModel));
        Executions = executions ?? Array.Empty<DerivedTumblingPipeline.ExecutionResult>();
    }

    public Type EntityType { get; }

    public EntityModel BaseModel { get; }

    public IReadOnlyList<DerivedTumblingPipeline.ExecutionResult> Executions { get; }

    public IReadOnlyList<PersistentQueryExecution> PersistentExecutions => _persistentExecutions;

    public void ReplacePersistentExecutions(IEnumerable<PersistentQueryExecution> executions)
    {
        _persistentExecutions.Clear();
        if (executions == null)
            return;

        _persistentExecutions.AddRange(executions);
    }
}
