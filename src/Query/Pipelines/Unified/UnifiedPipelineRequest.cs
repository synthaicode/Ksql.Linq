using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Query.Dsl;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Ksql.Linq.Query.Pipelines.Unified;

/// <summary>
/// Captures the inputs required to execute a derived pipeline end-to-end.
/// Keeps the legacy executor decoupled from the underlying orchestration implementation.
/// </summary>
internal sealed record UnifiedPipelineRequest(
    Type EntityType,
    EntityModel BaseModel,
    KsqlQueryModel QueryModel,
    Mapping.MappingRegistry MappingRegistry,
    ConcurrentDictionary<Type, EntityModel> EntityModels,
    ILogger PipelineLogger,
    Func<EntityModel, string, Task<KsqlDbResponse>> ExecuteDerivedAsync,
    Func<Analysis.DerivedTumblingPipeline.ExecutionResult, Task>? AfterExecution);
