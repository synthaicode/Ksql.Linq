using System;

namespace Ksql.Linq.Query.Pipelines.Unified;

/// <summary>
/// Signals that a unified pipeline stage failed and surfaces the in-flight execution context.
/// </summary>
internal sealed class UnifiedPipelineException : Exception
{
    public UnifiedPipelineException(string message, UnifiedPipelineContext context, Exception innerException)
        : base(message, innerException)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public UnifiedPipelineContext Context { get; }
}
