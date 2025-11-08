using System.Threading.Tasks;

namespace Ksql.Linq.Query.Pipelines.Unified;

/// <summary>
/// Represents a post-processing stage executed after the derived pipeline completes.
/// </summary>
internal interface IUnifiedPipelineStage
{
    Task ExecuteAsync(UnifiedPipelineContext context, UnifiedPipelineRequest request);
}
