using Ksql.Linq.Core.Abstractions;

namespace Ksql.Linq.Infrastructure.Ksql;

internal sealed record PersistentQueryExecution(
    string QueryId,
    global::Ksql.Linq.Core.Abstractions.EntityModel TargetModel,
    string TargetTopic,
    string Statement,
    string? InputTopic,
    bool IsDerived)
{
    public string? ConsumerGroupId { get; init; }
}

