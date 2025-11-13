using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Extensions;
using System.Reflection;

namespace Ksql.Linq.Query.Analysis;

internal static class ModelNaming
{
    public static string GetBaseId(EntityModel model)
    {
        var attr = model.EntityType.GetCustomAttribute<KsqlTopicAttribute>();
        var baseId = (attr?.Name ?? model.GetTopicName());
        return baseId.ToLowerInvariant();
    }
}

