using Ksql.Linq.Core.Abstractions;

namespace Ksql.Linq.Query.Metadata;

internal static class EntityModelMetadataExtensions
{
    public static QueryMetadata GetOrCreateMetadata(this EntityModel model)
    {
        if (model.QueryMetadata is { } existing)
            return existing;

        var metadata = QueryMetadataFactory.FromAdditionalSettings(model.AdditionalSettings);
        model.QueryMetadata = metadata;
        return metadata;
    }

    public static void SetMetadata(this EntityModel model, QueryMetadata metadata)
    {
        model.QueryMetadata = metadata;
        // leave population of AdditionalSettings to callers during dual-write phase
    }
}
