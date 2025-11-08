using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Ksql.Linq.Query.Adapters;

internal static class EntityModelAdapter
{
    public static IReadOnlyList<EntityModel> Adapt(IReadOnlyList<DerivedEntity> entities)
    {
        var list = new List<EntityModel>();
        foreach (var e in entities)
        {
            var keyShapes = e.KeyShape.ToList();
            if (keyShapes.Count == 0 && e.BasedOnSpec.JoinKeys.Count > 0)
            {
                foreach (var joinKey in e.BasedOnSpec.JoinKeys)
                {
                    if (string.IsNullOrWhiteSpace(joinKey))
                        continue;
                    keyShapes.Add(new ColumnShape(joinKey, typeof(string), false));
                }
            }
            var valueShapes = e.ValueShape.ToList();
            var keys = keyShapes.Select(k => k.Name).ToArray();
            var values = valueShapes.Select(v => v.Name).ToArray();
            if (keys.Length == 0 || values.Length == 0)
            {
                throw new InvalidOperationException("Key and value must not be empty");
            }

            var model = new EntityModel { EntityType = typeof(object) };
            // Classify Stream/Table based on role for downstream cache registration
            if (e.Role == Analysis.Role.Live)
                model.SetStreamTableType(Ksql.Linq.Query.Abstractions.StreamTableType.Table);
            else if (e.Role == Analysis.Role.Final1sStream)
                model.SetStreamTableType(Ksql.Linq.Query.Abstractions.StreamTableType.Stream);

            var metadata = QueryMetadataBuilder.FromDerivedEntity(e, keyShapes, valueShapes);
            QueryMetadataWriter.Apply(model, metadata);

            // No legacy Heartbeat-based roles
            list.Add(model);
        }
        return list;
    }
}
