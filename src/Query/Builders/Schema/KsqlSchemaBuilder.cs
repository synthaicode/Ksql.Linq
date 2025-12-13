using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Models;
using Ksql.Linq.Query.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Ksql.Linq.Query.Builders.Schema;

/// <summary>
/// Build KSQL column schema definitions from a select projection.
/// </summary>
internal static class KsqlSchemaBuilder
{
    public static List<KsqlColumn> BuildSchema(LambdaExpression selectExpression)
    {
        if (selectExpression == null) throw new ArgumentNullException(nameof(selectExpression));

        if (selectExpression.Body is not NewExpression newExpr)
            throw new NotSupportedException("Only object initializer projections are supported.");

        var targetType = newExpr.Type;
        var isWindowed = typeof(IWindowedRecord).IsAssignableFrom(targetType);
        bool ShouldInclude(PropertyInfo prop)
        {
            if (prop.GetCustomAttributes().Any(a => a.GetType().Name == "KsqlIgnoreAttribute"))
                return false;
            if (isWindowed && (string.Equals(prop.Name, nameof(IWindowedRecord.WindowStart), StringComparison.OrdinalIgnoreCase)
                || string.Equals(prop.Name, nameof(IWindowedRecord.WindowEnd), StringComparison.OrdinalIgnoreCase)))
                return false;
            return true;
        }

        var props = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.MetadataToken)
            .Where(ShouldInclude);

        var columns = new List<KsqlColumn>();

        foreach (var prop in props)
        {
            var meta = PropertyMeta.FromProperty(prop);

            var ksqlType = KsqlTypeMapping.MapToKsqlType(meta.PropertyType, meta.PropertyInfo, meta.Precision, meta.Scale);
            var isKey = meta.Attributes.Any(a => a.GetType().Name == "KsqlKeyAttribute");
            columns.Add(new KsqlColumn(meta.Name, ksqlType, isKey));
        }

        return columns;
    }
}
