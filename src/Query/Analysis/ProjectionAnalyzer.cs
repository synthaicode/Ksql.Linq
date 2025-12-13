using Ksql.Linq.Query.Builders.Visitors;
using System;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Analysis;

internal static class ProjectionAnalyzer
{
    public static void Validate(LambdaExpression projection)
    {
        if (projection == null) throw new ArgumentNullException(nameof(projection));
        var visitor = new WindowStartDetectionVisitor();
        visitor.Visit(projection.Body);
        // WindowStart() is optional for windowed queries; bounds come from windowed keys.
    }
}
