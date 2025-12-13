using System;
using System.Linq;
using System.Linq.Expressions;
using Ksql.Linq;
using Ksql.Linq.Query.Analysis;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class ProjectionAnalyzerTests
{
    [Fact]
    public void Validate_NoWindowStart_Throws()
    {
        Expression<Func<IGrouping<int, int>, object>> expr = g => new { Count = g.Count() };
        // Hopping/Tumbling projection may omit WindowStart when window bounds come from windowed key
        ProjectionAnalyzer.Validate(expr);
    }

    [Fact]
    public void Validate_MultipleWindowStart_Throws()
    {
        Expression<Func<IGrouping<int, int>, object>> expr = g => new { Start1 = g.WindowStart(), Start2 = g.WindowStart() };
        // Multiple WindowStart projections are tolerated; analyzer should not throw
        ProjectionAnalyzer.Validate(expr);
    }
}
