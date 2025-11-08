using System;
using System.Linq;
using Ksql.Linq.Query.Analysis;
using Ksql.Linq.Query.Hub.Adapters;
using Ksql.Linq.Query.Dsl;
using Xunit;

namespace Ksql.Linq.Tests.Query.Analysis;

public class DateComputedMetadataTests
{
    private class Rate
    {
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
    }

    private class OutDto
    {
        public int Yr { get; set; }
        public int Mo { get; set; }
    }

    private static int Year(DateTime dt) => dt.Year;
    private static int Month(DateTime dt) => dt.Month;

    [Fact]
    public void HubInput_Marks_DateParts_As_Computed_In_Metadata()
    {
        var model = new KsqlQueryRoot()
            .From<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(r => 1)
            .Select(g => new OutDto
            {
                Yr = Year(g.WindowStart()),
                Mo = Month(g.WindowStart())
            })
            .Build();

        var adapted = model.Clone();
        if (adapted.SelectProjection != null)
            adapted.SelectProjection = HubRowsProjectionAdapter.Adapt(adapted.SelectProjection);

        var meta = ProjectionMetadataAnalyzer.Build(adapted, isHubInput: true);
        var yr = meta.Members.First(m => m.Alias == nameof(OutDto.Yr));
        var mo = meta.Members.First(m => m.Alias == nameof(OutDto.Mo));

        Assert.Equal(ProjectionMemberKind.Computed, yr.Kind);
        Assert.Equal(ProjectionMemberKind.Computed, mo.Kind);
        // Resolved column names should fall back to alias when not mapped to hub columns
        Assert.Equal("YR", yr.ResolvedColumnName);
        Assert.Equal("MO", mo.ResolvedColumnName);
    }
}


