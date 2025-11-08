using Ksql.Linq;
using Ksql.Linq.Query.Builders.Utilities;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders.Utils;

public class QueryIdUtilsTests
{
    [Fact]
    public void ExtractQueryId_From_CommandStatus()
    {
        var json = "[{\"commandStatus\":{\"status\":\"SUCCESS\",\"queryId\":\"CTAS_FOO_123\"}}]";
        var resp = new KsqlDbResponse(true, json);
        var id = QueryIdUtils.ExtractQueryId(resp);
        Assert.Equal("CTAS_FOO_123", id);
    }

    [Fact]
    public void ExtractQueryId_Fallback_Regex()
    {
        var text = "Some response: Started query CSAS_BAR_456; ok";
        var resp = new KsqlDbResponse(true, text);
        var id = QueryIdUtils.ExtractQueryId(resp);
        Assert.Equal("CSAS_BAR_456", id);
    }
}
