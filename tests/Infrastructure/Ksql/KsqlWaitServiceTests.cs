using Ksql.Linq.Infrastructure.Ksql;
using Xunit;

namespace Ksql.Linq.Tests.Infrastructure.Ksql;

[Trait("Level", "L2")]
public class KsqlWaitServiceTests
{
    [Fact]
    public void NormalizeIdentifier_TrimsQuotesAndUppercases()
    {
        Assert.Equal("FOO_BAR", KsqlWaitService.NormalizeIdentifier("\"foo_bar\""));
    }

    [Fact]
    public void NormalizeSql_CollapsesWhitespace_Uppercases()
    {
        var s = KsqlWaitService.NormalizeSql("create  table   if  not    exists\nbar as select *  from  x");
        Assert.Equal("CREATE TABLE IF NOT EXISTS BAR AS SELECT * FROM X", s);
    }

    [Fact]
    public void TryGetQueryStateFromJson_MatchesRunningByState()
    {
        var json = @"[
          {""queries"": [
            {""id"": ""CTAS_BAR_1M_LIVE_1"", ""sinks"": [""BAR_1M_LIVE""], ""state"": ""RUNNING""}
          ]}
        ]";
        var ok = KsqlWaitService.TryGetQueryStateFromJson(json, "BAR_1M_LIVE", null, out var state);
        Assert.True(ok);
        Assert.Equal("RUNNING", state);
    }

    [Fact]
    public void FindQueryIdInShowQueries_JsonAndText()
    {
        var json = @"[
          {""queries"": [
            {""id"": ""Q1"", ""queryString"": ""CREATE TABLE IF NOT EXISTS BAR_1M_LIVE AS SELECT ..."", ""sinkKafkaTopics"": [""bar_1m_live""]}
          ]}
        ]";
        var id = KsqlWaitService.FindQueryIdInShowQueries(json, "bar_1m_live", "create table if not exists bar_1m_live as select");
        Assert.Equal("Q1", id);

        var text = @"\n Query ID | Kafka Topic | Query String | Status\n Q2 | BAR_1M_LIVE | CREATE TABLE IF NOT EXISTS BAR_1M_LIVE AS SELECT * FROM X | RUNNING\n";
        var id2 = KsqlWaitService.FindQueryIdInShowQueries(text, "bar_1m_live", "create table if not exists bar_1m_live as select");
        Assert.Equal("Q2", id2);
    }

    [Fact]
    public void CheckQueryRunningInText_VariousFormats()
    {
        var lines = new[]
        {
            " Q2 | BAR_1M_LIVE | CREATE TABLE ... | RUNNING ",
            " Q3 | OTHERS | SELECT ... | RUNNING:1 "
        };
        Assert.True(KsqlWaitService.CheckQueryRunningInText(lines, "BAR_1M_LIVE", null));
        Assert.True(KsqlWaitService.CheckQueryRunningInText(lines, "OTHERS", null));
    }
}